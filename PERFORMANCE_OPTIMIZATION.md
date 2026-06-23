# Оптимизация производительности восстановления из S3

## Проблема

При восстановлении большого количества таблиц ClickHouse из S3 с использованием инкрементальных бэкапов наблюдалась крайне низкая производительность:

- **Восстановление 280 GB данных (~3500 таблиц) из S3: 8 часов**
- **То же восстановление из локального бэкапа: 11 минут**
- **Разница в скорости: ~44x**

### Причина

Основная причина - избыточные вызовы API листинга S3 через функцию `BackupList`:

1. При восстановлении с инкрементальными бэкапами для каждой таблицы вызывается `ReadBackupMetadataRemote`
2. `ReadBackupMetadataRemote` вызывает `BackupList`, который выполняет полный `Walk` по S3
3. Для incremental backup chain это происходит рекурсивно для каждого уровня
4. **Итого**: для 3500 таблиц с 5-уровневой цепочкой инкрементов = ~17500 вызовов `Walk` к S3

## Решение

### 1. In-Memory кэш для `BackupList`

**Файл**: `pkg/storage/general.go`

Добавлен двухуровневый кэш:
- **Дисковый кэш** (уже существовал): `/tmp/.clickhouse-backup-metadata.cache.{storage_type}`
- **Новый in-memory кэш**: для запросов метаданных конкретного бэкапа

```go
var backupListCache = make(map[string][]Backup)
var backupListCacheLock sync.RWMutex

func (bd *BackupDestination) BackupList(ctx context.Context, parseMetadata bool, parseMetadataOnly string) ([]Backup, error) {
    // Проверяем in-memory кэш для запросов конкретного бэкапа
    if parseMetadataOnly != "" {
        backupListCacheLock.RLock()
        cacheKey := bd.Kind() + ":" + parseMetadataOnly
        if cachedList, ok := backupListCache[cacheKey]; ok {
            backupListCacheLock.RUnlock()
            log.Debug().Str("backup", parseMetadataOnly).Msg("BackupList: using in-memory cache")
            return cachedList, nil
        }
        backupListCacheLock.RUnlock()
    }
    
    // ... выполнение Walk если кэш не найден ...
    
    // Сохраняем в in-memory кэш для последующих запросов
    if parseMetadataOnly != "" && len(result) > 0 {
        backupListCacheLock.Lock()
        cacheKey := bd.Kind() + ":" + parseMetadataOnly
        backupListCache[cacheKey] = result
        backupListCacheLock.Unlock()
    }
    
    return result, nil
}
```

### 2. Prefetch метаданных цепочки бэкапов

**Файл**: `pkg/backup/download.go`

Перед началом восстановления предзагружаем метаданные всей цепочки инкрементальных бэкапов:

```go
func (b *Backuper) prefetchBackupMetadataChain(ctx context.Context, backupName string) error {
    // Проходим по цепочке RequiredBackup -> RequiredBackup -> ...
    // и загружаем метаданные каждого бэкапа в кэш
    // Это делается один раз в начале, вместо многократных вызовов при обработке каждой таблицы
}
```

### 3. Оптимизация логирования

Изменили уровень логирования `list_duration` с `Info` на `Debug`, так как эта метрика создавала слишком много записей в логах при тысячах таблиц.

### 4. Очистка кэша

Кэш очищается после завершения операции download:

```go
// Clear backup list cache after download completes
storage.ClearBackupListCache()
```

## Ожидаемый эффект

### До оптимизации:
- 17500 вызовов S3 API (Walk)
- Каждый вызов ~1-3 секунды
- **Итого: ~14-15 часов только на API вызовы**

### После оптимизации:
- ~5-10 вызовов S3 API (Walk) для prefetch цепочки
- Остальные запросы берутся из in-memory кэша
- **Ожидается сокращение времени в 40-100 раз**

### Реальные цифры (ожидаемые):
- **Было**: 8 часов для восстановления 280 GB из S3
- **Станет**: 15-30 минут (сравнимо с локальным восстановлением + время на network I/O)

## Дополнительные рекомендации

### 1. Настройка параметров

В конфигурации можно увеличить параллелизм:

```yaml
general:
  download_concurrency: 8  # Увеличьте для быстрой сети
  download_max_bytes_per_second: 0  # 0 = без ограничений
```

### 2. Использование S3 endpoint в той же зоне

Для AWS S3 используйте endpoint в той же availability zone, что и ClickHouse:
```yaml
s3:
  endpoint: https://s3.{region}.amazonaws.com
```

### 3. Мониторинг

Следите за метриками:
- `list_duration` (теперь в debug логах)
- Общее время download операции
- Количество S3 API вызовов

### 4. Тестирование

Для проверки эффективности кэша:

```bash
# Первый запуск - холодный кэш
time clickhouse-backup restore_remote backup_name

# Повторный запуск той же операции (если прервали)
# Должен быть значительно быстрее благодаря кэшу
time clickhouse-backup restore_remote backup_name --resume
```

## Технические детали

### Потокобезопасность

Все операции с кэшем защищены мьютексами:
- `backupListCacheLock` для in-memory кэша
- `metadataCacheLock` для дискового кэша

### Память

In-memory кэш хранит только структуры `Backup`, которые содержат метаданные (не данные таблиц):
- Размер одной записи: ~1-5 KB
- Для 100 бэкапов: ~100-500 KB
- **Общий overhead памяти: минимальный**

### Совместимость

Изменения полностью обратно совместимы:
- Старый дисковый кэш продолжает работать
- Новый in-memory кэш добавляется опционально
- Если кэш не нужен, он просто не используется

## Мониторинг и отладка

### Debug логи

Для отладки включите debug логи:

```bash
LOG_LEVEL=debug clickhouse-backup restore_remote backup_name
```

Вы увидите:
```
DBG BackupList: using in-memory cache backup=backup_name
DBG prefetchBackupMetadataChain: discovered chain of 5 backups: [backup5, backup4, ...] (took 10s)
```

### Метрики

После оптимизации в логах Info уровня будет значительно меньше записей `list_duration`.

## Changelog

- ✅ Добавлен in-memory кэш для `BackupList`
- ✅ Реализован prefetch метаданных цепочки бэкапов
- ✅ Логирование `list_duration` переведено на Debug уровень
- ✅ Добавлена функция `ClearBackupListCache()` для очистки кэша
