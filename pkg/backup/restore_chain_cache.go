package backup

import (
	"context"
	"sync"

	"github.com/Altinity/clickhouse-backup/v2/pkg/metadata"
	"github.com/pkg/errors"
	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"
	"golang.org/x/sync/errgroup"
)

// backupMetadataChainCache is a read-only snapshot of the entire incremental backup chain.
// It is built once before parallel restore goroutines start and never mutated afterwards,
// so goroutines can read it concurrently without any locking overhead.
type backupMetadataChainCache struct {
	// backups maps backupName -> BackupMetadata for every backup in the chain.
	backups map[string]*metadata.BackupMetadata
	// tableMetadata maps backupName -> TableTitle -> TableMetadata for every
	// ancestor backup that contains tables with required parts.
	tableMetadata map[string]map[metadata.TableTitle]*metadata.TableMetadata
}

// buildBackupChainCache eagerly fetches the metadata of the entire required-backup chain
// (current backup + all RequiredBackup ancestors) and all table metadata for tables that
// have required parts, issuing all S3 requests in parallel up front.
//
// This replaces the pattern where findObjectDiskPartRecursive was called per-goroutine
// per-part, each time calling ReadBackupMetadataRemote (which acquires metadataCacheLock)
// and downloadTableMetadataIfNotExists — producing O(parts × chain_depth) serialised
// S3 API calls.  With the cache the chain is fetched in O(chain_depth) sequential calls
// and all table metadata is fetched in one parallel batch.
func (b *Backuper) buildBackupChainCache(ctx context.Context, rootBackup metadata.BackupMetadata, tables ListOfTables) (*backupMetadataChainCache, error) {
	cache := &backupMetadataChainCache{
		backups:       make(map[string]*metadata.BackupMetadata),
		tableMetadata: make(map[string]map[metadata.TableTitle]*metadata.TableMetadata),
	}

	// Seed the root backup so goroutines can look it up by name too.
	rootCopy := rootBackup
	cache.backups[rootBackup.BackupName] = &rootCopy

	// Walk the RequiredBackup chain sequentially — each step requires the previous
	// result to know the next backup name, so parallelism is not possible here.
	currentName := rootBackup.RequiredBackup
	for currentName != "" {
		if _, already := cache.backups[currentName]; already {
			// Cycle guard — should never happen with valid backup metadata.
			break
		}
		remote, err := b.ReadBackupMetadataRemote(ctx, currentName)
		if err != nil {
			return nil, errors.Wrapf(err, "buildBackupChainCache: ReadBackupMetadataRemote(%s)", currentName)
		}
		cache.backups[currentName] = remote
		currentName = remote.RequiredBackup
	}

	// Determine which tables have any required parts — only those need ancestor metadata.
	tableTitlesWithRequired := make(map[metadata.TableTitle]struct{})
	for _, t := range tables {
		if t == nil {
			continue
		}
		for _, parts := range t.Parts {
			for _, part := range parts {
				if part.Required {
					tableTitlesWithRequired[metadata.TableTitle{
						Database: t.Database,
						Table:    t.Table,
					}] = struct{}{}
					break
				}
			}
		}
	}

	if len(tableTitlesWithRequired) == 0 {
		// No required parts → nothing to pre-fetch from ancestor backups.
		return cache, nil
	}

	// Build the list of (backupName, TableTitle) pairs to fetch in parallel.
	type fetchWork struct {
		backupName string
		title      metadata.TableTitle
	}
	var works []fetchWork
	for bkpName := range cache.backups {
		if bkpName == rootBackup.BackupName {
			// Root backup tables are already local — no remote fetch needed.
			continue
		}
		for title := range tableTitlesWithRequired {
			works = append(works, fetchWork{backupName: bkpName, title: title})
		}
	}

	log.Info().
		Int("ancestor_backups", len(cache.backups)-1).
		Int("tables_with_required_parts", len(tableTitlesWithRequired)).
		Int("total_fetch_tasks", len(works)).
		Msg("buildBackupChainCache: pre-fetching ancestor table metadata in parallel")

	var mu sync.Mutex
	fetchGroup, fetchCtx := errgroup.WithContext(ctx)
	// Reuse DownloadConcurrency to limit parallel S3 API calls.
	fetchGroup.SetLimit(int(b.cfg.General.DownloadConcurrency))

	for _, w := range works {
		w := w
		fetchGroup.Go(func() error {
			tm, err := b.downloadTableMetadataIfNotExists(fetchCtx, w.backupName, w.title)
			if err != nil {
				// Non-fatal: the table may simply not exist in this ancestor backup
				// (e.g. it was added in a later incremental).
				log.Warn().Msgf(
					"buildBackupChainCache: downloadTableMetadataIfNotExists(%s / %s.%s): %v — skipping",
					w.backupName, w.title.Database, w.title.Table, err,
				)
				return nil
			}
			mu.Lock()
			if cache.tableMetadata[w.backupName] == nil {
				cache.tableMetadata[w.backupName] = make(map[metadata.TableTitle]*metadata.TableMetadata)
			}
			cache.tableMetadata[w.backupName][w.title] = tm
			mu.Unlock()
			return nil
		})
	}
	if err := fetchGroup.Wait(); err != nil {
		return nil, errors.WithMessage(err, "buildBackupChainCache: fetchGroup")
	}

	log.Info().
		Int("ancestor_backups_cached", len(cache.backups)-1).
		Int("table_metadata_entries_cached", func() int {
			n := 0
			for _, m := range cache.tableMetadata {
				n += len(m)
			}
			return n
		}()).
		Msg("buildBackupChainCache: done")

	return cache, nil
}

// findObjectDiskPartRecursiveCached is the lock-free, cache-based equivalent of
// findObjectDiskPartRecursive.  It reads exclusively from the pre-built
// backupMetadataChainCache and never issues any S3 API calls, making it safe
// to call from many concurrent goroutines without contention.
func (b *Backuper) findObjectDiskPartRecursiveCached(
	backup metadata.BackupMetadata,
	table metadata.TableMetadata,
	part metadata.Part,
	diskName string,
	cache *backupMetadataChainCache,
	logger zerolog.Logger,
) (string, string, error) {
	if !part.Required {
		return backup.BackupName, diskName, nil
	}
	if backup.RequiredBackup == "" {
		return "", "", errors.Errorf(
			"part %s has required flag in %s but backup.RequiredBackup is empty",
			part.Name, backup.BackupName,
		)
	}

	requiredBackup, ok := cache.backups[backup.RequiredBackup]
	if !ok {
		return "", "", errors.Errorf(
			"findObjectDiskPartRecursiveCached: backup %s not found in chain cache",
			backup.RequiredBackup,
		)
	}

	tableTitle := metadata.TableTitle{Database: table.Database, Table: table.Table}
	requiredTable, ok := cache.tableMetadata[backup.RequiredBackup][tableTitle]
	if !ok {
		return "", "", errors.Errorf(
			"findObjectDiskPartRecursiveCached: table %s.%s not found in chain cache for backup %s",
			table.Database, table.Table, backup.RequiredBackup,
		)
	}

	logger.Debug().Msgf(
		"findObjectDiskPartRecursiveCached: looking for part %s in %s (cached)",
		part.Name, backup.RequiredBackup,
	)

	for requiredDiskName, parts := range requiredTable.Parts {
		for _, requiredPart := range parts {
			if requiredPart.Name != part.Name {
				continue
			}
			if requiredPart.Required {
				// Part is itself required — recurse deeper into the chain.
				return b.findObjectDiskPartRecursiveCached(
					*requiredBackup, *requiredTable, requiredPart, requiredDiskName, cache, logger,
				)
			}
			return requiredBackup.BackupName, requiredDiskName, nil
		}
	}
	return "", "", errors.Errorf(
		"part %s has required flag in %s, but was not found in %s",
		part.Name, backup.BackupName, backup.RequiredBackup,
	)
}
