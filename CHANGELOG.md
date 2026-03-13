# Changelog

All notable changes to this project will be documented in this file.

## 0.1.5 - 2026-03-13

### Fixed

- Handle Syncthing rename-to-trash deletions. Syncthing deletes files by renaming them to `.trashed-*` instead of unlinking, which was not detected as a removal. The file watcher now treats Create/Modify events for missing files as deletions.
- Reduce log noise from deletion_watcher cleanup races (suppress "not found in local database" for already-removed records).

## 0.1.4 - 2026-03-13

### Added

- Debounce file watcher events to avoid redundant hashes and uploading partially-written files. Events for the same path are coalesced over a 2-second window before processing.

## 0.1.3 - 2026-03-12

### Fixed

- Build Linux releases against multiple glibc versions for broader compatibility.

## 0.1.2 - 2026-03-12

### Added

- `--dry-run` (`-n`) mode that skips all mutations (uploads, deletes, database writes).

## 0.1.1 - 2026-02-27

### Added

- Structured JSONL event log for observability across all workers.

### Changed

- Use worker name constants and improve event log error handling and parsing.

## 0.1.0 - 2026-02-17

Initial release of Immich Sync Service.
