# Changelog

All notable changes to this project will be documented in this file.

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
