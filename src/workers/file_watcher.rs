use crate::api::ImmichAPI;
use crate::event_log::{workers, EventLogger};
use crate::hash::hash_file;
use crate::local_db::LocalDatabase;
use crate::policy::{evaluate_delete_age, should_propagate_local_delete, DeleteAgeEligibility};
use crate::sync::ignored_path;
use log::info;
use notify::{Event, EventKind, RecursiveMode, Watcher};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use tokio::sync::mpsc;
use tokio::sync::Mutex;
use tokio_util::sync::CancellationToken;

/// Reacts to live filesystem events in the user directory.
///
/// Uses OS-level notifications (`notify` crate) to detect file changes in real time:
///
/// - **Create / Modify** — the file is hashed and upserted into the local database
///   without an Immich asset ID, so the upload worker will pick it up.
/// - **Remove** — if the asset is young enough (below `delete_threshold` days) and has
///   a plausible age (between 0 and `delete_max_age` days), the corresponding asset is
///   also deleted from Immich. Either way the local database record is removed.
///
/// This complements the discovery worker: the watcher handles live changes while
/// discovery catches pre-existing files and anything the watcher may have missed.
#[allow(clippy::too_many_arguments)]
pub async fn file_watcher(
    cancel: CancellationToken,
    local_db: Arc<Mutex<LocalDatabase>>,
    api: Arc<Mutex<ImmichAPI>>,
    user_path: PathBuf,
    user_id: String,
    delete_threshold: i64,
    delete_max_age: i64,
    event_logger: Option<EventLogger>,
    dry_run: bool,
) {
    info!("File watcher thread running...");

    let (tx, mut rx) = mpsc::channel::<Event>(256);

    let mut _watcher = notify::recommended_watcher({
        let tx = tx.clone();
        move |res: Result<Event, notify::Error>| {
            if let Ok(event) = res {
                let _ = tx.blocking_send(event);
            }
        }
    })
    .expect("Failed to create file watcher");

    _watcher.watch(&user_path, RecursiveMode::Recursive).expect("Failed to watch directory");

    loop {
        tokio::select! {
            Some(event) = rx.recv() => {
                for path in &event.paths {
                    if ignored_path(path) {
                        continue;
                    }
                    match event.kind {
                        EventKind::Create(_) | EventKind::Modify(_) => {
                            handle_create_or_modify(path, event.kind.is_create(), &local_db, &user_path, &user_id, &event_logger).await;
                        }
                        EventKind::Remove(_) => {
                            handle_remove(path, &local_db, &api, &user_path, &user_id, delete_threshold, delete_max_age, &event_logger, dry_run).await;
                        }
                        _ => {}
                    }
                }
            }
            _ = cancel.cancelled() => {
                return;
            }
        }
    }
}

async fn handle_create_or_modify(
    path: &Path,
    is_create: bool,
    local_db: &Mutex<LocalDatabase>,
    user_path: &Path,
    user_id: &str,
    event_logger: &Option<EventLogger>,
) {
    let action = if is_create { "added" } else { "modified" };
    info!("{} {}, queuing for upload", path.display(), action);

    let relative_path = match path.strip_prefix(user_path) {
        Ok(p) => p.to_string_lossy().to_string(),
        Err(_) => return,
    };

    if let Some(el) = event_logger {
        el.log(workers::FILE_WATCHER, "file_detected", user_id, Some(&relative_path), None, Some(action));
    }

    let checksum = match hash_file(path).await {
        Ok(c) => c,
        Err(e) => {
            info!("Failed to hash {}: {}", path.display(), e);
            return;
        }
    };

    if let Err(e) = local_db.lock().await.upsert_asset(user_id, &relative_path, &checksum, None, None) {
        info!("Failed to save asset: {}", e);
        return;
    }

    if let Some(el) = event_logger {
        el.log(workers::FILE_WATCHER, "file_queued", user_id, Some(&relative_path), None, None);
    }
}

#[allow(clippy::too_many_arguments)]
async fn handle_remove(
    path: &Path,
    local_db: &Mutex<LocalDatabase>,
    api: &Mutex<ImmichAPI>,
    user_path: &Path,
    user_id: &str,
    delete_threshold: i64,
    delete_max_age: i64,
    event_logger: &Option<EventLogger>,
    dry_run: bool,
) {
    let relative_path = match path.strip_prefix(user_path) {
        Ok(p) => p.to_string_lossy().to_string(),
        Err(_) => return,
    };

    let row = match local_db.lock().await.find_asset_by_path(user_id, &relative_path) {
        Ok(Some(r)) => r,
        Ok(None) => {
            info!("Asset {} not found in local database", relative_path);
            return;
        }
        Err(e) => {
            info!("Failed to look up asset: {}", e);
            return;
        }
    };

    let file_age_days = match local_db.lock().await.asset_age_days(user_id, &relative_path) {
        Ok(age_days) => match evaluate_delete_age(age_days, delete_max_age) {
            DeleteAgeEligibility::Eligible(age) => age,
            DeleteAgeEligibility::FutureCreatedAt(age) => {
                info!("{} has creation date in the future ({} days), skipping delete", path.display(), age);
                if let Some(el) = event_logger {
                    el.log(
                        workers::FILE_WATCHER,
                        "delete_skipped",
                        user_id,
                        Some(&relative_path),
                        None,
                        Some(&format!("future created_at ({} days)", age)),
                    );
                }
                return;
            }
            DeleteAgeEligibility::UnrealisticAge(age) => {
                info!("{} has unrealistic age ({} days), skipping delete", path.display(), age);
                if let Some(el) = event_logger {
                    el.log(
                        workers::FILE_WATCHER,
                        "delete_skipped",
                        user_id,
                        Some(&relative_path),
                        None,
                        Some(&format!("unrealistic age ({} days)", age)),
                    );
                }
                return;
            }
            DeleteAgeEligibility::MissingCreatedAt => {
                info!("{} has no creation date, skipping delete", path.display());
                if let Some(el) = event_logger {
                    el.log(
                        workers::FILE_WATCHER,
                        "delete_skipped",
                        user_id,
                        Some(&relative_path),
                        None,
                        Some("missing created_at"),
                    );
                }
                return;
            }
        },
        Err(e) => {
            info!("Failed to compute asset age for {}: {}", path.display(), e);
            return;
        }
    };

    if should_propagate_local_delete(file_age_days, delete_threshold) {
        if let Some(asset_id) = &row.asset_id {
            if dry_run {
                if let Some(el) = event_logger {
                    el.log(
                        workers::FILE_WATCHER,
                        "delete_skipped",
                        user_id,
                        Some(&relative_path),
                        Some(asset_id),
                        Some("dry-run"),
                    );
                }
            } else {
                info!(
                    "{} deleted, age {} days is below threshold of {} days, deleting asset in Immich",
                    path.display(),
                    file_age_days,
                    delete_threshold
                );
                if let Err(e) = api.lock().await.delete_asset(asset_id).await {
                    info!("Failed to delete asset: {}", e);
                } else if let Some(el) = event_logger {
                    el.log(
                        workers::FILE_WATCHER,
                        "delete_propagated",
                        user_id,
                        Some(&relative_path),
                        Some(asset_id),
                        None,
                    );
                }
            }
        } else {
            info!("{} deleted but not yet uploaded, removing from database", path.display());
            if let Some(el) = event_logger {
                el.log(
                    workers::FILE_WATCHER,
                    "delete_skipped",
                    user_id,
                    Some(&relative_path),
                    None,
                    Some("not yet uploaded"),
                );
            }
        }
    } else {
        info!(
            "{} deleted, age {} days exceeds threshold of {} days, skipping delete in Immich",
            path.display(),
            file_age_days,
            delete_threshold
        );
        if let Some(el) = event_logger {
            el.log(
                workers::FILE_WATCHER,
                "delete_skipped",
                user_id,
                Some(&relative_path),
                None,
                Some(&format!("age {} exceeds threshold {}", file_age_days, delete_threshold)),
            );
        }
    }

    if !dry_run {
        if let Err(e) = local_db.lock().await.delete_asset(user_id, &relative_path) {
            info!("Failed to remove asset from database: {}", e);
            return;
        }

        if let Some(el) = event_logger {
            el.log(workers::FILE_WATCHER, "db_record_removed", user_id, Some(&relative_path), None, None);
        }
    }
}
