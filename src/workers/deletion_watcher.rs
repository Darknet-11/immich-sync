use crate::api::{BulkCheckInput, ImmichAPI};
use crate::event_log::{workers, EventLogger};
use crate::hash::checksum_to_hex;
use crate::local_db::LocalDatabase;
use crate::policy::{evaluate_delete_age, DeleteAgeEligibility};
use log::info;
use std::path::PathBuf;
use std::sync::Arc;
use tokio::sync::Mutex;
use tokio::time::{sleep, Duration};
use tokio_util::sync::CancellationToken;

const BATCH_SIZE: usize = 2000;

/// Detects assets deleted on the Immich server and removes the local copies.
///
/// On each cycle every tracked asset is checked against the server using the
/// bulk-upload-check endpoint:
///
/// - **Still on server** (`reject`) — skipped.
/// - **Gone from server** (`accept`) — the local file is deleted from disk and the
///   database record is removed.
///
/// This is the reverse of the file watcher's delete handling: the file watcher
/// propagates local deletes to Immich, while this worker propagates Immich-side
/// deletes back to disk.
#[allow(clippy::too_many_arguments)]
pub async fn deletion_watcher(
    cancel: CancellationToken,
    local_db: Arc<Mutex<LocalDatabase>>,
    api: Arc<Mutex<ImmichAPI>>,
    user_path: PathBuf,
    user_id: String,
    poll_interval: u64,
    delete_max_age: i64,
    event_logger: Option<EventLogger>,
) {
    info!("Deletion watcher thread running (poll interval: {}s)...", poll_interval);

    loop {
        if cancel.is_cancelled() {
            return;
        }

        let tracked_assets = match local_db.lock().await.list_tracked_assets(&user_id) {
            Ok(a) => a,
            Err(e) => {
                info!("Failed to get tracked assets: {}", e);
                tokio::select! {
                    _ = sleep(Duration::from_secs(60)) => {}
                    _ = cancel.cancelled() => { return; }
                }
                continue;
            }
        };

        if tracked_assets.is_empty() {
            tokio::select! {
                _ = sleep(Duration::from_secs(poll_interval)) => {}
                _ = cancel.cancelled() => { return; }
            }
            continue;
        }

        if let Some(el) = &event_logger {
            el.log(
                workers::DELETION_WATCHER,
                "reconciliation_started",
                &user_id,
                None,
                None,
                Some(&format!("{} assets", tracked_assets.len())),
            );
        }

        info!("Reconciling {} tracked assets for user {}", tracked_assets.len(), user_id);

        // Build inputs with asset_path as the id for correlation
        let inputs: Vec<(BulkCheckInput, String)> = tracked_assets
            .iter()
            .map(|a| {
                (
                    BulkCheckInput { id: a.asset_path.clone(), checksum_hex: checksum_to_hex(&a.checksum) },
                    a.asset_path.clone(),
                )
            })
            .collect();

        for chunk in inputs.chunks(BATCH_SIZE) {
            if cancel.is_cancelled() {
                return;
            }

            let bulk_inputs: Vec<&BulkCheckInput> = chunk.iter().map(|(input, _)| input).collect();
            // Need to clone for the API call
            let api_inputs: Vec<BulkCheckInput> = bulk_inputs
                .iter()
                .map(|i| BulkCheckInput { id: i.id.clone(), checksum_hex: i.checksum_hex.clone() })
                .collect();

            let results = match api.lock().await.bulk_upload_check(&api_inputs).await {
                Ok(r) => r,
                Err(e) => {
                    info!("bulk-upload-check failed during reconciliation: {}", e);
                    break;
                }
            };

            for result in &results.results {
                if cancel.is_cancelled() {
                    return;
                }

                if result.action != "accept" {
                    continue;
                }

                if let Some(el) = &event_logger {
                    el.log(workers::DELETION_WATCHER, "remote_delete_detected", &user_id, Some(&result.id), None, None);
                }

                // Check asset age before deleting — respect delete_max_age
                let skip = match local_db.lock().await.asset_age_days(&user_id, &result.id) {
                    Ok(age_days) => match evaluate_delete_age(age_days, delete_max_age) {
                        DeleteAgeEligibility::Eligible(_) => false,
                        DeleteAgeEligibility::FutureCreatedAt(age) => {
                            info!(
                                "Asset {} has creation date in the future ({} days), skipping remote-to-local delete",
                                result.id, age
                            );
                            true
                        }
                        DeleteAgeEligibility::UnrealisticAge(age) => {
                            info!(
                                "Asset {} has unrealistic age ({} days), skipping remote-to-local delete",
                                result.id, age
                            );
                            true
                        }
                        DeleteAgeEligibility::MissingCreatedAt => {
                            info!("Asset {} has no creation date, skipping remote-to-local delete", result.id);
                            true
                        }
                    },
                    Err(e) => {
                        info!("Failed to compute asset age for {}: {}", result.id, e);
                        true
                    }
                };

                if skip {
                    if let Some(el) = &event_logger {
                        el.log(
                            workers::DELETION_WATCHER,
                            "remote_delete_skipped",
                            &user_id,
                            Some(&result.id),
                            None,
                            Some("age/policy"),
                        );
                    }
                    continue;
                }

                // Asset no longer exists in Immich — delete local file
                let full_path = user_path.join(&result.id);
                info!("Asset {} deleted from Immich, removing local file {}", result.id, full_path.display());

                if full_path.exists() {
                    if let Err(e) = tokio::fs::remove_file(&full_path).await {
                        info!("Failed to remove file {}: {}", full_path.display(), e);
                    } else if let Some(el) = &event_logger {
                        el.log(workers::DELETION_WATCHER, "local_file_deleted", &user_id, Some(&result.id), None, None);
                    }
                } else {
                    info!("Local file {} already removed", full_path.display());
                }

                if let Err(e) = local_db.lock().await.delete_asset(&user_id, &result.id) {
                    info!("Failed to remove asset from database: {}", e);
                } else if let Some(el) = &event_logger {
                    el.log(workers::DELETION_WATCHER, "db_record_removed", &user_id, Some(&result.id), None, None);
                }
            }
        }

        tokio::select! {
            _ = sleep(Duration::from_secs(poll_interval)) => {}
            _ = cancel.cancelled() => { return; }
        }
    }
}
