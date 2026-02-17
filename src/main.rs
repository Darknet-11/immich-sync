mod api;
mod config;
mod hash;
mod local_db;
mod policy;
mod sync;
mod workers;

use anyhow::Result;
use config::{parse_config_path, Config};
use local_db::LocalDatabase;
use log::info;
use std::sync::Arc;
use sync::run_user_sync;
use tokio::sync::Mutex;
use tokio::time::{sleep, Duration};
use tokio_util::sync::CancellationToken;

#[tokio::main]
async fn main() -> Result<()> {
    env_logger::Builder::from_env(env_logger::Env::default().default_filter_or("info")).format_timestamp(None).init();

    let config_path = parse_config_path();
    let config = Config::load(&config_path)?;

    let config = Arc::new(config);

    let db_path = config.database_path();
    let local_db = LocalDatabase::open(&db_path)?;
    let local_db = Arc::new(Mutex::new(local_db));

    if config.users.is_empty() {
        info!("No users configured, exiting");
        return Ok(());
    }

    let cancel = CancellationToken::new();

    let cancel_for_signal = cancel.clone();
    tokio::spawn(async move {
        tokio::signal::ctrl_c().await.expect("Failed to register signal handler");
        info!("Received shutdown signal, shutting down...");
        cancel_for_signal.cancel();
    });

    let mut handles = Vec::new();
    let user_ids: Vec<String> = config.users.iter().map(|u| u.user_id.clone()).collect();
    for user_id in user_ids {
        let cancel = cancel.clone();
        let local_db = Arc::clone(&local_db);
        let config = Arc::clone(&config);

        let handle = tokio::spawn(async move {
            if let Err(e) = run_user_sync(cancel, local_db, &config, &user_id).await {
                info!("User sync task failed: {}", e);
            }
        });
        handles.push(handle);
    }

    loop {
        tokio::select! {
            _ = sleep(Duration::from_secs(3600)) => {
                for (i, handle) in handles.iter().enumerate() {
                    if handle.is_finished() {
                        info!("Critical: User sync task {} has finished unexpectedly", i);
                    }
                }
            }
            _ = cancel.cancelled() => {
                break;
            }
        }
    }

    for handle in handles {
        let _ = handle.await;
    }

    Ok(())
}
