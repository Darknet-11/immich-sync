use anyhow::Result;
use log::info;
use std::path::Path;
use std::sync::Arc;
use tokio::sync::Mutex;
use tokio_util::sync::CancellationToken;

use crate::api::ImmichAPI;
use crate::config::Config;
use crate::event_log::EventLogger;
use crate::local_db::LocalDatabase;
use crate::workers::{deletion_watcher, discovery, file_watcher, uploader};

pub fn ignored_path(path: &Path) -> bool {
    if let Some(name) = path.file_name().and_then(|n| n.to_str()) {
        if name.starts_with('.') {
            return true;
        }
    }
    path.is_dir()
}

pub async fn run_user_sync(
    cancel: CancellationToken,
    local_db: Arc<Mutex<LocalDatabase>>,
    config: &Config,
    user_id: &str,
    event_logger: Option<EventLogger>,
) -> Result<()> {
    let user = config
        .users
        .iter()
        .find(|u| u.user_id == user_id)
        .unwrap_or_else(|| panic!("User {} not found in config", user_id));
    let api = Arc::new(Mutex::new(ImmichAPI::new(&config.immich.server_url, &user.user_key)));
    let user_path = Path::new(&user.path);

    info!("Starting sync for user {} at {}", user.user_id, user_path.display());

    if !user_path.exists() {
        anyhow::bail!("User path does not exist: {}", user_path.display());
    }

    let import_handle = tokio::spawn(discovery::discovery_worker(
        cancel.clone(),
        Arc::clone(&local_db),
        user_path.to_path_buf(),
        user.user_id.clone(),
        config.immich.import_poll_interval,
        event_logger.clone(),
    ));

    let upload_handle = tokio::spawn(uploader::upload_worker(
        cancel.clone(),
        Arc::clone(&local_db),
        Arc::clone(&api),
        user_path.to_path_buf(),
        user.user_id.clone(),
        config.immich.upload_poll_interval,
        event_logger.clone(),
    ));

    let file_handle = tokio::spawn(file_watcher::file_watcher(
        cancel.clone(),
        Arc::clone(&local_db),
        Arc::clone(&api),
        user_path.to_path_buf(),
        user.user_id.clone(),
        config.immich.delete_threshold,
        config.immich.delete_max_age,
        event_logger.clone(),
    ));

    let deletion_handle = tokio::spawn(deletion_watcher::deletion_watcher(
        cancel.clone(),
        Arc::clone(&local_db),
        Arc::clone(&api),
        user_path.to_path_buf(),
        user.user_id.clone(),
        config.immich.delete_poll_interval,
        config.immich.delete_max_age,
        event_logger,
    ));

    tokio::select! {
        r = import_handle => {
            if let Err(e) = r {
                info!("Critical: Discovery worker task failed: {}", e);
            }
        }
        r = upload_handle => {
            if let Err(e) = r {
                info!("Critical: Upload worker task failed: {}", e);
            }
        }
        r = file_handle => {
            if let Err(e) = r {
                info!("Critical: File watcher task failed: {}", e);
            }
        }
        r = deletion_handle => {
            if let Err(e) = r {
                info!("Critical: Deletion watcher task failed: {}", e);
            }
        }
        _ = cancel.cancelled() => {}
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn ignored_dotfiles() {
        assert!(ignored_path(Path::new("/data/.hidden")));
        assert!(ignored_path(Path::new("/data/.DS_Store")));
        assert!(ignored_path(Path::new(".gitignore")));
    }

    #[test]
    fn ignored_directories() {
        let tmp = tempfile::tempdir().unwrap();
        let dir = tmp.path().join("subdir");
        std::fs::create_dir(&dir).unwrap();
        assert!(ignored_path(&dir));
    }

    #[test]
    fn not_ignored_regular_files() {
        let tmp = tempfile::tempdir().unwrap();
        let file = tmp.path().join("photo.jpg");
        std::fs::write(&file, b"data").unwrap();
        assert!(!ignored_path(&file));
    }

    #[test]
    fn not_ignored_nested_file() {
        let tmp = tempfile::tempdir().unwrap();
        let nested = tmp.path().join("album/photo.png");
        std::fs::create_dir_all(nested.parent().unwrap()).unwrap();
        std::fs::write(&nested, b"data").unwrap();
        assert!(!ignored_path(&nested));
    }
}
