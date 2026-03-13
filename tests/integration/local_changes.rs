use std::io::Write;
use std::path::PathBuf;
use std::time::Duration;

use serial_test::serial;
use sync_service::api::ImmichAPI;
use sync_service::config::Config;
use tokio::time::sleep;

use crate::common::*;

/// Simulate Syncthing's delete behavior: rename file to .trashed-<id>-<name>
fn syncthing_trash(path: &std::path::Path) {
    let dir = path.parent().unwrap();
    let name = path.file_name().unwrap().to_str().unwrap();
    let trashed = dir.join(format!(".trashed-1234567890-{}", name));
    std::fs::rename(path, &trashed).expect("rename to .trashed");
}

#[tokio::test]
#[serial]
#[ignore = "requires Immich installed and configured on localhost"]
async fn inotify_detects_and_uploads_new_file() {
    let (config_path, _tmp) = setup_config();
    let config = Config::load(config_path.to_str().unwrap()).expect("load config");
    let el = event_log_path(&_tmp);

    let user_dir = PathBuf::from(&config.users[0].path);
    let api = ImmichAPI::new(&config.immich.server_url, &config.users[0].user_key);
    clean_slate(&api, &user_dir).await;

    // Start sync service FIRST, then create the image so inotify detects it
    let (_guard, _log_lines) = start_sync_service(&config_path).await;
    create_test_image(&user_dir, "test_inotify.jpg");
    let _asset_id = wait_for_asset(&api, "test_inotify.jpg").await;

    // Verify the file was picked up by inotify
    wait_for_event_with_path(&el, "file_detected", "test_inotify.jpg").await;
}

#[tokio::test]
#[serial]
#[ignore = "requires Immich installed and configured on localhost"]
async fn local_delete_triggers_remote_delete() {
    let (config_path, _tmp) = setup_config();
    let config = Config::load(config_path.to_str().unwrap()).expect("load config");
    let el = event_log_path(&_tmp);

    let user_dir = PathBuf::from(&config.users[0].path);
    let api = ImmichAPI::new(&config.immich.server_url, &config.users[0].user_key);
    clean_slate(&api, &user_dir).await;

    let (_guard, _log_lines) = start_sync_service(&config_path).await;

    // Create file after service start (inotify path) and wait for upload
    let image_path = create_test_image(&user_dir, "test_local_del.jpg");
    let _asset_id = wait_for_asset(&api, "test_local_del.jpg").await;

    // Delete the local file — inotify REMOVE fires
    std::fs::remove_file(&image_path).expect("remove local file");

    // File watcher should detect removal, find asset_id in DB, and delete from Immich
    wait_for_event_with_path(&el, "delete_propagated", "test_local_del.jpg").await;

    // Verify the asset is gone from Immich
    wait_for_no_asset(&api, "test_local_del.jpg").await;
}

#[tokio::test]
#[serial]
#[ignore = "requires Immich installed and configured on localhost"]
async fn modify_file_rehashes_and_reuploads() {
    let (config_path, _tmp) = setup_config();
    let config = Config::load(config_path.to_str().unwrap()).expect("load config");
    let el = event_log_path(&_tmp);

    let user_dir = PathBuf::from(&config.users[0].path);
    let api = ImmichAPI::new(&config.immich.server_url, &config.users[0].user_key);
    clean_slate(&api, &user_dir).await;

    let (_guard, _log_lines) = start_sync_service(&config_path).await;

    // Create file (inotify CREATE) and wait for upload
    let image_path = create_test_image(&user_dir, "test_modify.jpg");
    wait_for_asset(&api, "test_modify.jpg").await;
    wait_for_event_with_path(&el, "file_uploaded", "test_modify.jpg").await;

    // Overwrite with different content (inotify MODIFY) — different hash
    {
        let mut f = std::fs::File::create(&image_path).expect("overwrite image");
        f.write_all(TEST_JPEG).expect("write jpeg");
        f.write_all(b"modified_content").expect("write suffix");
    }

    // Wait for "modified" detection event
    for _ in 1..=60 {
        let events = read_event_log(&el);
        let detected = filter_events_with_path(&events, "file_detected", "test_modify.jpg");
        if detected.iter().any(|e| e["detail"].as_str() == Some("modified")) {
            break;
        }
        sleep(Duration::from_secs(1)).await;
    }

    // Wait for re-upload (second file_uploaded event for test_modify.jpg)
    wait_for_n_events_with_path(&el, "file_uploaded", "test_modify.jpg", 2).await;
}

#[tokio::test]
#[serial]
#[ignore = "requires Immich installed and configured on localhost"]
async fn rapid_create_delete_before_upload() {
    let (config_path, _tmp) = setup_config();
    let config = Config::load(config_path.to_str().unwrap()).expect("load config");
    let _el = event_log_path(&_tmp);

    let user_dir = PathBuf::from(&config.users[0].path);
    let api = ImmichAPI::new(&config.immich.server_url, &config.users[0].user_key);
    clean_slate(&api, &user_dir).await;

    // Start service first
    let (_guard, _log_lines) = start_sync_service(&config_path).await;

    // Create file then immediately delete — service may try to hash a disappeared file
    let image_path = create_test_image(&user_dir, "test_rapid_delete.jpg");
    std::fs::remove_file(&image_path).expect("remove file immediately");

    // Wait long enough for the service to process the events (a couple of poll cycles).
    // With debouncing (2s window), the rapid create→delete merges into a single Remove
    // event, so the service never hashes the vanished file.
    sleep(Duration::from_secs(15)).await;

    // The service should still be running (no crash/panic)
    // Verify by creating another file and checking it gets uploaded
    create_test_image_with_suffix(&user_dir, "test_still_alive.jpg", b"alive");
    let _asset_id = wait_for_asset(&api, "test_still_alive.jpg").await;
}

/// Syncthing deletes files by renaming them to `.trashed-<id>-<name>` rather
/// than unlinking. This produces a MOVED_FROM inotify event (reported as
/// Modify by the notify crate) with no corresponding DELETE. The file watcher
/// must detect that the file no longer exists and treat it as a removal.
#[tokio::test]
#[serial]
#[ignore = "requires Immich installed and configured on localhost"]
async fn syncthing_trash_rename_triggers_remote_delete() {
    let (config_path, _tmp) = setup_config();
    let config = Config::load(config_path.to_str().unwrap()).expect("load config");
    let el = event_log_path(&_tmp);

    let user_dir = PathBuf::from(&config.users[0].path);
    let api = ImmichAPI::new(&config.immich.server_url, &config.users[0].user_key);
    clean_slate(&api, &user_dir).await;

    let (_guard, _log_lines) = start_sync_service(&config_path).await;

    // Create file and wait for upload
    let image_path = create_test_image(&user_dir, "test_syncthing_trash.jpg");
    let _asset_id = wait_for_asset(&api, "test_syncthing_trash.jpg").await;

    // Simulate Syncthing delete: rename to .trashed-* (no DELETE event emitted)
    syncthing_trash(&image_path);

    // File watcher should see Modify(Name), detect file is gone, and route to handle_remove
    wait_for_event_with_path(&el, "delete_propagated", "test_syncthing_trash.jpg").await;

    // Verify the asset is gone from Immich
    wait_for_no_asset(&api, "test_syncthing_trash.jpg").await;
}
