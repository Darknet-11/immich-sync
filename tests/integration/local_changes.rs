use std::io::Write;
use std::path::PathBuf;
use std::time::Duration;

use serial_test::serial;
use sync_service::api::ImmichAPI;
use sync_service::config::Config;
use tokio::time::sleep;

use crate::common::*;

#[tokio::test]
#[serial]
#[ignore = "requires Immich installed and configured on localhost"]
async fn inotify_detects_and_uploads_new_file() {
    let (config_path, _tmp) = setup_config();
    let config = Config::load(config_path.to_str().unwrap()).expect("load config");

    let user_dir = PathBuf::from(&config.users[0].path);
    let api = ImmichAPI::new(&config.immich.server_url, &config.users[0].user_key);
    clean_slate(&api, &user_dir).await;

    // Start sync service FIRST, then create the image so inotify detects it
    let (_guard, log_lines) = start_sync_service(&config_path).await;
    create_test_image(&user_dir, "test_inotify.jpg");
    let _asset_id = wait_for_asset(&api, "test_inotify.jpg").await;

    // Verify the file was picked up by inotify, not discovery
    let logs = log_lines.lock().await;
    assert!(
        logs.iter().any(|l| l.contains("test_inotify.jpg") && l.contains("added")),
        "File should have been detected by inotify. Logs:\n{}",
        logs.join("\n")
    );
}

#[tokio::test]
#[serial]
#[ignore = "requires Immich installed and configured on localhost"]
async fn local_delete_triggers_remote_delete() {
    let (config_path, _tmp) = setup_config();
    let config = Config::load(config_path.to_str().unwrap()).expect("load config");

    let user_dir = PathBuf::from(&config.users[0].path);
    let api = ImmichAPI::new(&config.immich.server_url, &config.users[0].user_key);
    clean_slate(&api, &user_dir).await;

    let (_guard, log_lines) = start_sync_service(&config_path).await;

    // Create file after service start (inotify path) and wait for upload
    let image_path = create_test_image(&user_dir, "test_local_del.jpg");
    let _asset_id = wait_for_asset(&api, "test_local_del.jpg").await;

    // Delete the local file — inotify REMOVE fires
    std::fs::remove_file(&image_path).expect("remove local file");

    // File watcher should detect removal, find asset_id in DB, and delete from Immich
    wait_for_log(&log_lines, "deleting asset in Immich").await;

    // Verify the asset is gone from Immich
    wait_for_no_asset(&api, "test_local_del.jpg").await;
}

#[tokio::test]
#[serial]
#[ignore = "requires Immich installed and configured on localhost"]
async fn modify_file_rehashes_and_reuploads() {
    let (config_path, _tmp) = setup_config();
    let config = Config::load(config_path.to_str().unwrap()).expect("load config");

    let user_dir = PathBuf::from(&config.users[0].path);
    let api = ImmichAPI::new(&config.immich.server_url, &config.users[0].user_key);
    clean_slate(&api, &user_dir).await;

    let (_guard, log_lines) = start_sync_service(&config_path).await;

    // Create file (inotify CREATE) and wait for upload
    let image_path = create_test_image(&user_dir, "test_modify.jpg");
    wait_for_asset(&api, "test_modify.jpg").await;
    wait_for_log(&log_lines, "Uploaded test_modify.jpg").await;

    // Overwrite with different content (inotify MODIFY) — different hash
    {
        let mut f = std::fs::File::create(&image_path).expect("overwrite image");
        f.write_all(TEST_JPEG).expect("write jpeg");
        f.write_all(b"modified_content").expect("write suffix");
    }

    // Wait for "modified" log (MODIFY event, not CREATE)
    wait_for_log(&log_lines, "test_modify.jpg modified").await;

    // Wait for re-upload (second "Uploaded test_modify.jpg")
    for _ in 1..=60 {
        let logs = log_lines.lock().await;
        let upload_count = logs.iter().filter(|l| l.contains("Uploaded test_modify.jpg")).count();
        if upload_count >= 2 {
            return;
        }
        drop(logs);
        sleep(Duration::from_secs(1)).await;
    }
    panic!("Second upload of test_modify.jpg did not occur within 60s");
}

#[tokio::test]
#[serial]
#[ignore = "requires Immich installed and configured on localhost"]
async fn rapid_create_delete_before_upload() {
    let (config_path, _tmp) = setup_config();
    let config = Config::load(config_path.to_str().unwrap()).expect("load config");

    let user_dir = PathBuf::from(&config.users[0].path);
    let api = ImmichAPI::new(&config.immich.server_url, &config.users[0].user_key);
    clean_slate(&api, &user_dir).await;

    // Start service first
    let (_guard, log_lines) = start_sync_service(&config_path).await;

    // Create file then immediately delete — service may try to hash a disappeared file
    let image_path = create_test_image(&user_dir, "test_rapid_delete.jpg");
    std::fs::remove_file(&image_path).expect("remove file immediately");

    // Wait long enough for the service to process the events (a couple of poll cycles)
    sleep(Duration::from_secs(15)).await;

    // The service should still be running (no crash/panic)
    // Verify by creating another file and checking it gets uploaded
    create_test_image_with_suffix(&user_dir, "test_still_alive.jpg", b"alive");
    let _asset_id = wait_for_asset(&api, "test_still_alive.jpg").await;

    // Verify service saw the rapid file
    let logs = log_lines.lock().await;
    assert!(
        logs.iter().any(|l| l.contains("test_rapid_delete.jpg")),
        "Service should have attempted to process the rapidly deleted file. Logs:\n{}",
        logs.join("\n")
    );
}
