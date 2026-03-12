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
async fn dry_run_does_not_upload_discovered_file() {
    let (config_path, _tmp) = setup_config();
    let config = Config::load(config_path.to_str().unwrap()).expect("load config");
    let el = event_log_path(&_tmp);

    let user_dir = PathBuf::from(&config.users[0].path);
    let api = ImmichAPI::new(&config.immich.server_url, &config.users[0].user_key);
    clean_slate(&api, &user_dir).await;

    // Create file BEFORE starting — discovery worker picks it up
    create_test_image(&user_dir, "test_dry_disc.jpg");
    let (_guard, _log_lines) = start_sync_service_dry_run(&config_path).await;

    wait_for_event_with_path(&el, "upload_skipped", "test_dry_disc.jpg").await;

    let assets = api.search_assets("test_dry_disc.jpg").await.expect("search");
    assert!(assets.is_empty(), "File should not have been uploaded in dry-run mode");

    let events = read_event_log(&el);

    // Verify file was found by the discovery worker
    assert!(
        !filter_events_by_worker_and_path(&events, "discovery", "test_dry_disc.jpg").is_empty(),
        "File should have been found by the discovery worker.\nEvents:\n{}",
        format_events(&events)
    );

    let skipped = filter_events_with_detail(&events, "upload_skipped", "dry-run");
    assert!(
        !skipped.is_empty(),
        "Expected upload_skipped event with detail 'dry-run'.\nAll events:\n{}",
        format_events(&events)
    );
}

#[tokio::test]
#[serial]
#[ignore = "requires Immich installed and configured on localhost"]
async fn dry_run_does_not_upload_inotify_file() {
    let (config_path, _tmp) = setup_config();
    let config = Config::load(config_path.to_str().unwrap()).expect("load config");
    let el = event_log_path(&_tmp);

    let user_dir = PathBuf::from(&config.users[0].path);
    let api = ImmichAPI::new(&config.immich.server_url, &config.users[0].user_key);
    clean_slate(&api, &user_dir).await;

    // Start service FIRST, then create the file so inotify detects it
    let (_guard, _log_lines) = start_sync_service_dry_run(&config_path).await;
    create_test_image(&user_dir, "test_dry_ino.jpg");

    wait_for_event_with_path(&el, "upload_skipped", "test_dry_ino.jpg").await;

    let assets = api.search_assets("test_dry_ino.jpg").await.expect("search");
    assert!(assets.is_empty(), "File should not have been uploaded in dry-run mode");

    let events = read_event_log(&el);

    // Verify file was detected by inotify (file_watcher)
    assert!(
        !filter_events_by_worker_and_path(&events, "file_watcher", "test_dry_ino.jpg").is_empty(),
        "File should have been detected by the file watcher.\nEvents:\n{}",
        format_events(&events)
    );

    let skipped = filter_events_with_detail(&events, "upload_skipped", "dry-run");
    assert!(
        !skipped.is_empty(),
        "Expected upload_skipped event with detail 'dry-run'.\nAll events:\n{}",
        format_events(&events)
    );
}

#[tokio::test]
#[serial]
#[ignore = "requires Immich installed and configured on localhost"]
async fn dry_run_does_not_delete_from_immich() {
    let (config_path, _tmp) = setup_config();
    let config = Config::load(config_path.to_str().unwrap()).expect("load config");
    let el = event_log_path(&_tmp);

    let user_dir = PathBuf::from(&config.users[0].path);
    let api = ImmichAPI::new(&config.immich.server_url, &config.users[0].user_key);
    clean_slate(&api, &user_dir).await;

    // First, do a normal run to upload the file
    let image_path = create_test_image(&user_dir, "test_dry_del.jpg");
    let (normal_guard, _normal_logs) = start_sync_service(&config_path).await;
    let _asset_id = wait_for_asset(&api, "test_dry_del.jpg").await;
    drop(normal_guard);

    // Now restart in dry-run mode and delete the local file
    let (_guard, _log_lines) = start_sync_service_dry_run(&config_path).await;
    std::fs::remove_file(&image_path).expect("remove local file");

    // Wait for the file watcher to notice and log the dry-run skip
    wait_for_event_with_path(&el, "delete_skipped", "test_dry_del.jpg").await;

    // Give some extra time to make sure no actual delete happens
    sleep(Duration::from_secs(5)).await;

    // Asset must still exist in Immich
    let assets = api.search_assets("test_dry_del.jpg").await.expect("search");
    assert!(!assets.is_empty(), "Asset should still exist in Immich in dry-run mode");

    let events = read_event_log(&el);
    let skipped = filter_events_with_detail(&events, "delete_skipped", "dry-run");
    assert!(
        !skipped.is_empty(),
        "Expected delete_skipped event with detail 'dry-run'.\nAll events:\n{}",
        format_events(&events)
    );
}

#[tokio::test]
#[serial]
#[ignore = "requires Immich installed and configured on localhost"]
async fn dry_run_does_not_delete_local_file() {
    let (config_path, _tmp) = setup_config();
    let config = Config::load(config_path.to_str().unwrap()).expect("load config");
    let el = event_log_path(&_tmp);

    let user_dir = PathBuf::from(&config.users[0].path);
    let api = ImmichAPI::new(&config.immich.server_url, &config.users[0].user_key);
    clean_slate(&api, &user_dir).await;

    // Normal run: upload the file
    let image_path = create_test_image(&user_dir, "test_dry_local.jpg");
    let (normal_guard, _normal_logs) = start_sync_service(&config_path).await;
    let asset_id = wait_for_asset(&api, "test_dry_local.jpg").await;
    drop(normal_guard);

    // Delete from Immich
    api.delete_asset(&asset_id).await.expect("delete from immich");
    api.empty_trash().await.expect("empty trash");

    // Restart in dry-run mode
    let (_guard, _log_lines) = start_sync_service_dry_run(&config_path).await;

    // Wait for deletion watcher to detect the remote delete
    wait_for_event(&el, "local_delete_skipped").await;

    // Local file must still exist
    assert!(image_path.exists(), "Local file should not have been deleted in dry-run mode");

    let events = read_event_log(&el);
    let skipped = filter_events_with_detail(&events, "local_delete_skipped", "dry-run");
    assert!(
        !skipped.is_empty(),
        "Expected local_delete_skipped event with detail 'dry-run'.\nAll events:\n{}",
        format_events(&events)
    );
}
