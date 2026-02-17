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
async fn threshold_zero_blocks_local_to_remote_delete() {
    // Config: delete_threshold = 0 means file_age_days < 0 is never true → skips all
    let (config_path, _tmp) =
        setup_config_with_overrides(&ConfigOverrides { delete_threshold: 0, ..Default::default() });
    let config = Config::load(config_path.to_str().unwrap()).expect("load config");

    let user_dir = PathBuf::from(&config.users[0].path);
    let api = ImmichAPI::new(&config.immich.server_url, &config.users[0].user_key);
    clean_slate(&api, &user_dir).await;

    // Start service, create file via inotify, wait for upload
    let (_guard, log_lines) = start_sync_service(&config_path).await;
    let image_path = create_test_image(&user_dir, "test_threshold_zero.jpg");
    let _asset_id = wait_for_asset(&api, "test_threshold_zero.jpg").await;

    // Delete the local file
    std::fs::remove_file(&image_path).expect("remove local file");

    // File watcher should see age 0 >= threshold 0, so it skips the Immich delete
    wait_for_log(&log_lines, "exceeds threshold of 0 days, skipping delete").await;

    // Asset should still be in Immich
    sleep(Duration::from_secs(5)).await;
    let assets = api.search_assets("test_threshold_zero.jpg").await.expect("search");
    assert!(!assets.is_empty(), "Asset should still exist in Immich when delete_threshold=0");
}

#[tokio::test]
#[serial]
#[ignore = "requires Immich installed and configured on localhost"]
async fn delete_max_age_blocks_local_to_remote_delete() {
    let (config_path, _tmp) = setup_config();
    let config = Config::load(config_path.to_str().unwrap()).expect("load config");

    let user_dir = PathBuf::from(&config.users[0].path);
    let api = ImmichAPI::new(&config.immich.server_url, &config.users[0].user_key);
    clean_slate(&api, &user_dir).await;

    // Start service, create file via inotify, wait for upload
    let (_guard, log_lines) = start_sync_service(&config_path).await;
    let image_path = create_test_image(&user_dir, "test_max_age_local.jpg");
    let _asset_id = wait_for_asset(&api, "test_max_age_local.jpg").await;

    // Manipulate the DB: set created_at to a very old date (~9500 days old)
    let db_path = _tmp.path().join("sync-service.db");
    set_asset_created_at(&db_path, "test_max_age_local.jpg", "2000-01-01T00:00:00Z");

    // Delete the local file
    std::fs::remove_file(&image_path).expect("remove local file");

    // File watcher should see age > delete_max_age (3650) → "unrealistic age, skipping delete"
    wait_for_log(&log_lines, "unrealistic age").await;

    // Asset should still be in Immich
    sleep(Duration::from_secs(5)).await;
    let assets = api.search_assets("test_max_age_local.jpg").await.expect("search");
    assert!(!assets.is_empty(), "Asset should still exist in Immich when age exceeds delete_max_age");
}

#[tokio::test]
#[serial]
#[ignore = "requires Immich installed and configured on localhost"]
async fn delete_max_age_blocks_remote_to_local_delete() {
    let (config_path, _tmp) = setup_config();
    let config = Config::load(config_path.to_str().unwrap()).expect("load config");

    let user_dir = PathBuf::from(&config.users[0].path);
    let api = ImmichAPI::new(&config.immich.server_url, &config.users[0].user_key);
    clean_slate(&api, &user_dir).await;

    // Create file before service start (discovery path) for reliable DB entry
    let image_path = create_test_image(&user_dir, "test_max_age_remote.jpg");
    let (_guard, log_lines) = start_sync_service(&config_path).await;
    let asset_id = wait_for_asset(&api, "test_max_age_remote.jpg").await;

    // Manipulate the DB: set created_at to a very old date
    let db_path = _tmp.path().join("sync-service.db");
    set_asset_created_at(&db_path, "test_max_age_remote.jpg", "2000-01-01T00:00:00Z");

    // Delete from Immich (trash + empty)
    api.delete_asset(&asset_id).await.expect("delete asset");
    api.empty_trash().await.expect("empty trash");

    // Deletion watcher should see age > delete_max_age → skip local delete
    wait_for_log(&log_lines, "unrealistic age").await;

    // Wait a few cycles to make sure it doesn't get deleted anyway
    sleep(Duration::from_secs(15)).await;

    // Local file should still exist
    assert!(image_path.exists(), "Local file should NOT be deleted when asset age exceeds delete_max_age");
}

#[tokio::test]
#[serial]
#[ignore = "requires Immich installed and configured on localhost"]
async fn delete_threshold_does_not_affect_remote_to_local() {
    // Config: delete_threshold = 0 blocks all local→remote deletes,
    // but should NOT block remote→local deletes
    let (config_path, _tmp) =
        setup_config_with_overrides(&ConfigOverrides { delete_threshold: 0, ..Default::default() });
    let config = Config::load(config_path.to_str().unwrap()).expect("load config");

    let user_dir = PathBuf::from(&config.users[0].path);
    let api = ImmichAPI::new(&config.immich.server_url, &config.users[0].user_key);
    clean_slate(&api, &user_dir).await;

    // Create file before service start, wait for upload
    let image_path = create_test_image(&user_dir, "test_threshold_remote.jpg");
    let (_guard, _log_lines) = start_sync_service(&config_path).await;
    let asset_id = wait_for_asset(&api, "test_threshold_remote.jpg").await;

    // Delete from Immich (trash + empty)
    api.delete_asset(&asset_id).await.expect("delete asset");
    api.empty_trash().await.expect("empty trash");

    // Deletion watcher should still delete the local file (threshold doesn't apply to remote→local)
    wait_for_file_removed(&image_path).await;
}
