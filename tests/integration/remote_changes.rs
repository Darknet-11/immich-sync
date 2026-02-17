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
async fn remote_delete_removes_local_file() {
    let (config_path, _tmp) = setup_config();
    let config = Config::load(config_path.to_str().unwrap()).expect("load config");

    let user_dir = PathBuf::from(&config.users[0].path);
    let api = ImmichAPI::new(&config.immich.server_url, &config.users[0].user_key);
    clean_slate(&api, &user_dir).await;

    let image_path = create_test_image(&user_dir, "test_remote_del.jpg");
    let (_guard, _log_lines) = start_sync_service(&config_path).await;
    let asset_id = wait_for_asset(&api, "test_remote_del.jpg").await;

    // Delete asset from Immich and verify local file is removed
    api.delete_asset(&asset_id).await.expect("delete asset");
    api.empty_trash().await.expect("empty trash");
    wait_for_file_removed(&image_path).await;
}

#[tokio::test]
#[serial]
#[ignore = "requires Immich installed and configured on localhost"]
async fn remote_delete_removes_local_file_inotify_path() {
    let (config_path, _tmp) = setup_config();
    let config = Config::load(config_path.to_str().unwrap()).expect("load config");

    let user_dir = PathBuf::from(&config.users[0].path);
    let api = ImmichAPI::new(&config.immich.server_url, &config.users[0].user_key);
    clean_slate(&api, &user_dir).await;

    // Start sync service FIRST, then create the image so inotify detects it
    let (_guard, _log_lines) = start_sync_service(&config_path).await;
    let image_path = create_test_image(&user_dir, "test_remote_del_inotify.jpg");
    let asset_id = wait_for_asset(&api, "test_remote_del_inotify.jpg").await;

    // Delete asset from Immich and verify local file is removed
    api.delete_asset(&asset_id).await.expect("delete asset");
    api.empty_trash().await.expect("empty trash");
    wait_for_file_removed(&image_path).await;
}

#[tokio::test]
#[serial]
#[ignore = "requires Immich installed and configured on localhost"]
async fn trash_without_empty_does_not_remove_local_file() {
    let (config_path, _tmp) = setup_config();
    let config = Config::load(config_path.to_str().unwrap()).expect("load config");

    let user_dir = PathBuf::from(&config.users[0].path);
    let api = ImmichAPI::new(&config.immich.server_url, &config.users[0].user_key);
    clean_slate(&api, &user_dir).await;

    let image_path = create_test_image(&user_dir, "test_trash_only.jpg");
    let (_guard, _log_lines) = start_sync_service(&config_path).await;
    let asset_id = wait_for_asset(&api, "test_trash_only.jpg").await;

    // Move to trash but do NOT empty — asset is still in Immich (just trashed)
    api.delete_asset(&asset_id).await.expect("delete asset (trash)");
    // Deliberately skip: api.empty_trash()

    // Wait 20s (4+ deletion_watcher cycles at poll_interval=5)
    sleep(Duration::from_secs(20)).await;

    // Local file should still exist — bulk-upload-check returns "reject" for trashed assets
    assert!(
        image_path.exists(),
        "Local file should NOT be deleted when asset is only trashed (not permanently deleted)"
    );
}
