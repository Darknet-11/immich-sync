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
async fn duplicate_content_discovered_and_linked() {
    let (config_path, _tmp) = setup_config();
    let config = Config::load(config_path.to_str().unwrap()).expect("load config");

    let user_dir = PathBuf::from(&config.users[0].path);
    let api = ImmichAPI::new(&config.immich.server_url, &config.users[0].user_key);
    clean_slate(&api, &user_dir).await;

    // Create two files with identical content (same suffix → same SHA1) before service start
    let shared_suffix = b"shared_dedup_content";
    create_test_image_with_suffix(&user_dir, "dedup_a.jpg", shared_suffix);
    create_test_image_with_suffix(&user_dir, "dedup_b.jpg", shared_suffix);

    let (_guard, log_lines) = start_sync_service(&config_path).await;

    // Wait for bulk check to process both
    wait_for_log(&log_lines, "Checking 2 assets").await;

    // Wait for the one upload to complete and the duplicate to be deduplicated
    wait_for_log(&log_lines, "Uploaded").await;
    wait_for_log(&log_lines, "deduplicated locally").await;

    // Give extra time for upload processing to complete
    sleep(Duration::from_secs(5)).await;

    // At most one "uploading" log line — the second should be deduped
    let logs = log_lines.lock().await;
    let upload_count = logs
        .iter()
        .filter(|l| {
            (l.contains("dedup_a.jpg") || l.contains("dedup_b.jpg")) && l.contains("not found in Immich, uploading")
        })
        .count();
    assert!(
        upload_count <= 1,
        "Expected at most 1 upload for identical-content files, got {}. Logs:\n{}",
        upload_count,
        logs.join("\n")
    );
}

#[tokio::test]
#[serial]
#[ignore = "requires Immich installed and configured on localhost"]
async fn duplicate_content_inotify_and_linked() {
    let (config_path, _tmp) = setup_config();
    let config = Config::load(config_path.to_str().unwrap()).expect("load config");

    let user_dir = PathBuf::from(&config.users[0].path);
    let api = ImmichAPI::new(&config.immich.server_url, &config.users[0].user_key);
    clean_slate(&api, &user_dir).await;

    // Start service FIRST, then create files (inotify path)
    let (_guard, log_lines) = start_sync_service(&config_path).await;

    let shared_suffix = b"shared_inotify_dedup";
    create_test_image_with_suffix(&user_dir, "inotify_dup_a.jpg", shared_suffix);
    create_test_image_with_suffix(&user_dir, "inotify_dup_b.jpg", shared_suffix);

    // Both should be detected by inotify
    wait_for_log(&log_lines, "inotify_dup_a.jpg added").await;
    wait_for_log(&log_lines, "inotify_dup_b.jpg added").await;

    // Wait for the one upload to complete and the duplicate to be deduplicated
    wait_for_log(&log_lines, "Uploaded").await;
    wait_for_log(&log_lines, "deduplicated locally").await;

    // Give time for upload processing to complete
    sleep(Duration::from_secs(5)).await;

    // At most one actual upload — the second should be deduped by bulk-upload-check
    let logs = log_lines.lock().await;
    let upload_count = logs
        .iter()
        .filter(|l| {
            (l.contains("inotify_dup_a.jpg") || l.contains("inotify_dup_b.jpg"))
                && l.contains("not found in Immich, uploading")
        })
        .count();
    assert!(
        upload_count <= 1,
        "Expected at most 1 upload for identical-content files, got {}. Logs:\n{}",
        upload_count,
        logs.join("\n")
    );
}

#[tokio::test]
#[serial]
#[ignore = "requires Immich installed and configured on localhost"]
async fn duplicate_content_across_subdirectories() {
    let (config_path, _tmp) = setup_config();
    let config = Config::load(config_path.to_str().unwrap()).expect("load config");

    let user_dir = PathBuf::from(&config.users[0].path);
    let api = ImmichAPI::new(&config.immich.server_url, &config.users[0].user_key);
    clean_slate(&api, &user_dir).await;

    // Create identical files in different subdirectories before service start
    let album1 = user_dir.join("album1");
    let album2 = user_dir.join("album2");
    std::fs::create_dir_all(&album1).expect("create album1");
    std::fs::create_dir_all(&album2).expect("create album2");

    let shared_suffix = b"shared_subdir_dedup";
    create_test_image_with_suffix(&album1, "photo.jpg", shared_suffix);
    create_test_image_with_suffix(&album2, "photo.jpg", shared_suffix);

    let (_guard, log_lines) = start_sync_service(&config_path).await;

    // Wait for bulk check and upload processing
    wait_for_log(&log_lines, "Checking 2 assets").await;
    wait_for_log(&log_lines, "Uploaded").await;
    wait_for_log(&log_lines, "deduplicated locally").await;

    // Give extra time for processing
    sleep(Duration::from_secs(5)).await;

    // Verify at most one upload
    let logs = log_lines.lock().await;
    let upload_count =
        logs.iter().filter(|l| l.contains("photo.jpg") && l.contains("not found in Immich, uploading")).count();
    assert!(
        upload_count <= 1,
        "Expected at most 1 upload for identical-content files across subdirs, got {}. Logs:\n{}",
        upload_count,
        logs.join("\n")
    );
}
