use std::path::PathBuf;
use sync_service::api::ImmichAPI;
use sync_service::config::Config;

use crate::common::*;

/// Upload a file and verify event log contains discovery + upload events.
#[tokio::test]
#[serial_test::serial]
#[ignore = "requires Immich installed and configured on localhost"]
async fn event_log_records_upload() {
    let (config_path, _tmp) = setup_config();
    let config = Config::load(config_path.to_str().unwrap()).expect("load config");
    let el = event_log_path(&_tmp);

    let user_dir = PathBuf::from(&config.users[0].path);
    let api = ImmichAPI::new(&config.immich.server_url, &config.users[0].user_key);
    clean_slate(&api, &user_dir).await;

    create_test_image(&user_dir, "event_test.jpg");

    let (_guard, _log_lines) = start_sync_service(&config_path).await;

    wait_for_event_with_path(&el, "file_uploaded", "event_test.jpg").await;

    let events = read_event_log(&el);
    assert!(!events.is_empty(), "Event log should not be empty");

    let discovered = filter_events(&events, "file_discovered");
    assert!(
        !discovered.is_empty() || !filter_events(&events, "file_queued").is_empty(),
        "Expected a file_discovered or file_queued event"
    );

    let uploaded = filter_events(&events, "file_uploaded");
    assert!(!uploaded.is_empty(), "Expected a file_uploaded event");

    let upload_ev = uploaded[0];
    assert_eq!(upload_ev["worker"].as_str(), Some("uploader"));
    assert!(upload_ev["asset_id"].as_str().is_some(), "file_uploaded should have asset_id");
}

/// Verify that scan_started and scan_completed events are emitted.
#[tokio::test]
#[serial_test::serial]
#[ignore = "requires Immich installed and configured on localhost"]
async fn event_log_records_scan_cycle() {
    let (config_path, _tmp) = setup_config();
    let config = Config::load(config_path.to_str().unwrap()).expect("load config");
    let el = event_log_path(&_tmp);

    let user_dir = PathBuf::from(&config.users[0].path);
    let api = ImmichAPI::new(&config.immich.server_url, &config.users[0].user_key);
    clean_slate(&api, &user_dir).await;

    let (_guard, _log_lines) = start_sync_service(&config_path).await;

    // Wait for discovery to complete at least one cycle
    wait_for_event(&el, "scan_completed").await;

    let events = read_event_log(&el);
    let started = filter_events(&events, "scan_started");
    let completed = filter_events(&events, "scan_completed");
    assert!(!started.is_empty(), "Expected scan_started event");
    assert!(!completed.is_empty(), "Expected scan_completed event");
}
