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
async fn dotfile_ignored_by_all_workers() {
    let (config_path, _tmp) = setup_config();
    let config = Config::load(config_path.to_str().unwrap()).expect("load config");
    let el = event_log_path(&_tmp);

    let user_dir = PathBuf::from(&config.users[0].path);
    let api = ImmichAPI::new(&config.immich.server_url, &config.users[0].user_key);
    clean_slate(&api, &user_dir).await;

    // Create both a dotfile and a visible file before starting
    create_test_image(&user_dir, ".hidden_photo.jpg");
    create_test_image_with_suffix(&user_dir, "visible_photo.jpg", b"visible");

    let (_guard, _log_lines) = start_sync_service(&config_path).await;

    // The visible file proves the service is working
    let _visible_id = wait_for_asset(&api, "visible_photo.jpg").await;

    // Extra wait to give any hypothetical dotfile processing time to complete
    sleep(Duration::from_secs(10)).await;

    // Dotfile should NOT appear in Immich
    let hidden_assets = api.search_assets(".hidden_photo").await.unwrap_or_default();
    assert!(hidden_assets.is_empty(), "Dotfile should not be uploaded to Immich");

    // No events should reference the dotfile
    let events = read_event_log(&el);
    let dotfile_events: Vec<_> = events
        .iter()
        .filter(|e| e["path"].as_str().is_some_and(|p| p.contains(".hidden_photo.jpg")))
        .collect();
    assert!(
        dotfile_events.is_empty(),
        "Events should not reference dotfile.\nMatching events:\n{}",
        dotfile_events.iter().map(|e| serde_json::to_string(e).unwrap_or_default()).collect::<Vec<_>>().join("\n")
    );
}
