use anyhow::Result;
use chrono::Utc;
use log::warn;
use serde::Serialize;
use std::fs::{File, OpenOptions};
use std::io::{BufWriter, Write};
use std::sync::{Arc, Mutex};

pub mod workers {
    pub const DISCOVERY: &str = "discovery";
    pub const UPLOADER: &str = "uploader";
    pub const FILE_WATCHER: &str = "file_watcher";
    pub const DELETION_WATCHER: &str = "deletion_watcher";
}

#[derive(Serialize)]
pub struct Event {
    pub timestamp: String,
    pub worker: String,
    pub event: String,
    pub user_id: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub path: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub asset_id: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub detail: Option<String>,
}

#[derive(Clone)]
pub struct EventLogger {
    inner: Arc<Mutex<BufWriter<File>>>,
}

impl EventLogger {
    pub fn open(path: &str) -> Result<Self> {
        let file = OpenOptions::new().create(true).append(true).open(path)?;
        Ok(Self { inner: Arc::new(Mutex::new(BufWriter::new(file))) })
    }

    pub fn log(
        &self,
        worker: &str,
        event: &str,
        user_id: &str,
        path: Option<&str>,
        asset_id: Option<&str>,
        detail: Option<&str>,
    ) {
        let ev = Event {
            timestamp: Utc::now().to_rfc3339(),
            worker: worker.to_string(),
            event: event.to_string(),
            user_id: user_id.to_string(),
            path: path.map(String::from),
            asset_id: asset_id.map(String::from),
            detail: detail.map(String::from),
        };
        let line = match serde_json::to_string(&ev) {
            Ok(line) => line,
            Err(e) => {
                warn!("Failed to serialize event (worker={}, event={}, user_id={}): {}", worker, event, user_id, e);
                return;
            }
        };

        let mut writer = match self.inner.lock() {
            Ok(writer) => writer,
            Err(e) => {
                warn!("Failed to lock event logger (worker={}, event={}, user_id={}): {}", worker, event, user_id, e);
                return;
            }
        };

        if let Err(e) = writeln!(writer, "{}", line) {
            warn!("Failed to write event log line (worker={}, event={}, user_id={}): {}", worker, event, user_id, e);
            return;
        }

        if let Err(e) = writer.flush() {
            warn!("Failed to flush event log line (worker={}, event={}, user_id={}): {}", worker, event, user_id, e);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn event_serialization_minimal() {
        let ev = Event {
            timestamp: "2025-01-01T00:00:00+00:00".to_string(),
            worker: "discovery".to_string(),
            event: "scan_started".to_string(),
            user_id: "user-1".to_string(),
            path: None,
            asset_id: None,
            detail: None,
        };
        let json = serde_json::to_string(&ev).unwrap();
        assert!(!json.contains("path"));
        assert!(!json.contains("asset_id"));
        assert!(!json.contains("detail"));
        assert!(json.contains("\"event\":\"scan_started\""));
    }

    #[test]
    fn event_serialization_full() {
        let ev = Event {
            timestamp: "2025-01-01T00:00:00+00:00".to_string(),
            worker: "uploader".to_string(),
            event: "file_uploaded".to_string(),
            user_id: "user-1".to_string(),
            path: Some("album/photo.jpg".to_string()),
            asset_id: Some("abc-123".to_string()),
            detail: Some("200 OK".to_string()),
        };
        let json = serde_json::to_string(&ev).unwrap();
        assert!(json.contains("\"path\":\"album/photo.jpg\""));
        assert!(json.contains("\"asset_id\":\"abc-123\""));
        assert!(json.contains("\"detail\":\"200 OK\""));
    }

    #[test]
    fn logger_writes_jsonl() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("events.jsonl");
        let logger = EventLogger::open(path.to_str().unwrap()).unwrap();
        logger.log("test", "test_event", "user-1", Some("a.jpg"), None, None);
        logger.log("test", "test_event_2", "user-1", None, None, Some("detail"));

        let content = std::fs::read_to_string(&path).unwrap();
        let lines: Vec<&str> = content.lines().collect();
        assert_eq!(lines.len(), 2);

        let first: serde_json::Value = serde_json::from_str(lines[0]).unwrap();
        assert_eq!(first["event"], "test_event");
        assert_eq!(first["path"], "a.jpg");

        let second: serde_json::Value = serde_json::from_str(lines[1]).unwrap();
        assert_eq!(second["event"], "test_event_2");
        assert_eq!(second["detail"], "detail");
    }
}
