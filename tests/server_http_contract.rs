use std::net::SocketAddr;
use std::path::PathBuf;
use std::process::{Child, Stdio};
use std::time::Duration;

use axum::extract::Json;
use axum::http::HeaderMap;
use axum::routing::{any, post};
use axum::{Router, body::Body, http::StatusCode};
use nanoid::nanoid;
use reqwest::Client;
use serde_json::Value;
use tokio::net::TcpListener;
use tokio::sync::oneshot;

fn temp_db_path(prefix: &str) -> PathBuf {
    std::env::temp_dir().join(format!("{}-{}.db", prefix, nanoid!(8)))
}

struct BackendGuard {
    child: Child,
    db_path: PathBuf,
}

impl Drop for BackendGuard {
    fn drop(&mut self) {
        let _ = self.child.kill();
        let _ = self.child.wait();
        let _ = std::fs::remove_file(&self.db_path);
    }
}

async fn spawn_mock_upstream(
    expected_api_key: String,
) -> (SocketAddr, oneshot::Receiver<(String, String)>) {
    let (tx, rx) = oneshot::channel::<(String, String)>();
    let tx = std::sync::Arc::new(std::sync::Mutex::new(Some(tx)));
    let app = Router::new()
        .route(
            "/search",
            post({
                let tx = tx.clone();
                move |headers: HeaderMap, Json(body): Json<Value>| {
                    let tx = tx.clone();
                    let expected_api_key = expected_api_key.clone();
                    async move {
                        let forwarded_auth = headers
                            .get(axum::http::header::AUTHORIZATION)
                            .and_then(|v| v.to_str().ok())
                            .unwrap_or_default()
                            .to_string();
                        let forwarded_api_key = body
                            .get("api_key")
                            .and_then(|v| v.as_str())
                            .unwrap_or_default()
                            .to_string();
                        if let Ok(mut guard) = tx.lock()
                            && let Some(ch) = guard.take()
                        {
                            let _ = ch.send((forwarded_auth.clone(), forwarded_api_key.clone()));
                        }

                        if forwarded_auth != format!("Bearer {expected_api_key}")
                            || forwarded_api_key != expected_api_key
                        {
                            return (
                                StatusCode::UNAUTHORIZED,
                                Json(serde_json::json!({"error":"bad credentials"})),
                            );
                        }
                        (
                            StatusCode::OK,
                            Json(serde_json::json!({"status":200,"results":[]})),
                        )
                    }
                }
            }),
        )
        .route("/mcp", any(|| async { (StatusCode::OK, Body::from("{}")) }));

    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(async move {
        axum::serve(listener, app.into_make_service())
            .await
            .unwrap();
    });
    (addr, rx)
}

async fn wait_for_health(port: u16) {
    let client = Client::new();
    for _ in 0..80 {
        if let Ok(resp) = client
            .get(format!("http://127.0.0.1:{port}/health"))
            .timeout(Duration::from_millis(300))
            .send()
            .await
            && resp.status().is_success()
        {
            return;
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    panic!("server did not become healthy in time");
}

fn spawn_backend_process(port: u16, upstream_addr: SocketAddr, db_path: PathBuf) -> BackendGuard {
    let binary = env!("CARGO_BIN_EXE_tavily-hikari");
    let child = std::process::Command::new(binary)
        .arg("--bind")
        .arg("127.0.0.1")
        .arg("--port")
        .arg(port.to_string())
        .arg("--db-path")
        .arg(&db_path)
        .arg("--keys")
        .arg("tvly-test-key")
        .arg("--upstream")
        .arg(format!("http://{upstream_addr}/mcp"))
        .arg("--usage-base")
        .arg(format!("http://{upstream_addr}"))
        .arg("--dev-open-admin")
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .spawn()
        .expect("spawn backend");
    BackendGuard { child, db_path }
}

#[tokio::test]
async fn health_endpoint_is_stable_after_modularization() {
    let (upstream_addr, _rx) = spawn_mock_upstream("tvly-test-key".to_string()).await;

    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let port = listener.local_addr().unwrap().port();
    drop(listener);

    let db_path = temp_db_path("server-http-contract-health");
    let _backend = spawn_backend_process(port, upstream_addr, db_path);

    wait_for_health(port).await;

    let resp = Client::new()
        .get(format!("http://127.0.0.1:{port}/health"))
        .send()
        .await
        .expect("health request");
    assert_eq!(resp.status(), reqwest::StatusCode::OK);
}

#[tokio::test]
async fn search_endpoint_rewrites_upstream_credentials() {
    let (upstream_addr, rx) = spawn_mock_upstream("tvly-test-key".to_string()).await;

    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let port = listener.local_addr().unwrap().port();
    drop(listener);

    let db_path = temp_db_path("server-http-contract-search");
    let _backend = spawn_backend_process(port, upstream_addr, db_path);

    wait_for_health(port).await;

    let resp = Client::new()
        .post(format!("http://127.0.0.1:{port}/api/tavily/search"))
        .json(&serde_json::json!({
            "query": "modularization contract",
            "api_key": "th-demo-token",
            "max_results": 3
        }))
        .send()
        .await
        .expect("search request");
    assert_eq!(resp.status(), reqwest::StatusCode::OK);

    let (forwarded_auth, forwarded_api_key) = tokio::time::timeout(Duration::from_secs(2), rx)
        .await
        .expect("mock upstream timeout")
        .expect("receive forwarded credentials");
    assert_eq!(forwarded_auth, "Bearer tvly-test-key");
    assert_eq!(forwarded_api_key, "tvly-test-key");
}
