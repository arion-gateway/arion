// Copyright 2025 The kmesh Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use http::{HeaderName, StatusCode};
use std::net::SocketAddr;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;

use orion_configuration::config::metrics::{
    CustomMetric, CustomMetrics, MetricsConfig, PartitionKey, SourceHeaderName,
};
use orion_e2e_tests::config_builder::presets;
use orion_e2e_tests::{
    cleanup_config_file, OrionInstance, PortBlock, PreConfiguredResponse, SpawnOptions, TestBackend, TestClient,
};

fn parse_custom_metric_value(prometheus_output: &str, metric_name: &str, labels: &[(&str, &str)]) -> Option<f64> {
    for line in prometheus_output.lines() {
        if line.starts_with('#') {
            continue;
        }
        if line.starts_with(metric_name) {
            let mut matches = true;
            for (name, val) in labels {
                let pattern = format!("{name}=\"{val}\"");
                if !line.contains(&pattern) {
                    matches = false;
                    break;
                }
            }
            if matches {
                let parts: Vec<&str> = line.split_whitespace().collect();
                if let Some(val_str) = parts.last() {
                    if let Ok(val) = val_str.parse::<f64>() {
                        return Some(val);
                    }
                }
            }
        }
    }
    None
}

#[tokio::test]
#[ignore]
#[allow(clippy::indexing_slicing)]
#[allow(clippy::too_many_lines)]
async fn test_custom_metrics() {
    let port_block = PortBlock::reserve().expect("Failed to reserve port block");
    let admin_port = port_block.allocate().expect("Failed to allocate admin port");
    let admin_addr = SocketAddr::from(([127, 0, 0, 1], admin_port));

    let backend = TestBackend::start().await.expect("Failed to start test backend");
    let backend_addr = backend.addr();

    // Configure custom metrics for both request and response hooks
    let custom_metrics = CustomMetrics {
        incoming_request: vec![
            CustomMetric::Counter {
                name: "custom_req_counter".into(),
                description: "A custom request counter".into(),
                header_name: HeaderName::from_static("x-req-color"),
                attribute_name: Some("color".into()),
            },
            CustomMetric::Gauge {
                name: "custom_req_gauge".into(),
                description: "A custom request gauge".into(),
                header_name: HeaderName::from_static("x-req-gauge-val"),
                attribute_name: Some("gauge_attr".into()),
            },
            CustomMetric::Histogram {
                name: "custom_req_histogram".into(),
                description: "A custom request histogram".into(),
                header_name: HeaderName::from_static("x-req-hist-val"),
                attribute_name: Some("hist_attr".into()),
                buckets: vec![10, 50, 100],
            },
        ],
        upstream_request: vec![],
        incoming_response: vec![
            CustomMetric::Counter {
                name: "custom_resp_counter".into(),
                description: "A custom response counter".into(),
                header_name: HeaderName::from_static("x-resp-color"),
                attribute_name: Some("color".into()),
            },
            CustomMetric::Gauge {
                name: "custom_resp_gauge".into(),
                description: "A custom response gauge".into(),
                header_name: HeaderName::from_static("x-resp-gauge-val"),
                attribute_name: Some("gauge_attr".into()),
            },
            CustomMetric::Histogram {
                name: "custom_resp_histogram".into(),
                description: "A custom response histogram".into(),
                header_name: HeaderName::from_static("x-resp-hist-val"),
                attribute_name: Some("hist_attr".into()),
                buckets: vec![10, 50, 100],
            },
        ],
        ext_proc_request: vec![],
        ext_proc_response: vec![],
        downstream_response: vec![],
    };

    let metrics_config = MetricsConfig {
        user_key: None,
        custom_keys: smallvec::smallvec![PartitionKey {
            source: SourceHeaderName::HeaderName(HeaderName::from_static("x-tenant-id")),
            attribute_name: Some("tenant".into()),
        }],
        rename: std::collections::HashMap::new(),
        custom_metrics,
    };

    let bootstrap =
        presets::simple_proxy("backend", backend_addr).admin("127.0.0.1", admin_port).metrics(metrics_config);
    let config_path = bootstrap.build_to_temp().expect("Failed to build config");

    let orion = OrionInstance::spawn_auto_port(&config_path, "http", SpawnOptions::default())
        .await
        .expect("Failed to spawn Orion");

    let admin_client = TestClient::new(admin_addr);
    let listener_addr = orion.listener_addr().unwrap();

    // --- 1. Test Request-Hook Metrics ---
    backend.set_default_response(PreConfiguredResponse::with_body("Hello!")).await;

    // Send requests with different colors, gauges, and histogram values
    let req_colors = ["red", "red", "green", "blue"];
    let req_gauges = ["10", "42", "42", "42"];
    let req_hists = ["5", "25", "75", "150"];

    for i in 0..4 {
        let color = req_colors[i];
        let gauge = req_gauges[i];
        let hist = req_hists[i];

        let req = format!(
            "GET /ok HTTP/1.1\r\n\
             Host: localhost\r\n\
             x-tenant-id: tenant-req\r\n\
             x-req-color: {color}\r\n\
             x-req-gauge-val: {gauge}\r\n\
             x-req-hist-val: {hist}\r\n\
             Connection: close\r\n\r\n"
        );
        let mut stream = TcpStream::connect(listener_addr).await.expect("Failed to connect");
        stream.write_all(req.as_bytes()).await.expect("Failed to write");
        let mut resp = Vec::new();
        stream.read_to_end(&mut resp).await.expect("Failed to read");
        assert!(String::from_utf8_lossy(&resp).contains("200 OK"));
    }

    // --- 2. Test Response-Hook Metrics ---
    // For response hooks, the custom metrics and partition keys are extracted from response headers.
    let resp_colors = ["yellow", "yellow", "orange", "orange"];
    let resp_gauges = ["20", "84", "84", "84"];
    let resp_hists = ["8", "45", "90", "120"];

    for i in 0..4 {
        let color = resp_colors[i];
        let gauge = resp_gauges[i];
        let hist = resp_hists[i];

        backend
            .enqueue_response(
                PreConfiguredResponse::with_body("Hello with headers!")
                    .header("x-tenant-id", "tenant-resp")
                    .header("x-resp-color", color)
                    .header("x-resp-gauge-val", gauge)
                    .header("x-resp-hist-val", hist),
            )
            .await;

        let req = b"GET /ok HTTP/1.1\r\nHost: localhost\r\nConnection: close\r\n\r\n";
        let mut stream = TcpStream::connect(listener_addr).await.expect("Failed to connect");
        stream.write_all(req).await.expect("Failed to write");
        let mut resp = Vec::new();
        stream.read_to_end(&mut resp).await.expect("Failed to read");
        assert!(String::from_utf8_lossy(&resp).contains("200 OK"));
    }

    // --- 3. Test Edge Cases ---
    // Edge Case A: Missing custom headers (metrics should not be updated)
    {
        let req = b"GET /ok HTTP/1.1\r\nHost: localhost\r\nx-tenant-id: tenant-req\r\nConnection: close\r\n\r\n";
        let mut stream = TcpStream::connect(listener_addr).await.expect("Failed to connect");
        stream.write_all(req).await.expect("Failed to write");
        let mut resp = Vec::new();
        stream.read_to_end(&mut resp).await.expect("Failed to read");
        assert!(String::from_utf8_lossy(&resp).contains("200 OK"))
    }

    // Edge Case B: Missing partition key header (should record metric without tenant label)
    {
        let req = b"GET /ok HTTP/1.1\r\nHost: localhost\r\nx-req-color: purple\r\nConnection: close\r\n\r\n";
        let mut stream = TcpStream::connect(listener_addr).await.expect("Failed to connect");
        stream.write_all(req).await.expect("Failed to write");
        let mut resp = Vec::new();
        stream.read_to_end(&mut resp).await.expect("Failed to read");
        assert!(String::from_utf8_lossy(&resp).contains("200 OK"))
    }

    // Edge Case C: Invalid numeric values for Gauge and Histogram (should be ignored without panic)
    {
        let req = b"GET /ok HTTP/1.1\r\nHost: localhost\r\nx-tenant-id: tenant-req\r\nx-req-gauge-val: abc\r\nx-req-hist-val: xyz\r\nConnection: close\r\n\r\n";
        let mut stream = TcpStream::connect(listener_addr).await.expect("Failed to connect");
        stream.write_all(req).await.expect("Failed to write");
        let mut resp = Vec::new();
        stream.read_to_end(&mut resp).await.expect("Failed to read");
        assert!(String::from_utf8_lossy(&resp).contains("200 OK"))
    }

    // --- VERIFY METRICS ---
    let metrics_resp = admin_client.get("/stats/prometheus").await.expect("Failed to get metrics");
    metrics_resp.assert_status(StatusCode::OK);
    let metrics = metrics_resp.body_str().unwrap();

    // A. Verify Request-Hook Metrics (tenant="tenant-req")
    // Counter
    assert_eq!(
        parse_custom_metric_value(metrics, "custom_custom_req_counter", &[("color", "red"), ("tenant", "tenant-req")]),
        Some(2.0)
    );
    assert_eq!(
        parse_custom_metric_value(
            metrics,
            "custom_custom_req_counter",
            &[("color", "green"), ("tenant", "tenant-req")]
        ),
        Some(1.0)
    );
    assert_eq!(
        parse_custom_metric_value(metrics, "custom_custom_req_counter", &[("color", "blue"), ("tenant", "tenant-req")]),
        Some(1.0)
    );
    // Gauge (last valid value should be 42, invalid "abc" should be ignored)
    assert_eq!(parse_custom_metric_value(metrics, "custom_custom_req_gauge", &[("tenant", "tenant-req")]), Some(42.0));
    // Histogram (invalid "xyz" should be ignored, count remains 4 and sum remains 255)
    assert_eq!(
        parse_custom_metric_value(metrics, "custom_custom_req_histogram_count", &[("tenant", "tenant-req")]),
        Some(4.0)
    );
    assert_eq!(
        parse_custom_metric_value(metrics, "custom_custom_req_histogram_sum", &[("tenant", "tenant-req")]),
        Some(255.0)
    );

    // B. Verify Response-Hook Metrics (tenant="tenant-resp")
    // Counter
    assert_eq!(
        parse_custom_metric_value(
            metrics,
            "custom_custom_resp_counter",
            &[("color", "yellow"), ("tenant", "tenant-resp")]
        ),
        Some(2.0)
    );
    assert_eq!(
        parse_custom_metric_value(
            metrics,
            "custom_custom_resp_counter",
            &[("color", "orange"), ("tenant", "tenant-resp")]
        ),
        Some(2.0)
    );
    // Gauge
    assert_eq!(
        parse_custom_metric_value(metrics, "custom_custom_resp_gauge", &[("tenant", "tenant-resp")]),
        Some(84.0)
    );
    // Histogram
    assert_eq!(
        parse_custom_metric_value(metrics, "custom_custom_resp_histogram_count", &[("tenant", "tenant-resp")]),
        Some(4.0)
    );
    assert_eq!(
        parse_custom_metric_value(metrics, "custom_custom_resp_histogram_sum", &[("tenant", "tenant-resp")]),
        Some(263.0)
    );

    // C. Verify Edge Case B (Missing partition key header: recorded with color="purple" but without tenant label)
    assert_eq!(parse_custom_metric_value(metrics, "custom_custom_req_counter", &[("color", "purple")]), Some(1.0));
    // Ensure it doesn't have the tenant label
    assert!(parse_custom_metric_value(
        metrics,
        "custom_custom_req_counter",
        &[("color", "purple"), ("tenant", "tenant-req")]
    )
    .is_none());

    orion.shutdown();
    cleanup_config_file(&config_path);
}

#[tokio::test]
#[ignore]
async fn test_custom_metrics_multiple_keys() {
    let port_block = PortBlock::reserve().expect("Failed to reserve port block");
    let admin_port = port_block.allocate().expect("Failed to allocate admin port");
    let admin_addr = SocketAddr::from(([127, 0, 0, 1], admin_port));

    let backend = TestBackend::start().await.expect("Failed to start test backend");
    let backend_addr = backend.addr();
    backend.set_default_response(PreConfiguredResponse::with_body("OK")).await;

    // Configure custom metrics with a counter
    let custom_metrics = CustomMetrics {
        incoming_request: vec![CustomMetric::Counter {
            name: "custom_multi_key_counter".into(),
            description: "A custom counter with multiple keys".into(),
            header_name: HeaderName::from_static("x-req-color"),
            attribute_name: Some("color".into()),
        }],
        upstream_request: vec![],
        incoming_response: vec![],
        downstream_response: vec![],
        ext_proc_request: vec![],
        ext_proc_response: vec![],
    };

    // Configure TWO custom partition keys
    let metrics_config = MetricsConfig {
        user_key: None,
        custom_keys: smallvec::smallvec![
            PartitionKey {
                source: SourceHeaderName::HeaderName(HeaderName::from_static("x-tenant-id")),
                attribute_name: Some("tenant".into()),
            },
            PartitionKey {
                source: SourceHeaderName::HeaderName(HeaderName::from_static("x-environment")),
                attribute_name: Some("env".into()),
            },
        ],
        rename: std::collections::HashMap::new(),
        custom_metrics,
    };

    let bootstrap =
        presets::simple_proxy("backend", backend_addr).admin("127.0.0.1", admin_port).metrics(metrics_config);
    let config_path = bootstrap.build_to_temp().expect("Failed to build config");

    let orion = OrionInstance::spawn_auto_port(&config_path, "http", SpawnOptions::default())
        .await
        .expect("Failed to spawn Orion");

    let admin_client = TestClient::new(admin_addr);
    let listener_addr = orion.listener_addr().unwrap();

    // Send requests with both tenant and environment headers
    let req1 = b"GET /ok HTTP/1.1\r\n\
Host: localhost\r\n\
x-tenant-id: tenant-1\r\n\
x-environment: production\r\n\
x-req-color: red\r\n\
Connection: close\r\n\r\n";
    {
        let mut stream = TcpStream::connect(listener_addr).await.expect("Failed to connect");
        stream.write_all(req1).await.expect("Failed to write");
        let mut resp = Vec::new();
        stream.read_to_end(&mut resp).await.expect("Failed to read");
        let resp_str = String::from_utf8_lossy(&resp);
        println!("REQ1 RESPONSE: {resp_str}");
        assert!(resp_str.contains("200 OK"))
    }

    let req2 = b"GET /ok HTTP/1.1\r\n\
Host: localhost\r\n\
x-tenant-id: tenant-2\r\n\
x-environment: staging\r\n\
x-req-color: blue\r\n\
Connection: close\r\n\r\n";
    {
        let mut stream = TcpStream::connect(listener_addr).await.expect("Failed to connect");
        stream.write_all(req2).await.expect("Failed to write");
        let mut resp = Vec::new();
        stream.read_to_end(&mut resp).await.expect("Failed to read");
        assert!(String::from_utf8_lossy(&resp).contains("200 OK"))
    }

    // Verify metrics
    let metrics_resp = admin_client.get("/stats/prometheus").await.expect("Failed to get metrics");
    metrics_resp.assert_status(StatusCode::OK);
    let metrics = metrics_resp.body_str().unwrap();

    // Verify that the counter has both tenant and env labels
    assert_eq!(
        parse_custom_metric_value(
            metrics,
            "custom_custom_multi_key_counter",
            &[("color", "red"), ("tenant", "tenant-1"), ("env", "production")]
        ),
        Some(1.0)
    );

    assert_eq!(
        parse_custom_metric_value(
            metrics,
            "custom_custom_multi_key_counter",
            &[("color", "blue"), ("tenant", "tenant-2"), ("env", "staging")]
        ),
        Some(1.0)
    );

    orion.shutdown();
    cleanup_config_file(&config_path);
}
