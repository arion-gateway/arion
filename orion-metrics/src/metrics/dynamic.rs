use std::sync::{Arc, OnceLock};
use std::thread::ThreadId;

use http::{HeaderMap, HeaderName};
use opentelemetry::{global, KeyValue};
use orion_configuration::config::metrics::DynamicMetric;
use smallvec::SmallVec;
use orion_interner::StringInterner;
use tracing::info;

use crate::{
    metrics::Metric,
    sharded::{ShardedHistogram, ShardedU64},
};

pub static DYNAMIC_METRICS: OnceLock<DynamicMetrics> = OnceLock::new();

pub fn init_metrics(config: &[DynamicMetric]) {
    if !config.is_empty() {
        _ = DYNAMIC_METRICS.set(DynamicMetrics::new(config));
    }
}

pub struct HeaderMetric<T> {
    pub header_name: HeaderName,
    pub label_name: &'static str,
    pub metric: Arc<Metric<T>>,
}

pub struct DynamicMetrics {
    counters: Vec<HeaderMetric<ShardedU64<ThreadId>>>,
    histograms: Vec<HeaderMetric<ShardedHistogram<ThreadId>>>,
}

impl DynamicMetrics {
    pub fn counters(&self) -> &[HeaderMetric<ShardedU64<ThreadId>>] {
        &self.counters
    }

    pub fn histograms(&self) -> &[HeaderMetric<ShardedHistogram<ThreadId>>] {
        &self.histograms
    }

    pub fn new(metrics: &[DynamicMetric]) -> Self {
        let mut counters = Vec::new();
        let mut histograms = Vec::new();

        info!("{:#?}", metrics);
        for metric in metrics {
            match metric {
                DynamicMetric::Counter(info) => {
                    let name = info.name.to_static_str();
                    let description = info.description.to_static_str();

                    let metric_obj = Arc::new(Metric::new("http", name, description, ShardedU64::new()));
                    let metric_clone = metric_obj.clone();

                    let _ = global::meter("orion.http")
                        .u64_observable_counter(name)
                        .with_description(description)
                        .with_callback(move |observer| {
                            let values = metric_clone.value.load_all();
                            values.iter().for_each(|(key, value)| {
                                observer.observe(*value, key);
                            });
                        })
                        .build();

                    counters.push(HeaderMetric {
                        header_name: info.http_header_name.clone(),
                        label_name: info.http_header_name.as_str().replace('-', "_").to_static_str(),
                        metric: metric_obj,
                    });
                },
                DynamicMetric::Histogram(info) => {
                    let name = info.name.to_static_str();
                    let description = info.description.to_static_str();

                    let otel_histogram =
                        global::meter("orion.http").u64_histogram(name).with_description(description).build();

                    let sharded = ShardedHistogram::new(info.buckets.clone(), Some(otel_histogram));
                    let metric_obj = Arc::new(Metric::new("http", name, description, sharded));

                    histograms.push(HeaderMetric {
                        header_name: info.http_header_name.clone(),
                        label_name: info.http_header_name.as_str().replace('-', "_").to_static_str(),
                        metric: metric_obj,
                    });
                },
            }
        }

        Self { counters, histograms }
    }

    pub fn with_request_headers(&self, headers: &HeaderMap, extra_attributes: &[KeyValue]) {
        let shard_id = std::thread::current().id();

        for counter in &self.counters {
            if let Some(header_value) = headers.get(&counter.header_name) {
                if let Ok(val_str) = header_value.to_str() {
                    info!("with_request_headers: -> val_str: {}", val_str);
                    let val_static = val_str.to_static_str();
                    let kv = KeyValue::new(counter.label_name, val_static);

                    // Allocate on the stack up to 8 elements, fallback to heap if exceeded
                    let mut attributes: SmallVec<[KeyValue; 4]> = SmallVec::with_capacity(extra_attributes.len() + 1);
                    attributes.extend(extra_attributes.iter().cloned());
                    attributes.push(kv);

                    counter.metric.value.add(1, shard_id, &attributes);
                }
            }
        }

        for histogram in &self.histograms {
            if let Some(header_value) = headers.get(&histogram.header_name) {
                if let Ok(val_str) = header_value.to_str() {
                    if let Ok(num_val) = val_str.parse::<u64>() {
                        histogram.metric.value.record(num_val, shard_id, extra_attributes);
                    }
                }
            }
        }
    }
}
