use std::borrow::Cow;
use std::sync::{Arc, OnceLock};
use std::thread::ThreadId;

use http::{HeaderMap, HeaderName};
use opentelemetry::{global, KeyValue};
use orion_configuration::config::metrics::CustomMetric;
use orion_interner::StringInterner;
use smallvec::SmallVec;
use tracing::info;

use crate::{
    metrics::Metric,
    sharded::{Gauge, ShardedHistogram, ShardedU64},
};

pub static CUSTOM_METRICS: OnceLock<CustomMetrics> = OnceLock::new();

pub fn init_metrics(config: &[CustomMetric]) {
    if !config.is_empty() {
        _ = CUSTOM_METRICS.set(CustomMetrics::new(config));
    }
}

pub struct HeaderMetric<T> {
    pub header_name: HeaderName,
    pub attr_name: &'static str,
    pub metric: Arc<Metric<T>>,
}

pub struct CustomMetrics {
    counters: Vec<HeaderMetric<ShardedU64<ThreadId>>>,
    histograms: Vec<HeaderMetric<ShardedHistogram<ThreadId>>>,
    gauges: Vec<HeaderMetric<Gauge>>,
}

impl CustomMetrics {
    pub fn counters(&self) -> &[HeaderMetric<ShardedU64<ThreadId>>] {
        &self.counters
    }

    pub fn histograms(&self) -> &[HeaderMetric<ShardedHistogram<ThreadId>>] {
        &self.histograms
    }

    pub fn gauges(&self) -> &[HeaderMetric<Gauge>] {
        &self.gauges
    }

    pub fn new(metrics: &[CustomMetric]) -> Self {
        let mut counters = Vec::new();
        let mut histograms = Vec::new();
        let mut gauges = Vec::new();

        info!("{:#?}", metrics);
        for metric in metrics {
            match metric {
                CustomMetric::Counter { name, description, http_header_name, attribute_name } => {
                    let name = name.to_static_str();
                    let description = description.to_static_str();

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

                    let attr_name = attribute_name
                        .as_ref()
                        .map(|s| Cow::Borrowed(s.as_str()))
                        .unwrap_or_else(|| {
                            let header_str = http_header_name.as_str();
                            if header_str.contains('-') {
                                Cow::Owned(header_str.replace('-', "_"))
                            } else {
                                Cow::Borrowed(header_str)
                            }
                        })
                        .as_ref()
                        .to_static_str();

                    counters.push(HeaderMetric {
                        header_name: http_header_name.clone(),
                        attr_name,
                        metric: metric_obj,
                    });
                },
                CustomMetric::Histogram { name, description, http_header_name, attribute_name, buckets } => {
                    let name = name.to_static_str();
                    let description = description.to_static_str();

                    let otel_histogram =
                        global::meter("orion.http").u64_histogram(name).with_description(description).build();

                    let sharded = ShardedHistogram::new(buckets.clone(), Some(otel_histogram));
                    let metric_obj = Arc::new(Metric::new("http", name, description, sharded));

                    let attr_name = attribute_name
                        .as_ref()
                        .map(|s| Cow::Borrowed(s.as_str()))
                        .unwrap_or_else(|| {
                            let header_str = http_header_name.as_str();
                            if header_str.contains('-') {
                                Cow::Owned(header_str.replace('-', "_"))
                            } else {
                                Cow::Borrowed(header_str)
                            }
                        })
                        .as_ref()
                        .to_static_str();

                    histograms.push(HeaderMetric {
                        header_name: http_header_name.clone(),
                        attr_name,
                        metric: metric_obj,
                    });
                },
                CustomMetric::Gauge { name, description, http_header_name, attribute_name } => {
                    let name = name.to_static_str();
                    let description = description.to_static_str();

                    let metric_obj = Arc::new(Metric::new("http", name, description, Gauge::new()));
                    let metric_clone = metric_obj.clone();

                    let _ = global::meter("orion.http")
                        .u64_observable_gauge(name)
                        .with_description(description)
                        .with_callback(move |observer| {
                            let values = metric_clone.value.load_all();
                            values.iter().for_each(|(key, value)| {
                                observer.observe(*value, key);
                            });
                        })
                        .build();

                    let attr_name = attribute_name
                        .as_ref()
                        .map(|s| Cow::Borrowed(s.as_str()))
                        .unwrap_or_else(|| {
                            let header_str = http_header_name.as_str();
                            if header_str.contains('-') {
                                Cow::Owned(header_str.replace('-', "_"))
                            } else {
                                Cow::Borrowed(header_str)
                            }
                        })
                        .as_ref()
                        .to_static_str();

                    gauges.push(HeaderMetric { header_name: http_header_name.clone(), attr_name, metric: metric_obj });
                },
            }
        }

        Self { counters, histograms, gauges }
    }

    pub fn with_request_headers(&self, headers: &HeaderMap, extra_attributes: &[KeyValue]) {
        let shard_id = std::thread::current().id();

        for counter in &self.counters {
            if let Some(header_value) = headers.get(&counter.header_name) {
                if let Ok(val_str) = header_value.to_str() {
                    info!("with_request_headers: -> val_str: {}", val_str);
                    let val_static = val_str.to_static_str();
                    let kv = KeyValue::new(counter.attr_name, val_static);

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

        for gauge in &self.gauges {
            if let Some(header_value) = headers.get(&gauge.header_name) {
                if let Ok(val_str) = header_value.to_str() {
                    if let Ok(num_val) = val_str.parse::<u64>() {
                        gauge.metric.value.record(num_val, extra_attributes);
                    }
                }
            }
        }
    }
}
