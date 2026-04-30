use std::borrow::Cow;
use std::sync::{Arc, OnceLock};
use std::thread::ThreadId;

use http::{HeaderMap, HeaderName};
use opentelemetry::{global, KeyValue};
use orion_configuration::config::metrics::CustomMetric;
use orion_interner::StringInterner;
use smallvec::SmallVec;

use crate::{
    metrics::Metric,
    sharded::{Gauge, ShardedHistogram, ShardedU64},
};

pub static CUSTOM_METRICS: OnceLock<CustomMetrics> = OnceLock::new();

pub fn init_metrics(config: &orion_configuration::config::metrics::CustomMetrics) {
    if !config.incoming_request.is_empty()
        || !config.upstream_request.is_empty()
        || !config.incoming_response.is_empty()
        || !config.downstream_response.is_empty()
    {
        _ = CUSTOM_METRICS.set(CustomMetrics::new(config));
    }
}

pub struct HeaderMetric<T> {
    pub header_name: HeaderName,
    pub attr_name: &'static str,
    pub metric: Arc<Metric<T>>,
}

pub struct CustomMetricCounters {
    counters: Vec<HeaderMetric<ShardedU64<ThreadId>>>,
    histograms: Vec<HeaderMetric<ShardedHistogram<ThreadId>>>,
    gauges: Vec<HeaderMetric<Gauge>>,
}

impl CustomMetricCounters {
    pub fn empty(&self) -> bool {
        self.counters.is_empty() && self.histograms.is_empty() && self.gauges.is_empty()
    }

    pub fn counters(&self) -> &[HeaderMetric<ShardedU64<ThreadId>>] {
        &self.counters
    }

    pub fn histograms(&self) -> &[HeaderMetric<ShardedHistogram<ThreadId>>] {
        &self.histograms
    }

    pub fn gauges(&self) -> &[HeaderMetric<Gauge>] {
        &self.gauges
    }
}

pub struct CustomMetrics {
    incoming_request: CustomMetricCounters,
    upstream_request: CustomMetricCounters,
    incoming_response: CustomMetricCounters,
    downstream_response: CustomMetricCounters,
}

#[derive(Debug, Copy, Clone, PartialEq, Eq)]
pub enum MetricsHook {
    IncomingRequest,
    UpstreamRequest,
    IncomingResponse,
    DownstreamResponse,
}

impl CustomMetricCounters {
    pub fn new(metrics: &[CustomMetric]) -> Self {
        let mut counters = Vec::new();
        let mut histograms = Vec::new();
        let mut gauges = Vec::new();

        for metric in metrics {
            match metric {
                CustomMetric::Counter { name, description, header_name: http_header_name, attribute_name } => {
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
                CustomMetric::Histogram {
                    name,
                    description,
                    header_name: http_header_name,
                    attribute_name,
                    buckets,
                } => {
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
                CustomMetric::Gauge { name, description, header_name: http_header_name, attribute_name } => {
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
}

impl CustomMetrics {
    pub fn new(config: &orion_configuration::config::metrics::CustomMetrics) -> Self {
        Self {
            incoming_request: CustomMetricCounters::new(&config.incoming_request),
            upstream_request: CustomMetricCounters::new(&config.upstream_request),
            incoming_response: CustomMetricCounters::new(&config.incoming_response),
            downstream_response: CustomMetricCounters::new(&config.downstream_response),
        }
    }

    pub fn with_headers(&self, hook: MetricsHook, headers: &HeaderMap, extra_attributes: &[KeyValue]) {
        let counters = match hook {
            MetricsHook::IncomingRequest => &self.incoming_request,
            MetricsHook::UpstreamRequest => &self.upstream_request,
            MetricsHook::IncomingResponse => &self.incoming_response,
            MetricsHook::DownstreamResponse => &self.downstream_response,
        };

        if counters.empty() {
            return;
        }

        let shard_id = std::thread::current().id();

        let mut base_attributes: SmallVec<[KeyValue; 4]> = SmallVec::with_capacity(extra_attributes.len() + 1);
        base_attributes.extend(extra_attributes.iter().cloned());

        for counter in &counters.counters {
            if let Some(header_value) = headers.get(&counter.header_name) {
                if let Ok(val_str) = header_value.to_str() {
                    let val_static = val_str.to_static_str();
                    let kv = KeyValue::new(counter.attr_name, val_static);
                    base_attributes.push(kv);
                    counter.metric.value.add(1, shard_id, &base_attributes);
                    base_attributes.pop();
                }
            }
        }

        for histogram in &counters.histograms {
            if let Some(header_value) = headers.get(&histogram.header_name) {
                if let Ok(val_str) = header_value.to_str() {
                    if let Ok(num_val) = val_str.parse::<u64>() {
                        histogram.metric.value.record(num_val, shard_id, extra_attributes);
                    }
                }
            }
        }

        for gauge in &counters.gauges {
            if let Some(header_value) = headers.get(&gauge.header_name) {
                if let Ok(val_str) = header_value.to_str() {
                    if let Ok(num_val) = val_str.parse::<u64>() {
                        gauge.metric.value.record(num_val, extra_attributes);
                    }
                }
            }
        }
    }

    pub fn counters(&self) -> impl Iterator<Item = &HeaderMetric<ShardedU64<ThreadId>>> {
        self.incoming_request
            .counters()
            .iter()
            .chain(self.upstream_request.counters().iter())
            .chain(self.incoming_response.counters().iter())
            .chain(self.downstream_response.counters().iter())
    }

    pub fn histograms(&self) -> impl Iterator<Item = &HeaderMetric<ShardedHistogram<ThreadId>>> {
        self.incoming_request
            .histograms()
            .iter()
            .chain(self.upstream_request.histograms().iter())
            .chain(self.incoming_response.histograms().iter())
            .chain(self.downstream_response.histograms().iter())
    }

    pub fn gauges(&self) -> impl Iterator<Item = &HeaderMetric<Gauge>> {
        self.incoming_request
            .gauges()
            .iter()
            .chain(self.upstream_request.gauges().iter())
            .chain(self.incoming_response.gauges().iter())
            .chain(self.downstream_response.gauges().iter())
    }
}
