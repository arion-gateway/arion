// Copyright 2025 The kmesh Authors
//
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
//
//

macro_rules! init_observable_counter {
    ($counter: ident, $prefix: literal, $name: literal, $descr: literal) => {
        _ = $counter.set(Metric::new($prefix, $name, $descr, ShardedU64::new()));
        _ = global::meter(concat!("orion.", $prefix))
            .u64_observable_counter($name)
            .with_description($descr)
            .with_callback(move |observer| {
                let values = $counter.get().unwrap().value.load_all();
                values.iter().for_each(|(key, value)| {
                    observer.observe(*value, key);
                });
            })
            .build();
    };
}

macro_rules! init_observable_histogram {
    ($histogram: ident, $prefix: literal, $name: literal, $descr: literal, $buckets: expr) => {
        let otel_histogram =
            global::meter(concat!("orion.", $prefix)).u64_histogram($name).with_description($descr).build();

        _ = $histogram.set(Metric::new(
            $prefix,
            $name,
            $descr,
            crate::sharded::ShardedHistogram::new($buckets, Some(otel_histogram)),
        ));
    };
}

macro_rules! init_observable_gauge {
    ($counter: ident, $prefix: literal, $name: literal, $descr: literal) => {
        _ = $counter.set(Metric::new($prefix, $name, $descr, ShardedU64::new()));
        _ = global::meter(concat!("orion.", $prefix))
            .u64_observable_gauge($name)
            .with_description($descr)
            .with_callback(move |observer| {
                let values = $counter.get().unwrap().value.load_all();
                values.iter().for_each(|(key, value)| {
                    observer.observe(*value, key);
                });
            })
            .build();
    };
}
