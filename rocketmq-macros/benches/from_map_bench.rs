/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

use std::collections::HashMap;

use cheetah_string::CheetahString;
use criterion::criterion_group;
use criterion::criterion_main;
use criterion::Criterion;
use rocketmq_error::RocketmqError;
use rocketmq_macros::*;
use rocketmq_remoting::protocol::command_custom_header::FromMap;

#[derive(Default, Debug, Clone)]
pub struct ExampleHeaderOld {
    pub a: CheetahString,
    pub b: CheetahString,
    pub c: i32,
    pub d: Option<i64>,
}

impl ExampleHeaderOld {
    pub const FIELD_A: &'static str = "a";
    pub const FIELD_B: &'static str = "b";
    pub const FIELD_C: &'static str = "c";
    pub const FIELD_D: &'static str = "d";
}

impl ExampleHeaderOld {
    pub fn from_old(map: &HashMap<CheetahString, CheetahString>) -> Result<Self, RocketmqError> {
        Ok(Self {
            a: map
                .get(&CheetahString::from_static_str(Self::FIELD_A))
                .cloned()
                .ok_or_else(|| RocketmqError::DeserializeHeaderError("miss a".into()))?,
            b: map
                .get(&CheetahString::from_static_str(Self::FIELD_B))
                .cloned()
                .ok_or_else(|| RocketmqError::DeserializeHeaderError("miss b".into()))?,
            c: map
                .get(&CheetahString::from_static_str(Self::FIELD_C))
                .ok_or_else(|| RocketmqError::DeserializeHeaderError("miss c".into()))?
                .as_str()
                .parse()
                .map_err(|_| RocketmqError::DeserializeHeaderError("parse c".into()))?,
            d: map
                .get(&CheetahString::from_static_str(Self::FIELD_D))
                .and_then(|s| s.as_str().parse::<i64>().ok()),
        })
    }
}

// ======================= new FromMap =========================
#[derive(Debug, Default, RequestHeaderCodecV2)]
pub struct ExampleHeader {
    #[required]
    pub a: CheetahString,
    #[required]
    pub b: CheetahString,
    #[required]
    pub c: i32,
    pub d: Option<i64>,
}

// ======================= Benchmark =========================

fn bench_from_map(c: &mut Criterion) {
    let mut map = HashMap::new();
    map.insert(
        CheetahString::from_static_str("a"),
        CheetahString::from_static_str("hello"),
    );
    map.insert(
        CheetahString::from_static_str("b"),
        CheetahString::from_static_str("world"),
    );
    map.insert(
        CheetahString::from_static_str("c"),
        CheetahString::from_static_str("42"),
    );
    map.insert(
        CheetahString::from_static_str("d"),
        CheetahString::from_static_str("12345"),
    );

    c.bench_function("old_from_map", |b| {
        b.iter(|| ExampleHeaderOld::from_old(&map).unwrap())
    });

    c.bench_function("new_from_map", |b| {
        b.iter(|| <ExampleHeader as FromMap>::from(&map).unwrap())
    });
}

criterion_group!(benches, bench_from_map);
criterion_main!(benches);
