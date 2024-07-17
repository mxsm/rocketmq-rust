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

use criterion::criterion_group;
use criterion::criterion_main;
use criterion::Criterion;
use lazy_static::lazy_static;
use rocketmq_common::TimeUtils::get_current_millis;

lazy_static! {
    pub static ref A: Vec<i32> = vec![1; 64];
}

pub fn delivery1() -> i32 {
    let a = A.get((get_current_millis() % 64) as usize);
    *a.unwrap()
}

pub fn delivery2() -> i32 {
    let a = A.get((get_current_millis() & 63) as usize);
    *a.unwrap()
}

fn criterion_benchmark(c: &mut Criterion) {
    c.bench_function("delivery1", |b| b.iter(|| delivery1()));
    c.bench_function("delivery2", |b| b.iter(|| delivery2()));
}

criterion_group!(benches, criterion_benchmark);
criterion_main!(benches);
