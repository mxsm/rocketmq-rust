// Copyright 2023 The RocketMQ Rust Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#![feature(sync_unsafe_cell)]
use std::cell::SyncUnsafeCell;
use std::collections::HashSet;

use criterion::criterion_group;
use criterion::criterion_main;
use criterion::Criterion;

pub struct Test {
    pub a: SyncUnsafeCell<HashSet<String>>,
    pub b: parking_lot::Mutex<HashSet<String>>,
}

impl Default for Test {
    fn default() -> Self {
        Self::new()
    }
}

impl Test {
    pub fn new() -> Self {
        Test {
            a: SyncUnsafeCell::new(HashSet::new()),
            b: parking_lot::Mutex::new(HashSet::new()),
        }
    }

    pub fn insert_1(&self, key: String) {
        unsafe {
            let a = &mut *self.a.get();
            a.insert(key);
        }
    }

    pub fn insert_2(&self, key: String) {
        let mut b = self.b.lock();
        b.insert(key);
    }

    pub fn get_1(&self, key: &str) -> String {
        unsafe {
            let a = &*self.a.get();
            a.get(key).unwrap().to_string()
        }
    }

    pub fn get_2(&self, key: &str) -> String {
        let b = self.b.lock();
        b.get(key).unwrap().as_str().to_string()
    }
}

fn benchmark_insert_1(c: &mut Criterion) {
    let test = Test::new();
    c.bench_function("insert_1", |b| {
        b.iter(|| {
            test.insert_1("key".to_string());
        })
    });
}

fn benchmark_insert_2(c: &mut Criterion) {
    let test = Test::new();
    c.bench_function("insert_2", |b| {
        b.iter(|| {
            test.insert_2("key".to_string());
        })
    });
}

fn benchmark_get_1(c: &mut Criterion) {
    let test = Test::new();
    let key = String::from("test_key");

    // Insert key for the get benchmarks
    test.insert_1(key.clone());

    c.bench_function("get_1", |b| {
        b.iter(|| {
            test.get_1("test_key");
        })
    });
}

fn benchmark_get_2(c: &mut Criterion) {
    let test = Test::new();
    let key = String::from("test_key");

    // Insert key for the get benchmarks
    test.insert_2(key.clone());

    c.bench_function("get_2", |b| {
        b.iter(|| {
            test.get_2("test_key");
        })
    });
}

criterion_group!(
    benches,
    benchmark_insert_1,
    benchmark_insert_2,
    benchmark_get_1,
    benchmark_get_2
);
criterion_main!(benches);
