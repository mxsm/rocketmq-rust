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
use std::collections::HashSet;
use std::string::ToString;
use std::sync::Arc;

use tracing::info;

use crate::common::attribute::AttributeTrait;

pub fn alter_current_attributes<A: AttributeTrait>(
    create: bool,
    all: HashMap<String, A>,
    current_attributes: HashMap<String, String>,
    new_attributes: HashMap<String, String>,
) -> HashMap<String, String> {
    let mut init: HashMap<String, String> = HashMap::new();
    let mut add: HashMap<String, String> = HashMap::new();
    let mut update: HashMap<String, String> = HashMap::new();
    let mut delete: HashMap<String, String> = HashMap::new();
    let mut keys: HashSet<String> = HashSet::new();

    for (key, value) in new_attributes {
        let real_key = real_key(key.as_str());
        validate(&real_key);
        duplication_check(&mut keys, &real_key);

        if create {
            if key.starts_with('+') {
                init.insert(real_key.clone(), value);
            } else {
                panic!(
                    "only add attribute is supported while creating topic. key: {}",
                    real_key
                );
            }
        } else if key.starts_with('+') {
            if !current_attributes.contains_key(&real_key) {
                add.insert(real_key.clone(), value);
            } else {
                update.insert(real_key.clone(), value);
            }
        } else if key.starts_with('-') {
            if !current_attributes.contains_key(&real_key) {
                panic!("attempt to delete a nonexistent key: {}", real_key);
            }
            delete.insert(real_key.clone(), value);
        } else {
            panic!("wrong format key: {}", real_key);
        }
    }

    validate_alter(&all, &init, true, false);
    validate_alter(&all, &add, false, false);
    validate_alter(&all, &update, false, false);
    validate_alter(&all, &delete, false, true);

    info!("add: {:?}, update: {:?}, delete: {:?}", add, update, delete);

    let mut final_attributes = current_attributes.clone();
    final_attributes.extend(init);
    final_attributes.extend(add);
    for key in delete.keys() {
        final_attributes.remove(key);
    }

    final_attributes
}

fn duplication_check(keys: &mut HashSet<String>, key: &String) {
    if !keys.insert(key.clone()) {
        panic!("alter duplication key. key: {}", key);
    }
}

fn validate(kv_attribute: &str) {
    if kv_attribute.is_empty() || kv_attribute.contains('+') || kv_attribute.contains('-') {
        panic!("kv string format wrong.");
    }
}

fn validate_alter<A: AttributeTrait>(
    all: &HashMap<String, A>,
    alter: &HashMap<String, String>,
    init: bool,
    delete: bool,
) {
    for (key, value) in alter {
        let attribute = all.get(key);
        if attribute.is_none() {
            panic!("unsupported key: {}", key);
        }
        let attribute = attribute.unwrap();
        if !init && !attribute.changeable() {
            panic!("attempt to update an unchangeable attribute. key: {}", key);
        }

        if !delete {
            attribute.verify(value);
        }
    }
}

fn real_key(key: &str) -> String {
    key.chars().skip(1).collect()
}
