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

use std::str;

use bytes::{BufMut, BytesMut};

pub fn write_str(buf: &mut BytesMut, use_short_length: bool, s: &str) {
    if use_short_length {
        buf.put_u16(0);
    } else {
        buf.put_u32(0);
    }
    buf.put(s.as_bytes());

    let len = buf.len() - if use_short_length { 2 } else { 4 };
    if use_short_length {
        buf[0..2].copy_from_slice(&(len as u16).to_be_bytes());
    } else {
        buf[0..4].copy_from_slice(&(len as u32).to_be_bytes());
    }
}
