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

use crc32fast::Hasher;

// Calculate the CRC32 checksum for the given byte array
pub fn crc32(buf: &[u8]) -> u32 {
    //crc32fast::hash(buf)
    let mut hasher = Hasher::new();
    hasher.update(buf);
    let checksum = hasher.finalize();
    checksum & 0x7FFFFFFF
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_crc32_negative() {
        let buf = [1, 2, 3, 4, 5];
        assert_ne!(crc32(&buf), 0xFFFFFFFF);
    }

    #[test]
    fn test_crc32_empty_buffer() {
        let buf: [u8; 0] = [];
        assert_eq!(crc32(&buf), 0);
    }
}
