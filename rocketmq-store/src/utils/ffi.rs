/*
 *   Licensed to the Apache Software Foundation (ASF) under one or more
 *   contributor license agreements.  See the NOTICE file distributed with
 *   this work for additional information regarding copyright ownership.
 *   The ASF licenses this file to You under the Apache License, Version 2.0
 *   (the "License"); you may not use this file except in compliance with
 *   the License.  You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 *   Unless required by applicable law or agreed to in writing, software
 *   distributed under the License is distributed on an "AS IS" BASIS,
 *   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *   See the License for the specific language governing permissions and
 *   limitations under the License.
 */
use std::ffi::c_void;

use libc::c_uchar;
use rocketmq_error::RocketMQResult;
use rocketmq_error::RocketmqError;

pub(crate) const MADV_NORMAL: i32 = 0;
pub(crate) const MADV_RANDOM: i32 = 1;
pub(crate) const MADV_WILLNEED: i32 = 3;
pub(crate) const MADV_DONTNEED: i32 = 4;

#[inline]
pub fn get_page_size() -> usize {
    page_size::get()
}

#[inline]
pub fn mlock(addr: *const u8, len: usize) -> RocketMQResult<()> {
    #[cfg(unix)]
    {
        let result = unsafe { libc::mlock(addr as *const c_void, len) };
        if result != 0 {
            return Err(RocketmqError::StoreCustomError("mlock failed".to_string()));
        }
        Ok(())
    }
}

#[inline]
pub fn munlock(addr: *const u8, len: usize) -> RocketMQResult<()> {
    #[cfg(unix)]
    {
        let result = unsafe { libc::munlock(addr as *const c_void, len) };
        if result != 0 {
            return Err(RocketmqError::StoreCustomError(
                "munlock failed".to_string(),
            ));
        }
        Ok(())
    }
}

pub fn madvise(addr: *const u8, len: usize, advice: i32) -> i32 {
    #[cfg(unix)]
    {
        unsafe { libc::madvise(addr as *mut c_void, len, advice) }
    }
}

pub fn mincore(addr: *const u8, len: usize, vec: *const u8) -> i32 {
    #[cfg(unix)]
    {
        unsafe { libc::mincore(addr as *mut c_void, len, vec as *mut c_uchar) }
    }
}
