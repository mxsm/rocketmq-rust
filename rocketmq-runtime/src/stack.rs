// Copyright 2026 The RocketMQ Rust Authors
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

// Normalize before adding lifecycle/select/timeout wrappers. This bounds
// inline state, not arbitrary user poll frames or recursive user destructors.
//
// Unoptimized builds give every by-value move its own stack slot, so each
// wrapper and forwarding call below this boundary keeps another copy of an
// inline future and submission can take many times the future's size. Such
// builds use Tokio's debug boxing boundary instead.
pub(crate) const MAX_INLINE_SIZE: usize = if cfg!(debug_assertions) { 2 * 1024 } else { 16 * 1024 };

/// Returns how many bytes of the calling thread's stack are committed.
///
/// Windows commits a thread stack one page at a time as it grows and keeps
/// those pages until the thread exits, so the value is the deepest stack use
/// of the thread so far, rounded up to whole pages. A process entrypoint can
/// read it just before returning to measure the headroom left on the main
/// thread, whose size the runtime cannot choose.
///
/// Returns `None` on other platforms, or if the stack layout cannot be read.
pub fn current_thread_committed_stack_bytes() -> Option<usize> {
    #[cfg(windows)]
    {
        win::committed_stack_bytes()
    }
    #[cfg(not(windows))]
    {
        None
    }
}

#[cfg(windows)]
mod win {
    use std::ffi::c_void;
    use std::mem::size_of;

    use windows_sys::Win32::System::Memory::VirtualQuery;
    use windows_sys::Win32::System::Memory::MEMORY_BASIC_INFORMATION;
    use windows_sys::Win32::System::Memory::MEM_COMMIT;
    use windows_sys::Win32::System::Memory::PAGE_GUARD;
    use windows_sys::Win32::System::Threading::GetCurrentThreadStackLimits;

    pub(super) fn committed_stack_bytes() -> Option<usize> {
        let mut low = 0_usize;
        let mut high = 0_usize;
        // SAFETY: both arguments point to live, writable locals.
        unsafe { GetCurrentThreadStackLimits(&mut low, &mut high) };

        // The reservation runs from `low` (never committed) through the guard
        // pages to the committed pages that end at `high`. Walk it upward and
        // stop at the first committed page that is not a guard page.
        let mut address = low;
        while address < high {
            let mut region = MEMORY_BASIC_INFORMATION::default();
            // SAFETY: `region` is a live, writable buffer of the length passed.
            // VirtualQuery reads the page attributes only, never the page
            // contents, so it cannot trip a guard page.
            let written = unsafe {
                VirtualQuery(
                    address as *const c_void,
                    &mut region,
                    size_of::<MEMORY_BASIC_INFORMATION>(),
                )
            };
            if written == 0 || region.RegionSize == 0 {
                return None;
            }
            let base = region.BaseAddress as usize;
            if region.State == MEM_COMMIT && region.Protect & PAGE_GUARD == 0 {
                return high.checked_sub(base);
            }
            address = base.checked_add(region.RegionSize)?;
        }
        None
    }
}

#[cfg(test)]
mod tests {
    use super::current_thread_committed_stack_bytes;

    #[test]
    #[cfg(windows)]
    fn committed_stack_grows_with_stack_use() {
        let (before, after) = std::thread::Builder::new()
            .stack_size(1024 * 1024)
            .spawn(|| {
                let before = current_thread_committed_stack_bytes().expect("committed stack before use");
                (before, use_stack(256 * 1024))
            })
            .expect("spawn stack test thread")
            .join()
            .expect("stack test thread");
        assert!(before > 0);
        assert!(
            after >= before + 200 * 1024,
            "committed stack {after} did not grow past {before} after using 256 KiB"
        );
    }

    #[cfg(windows)]
    #[inline(never)]
    fn use_stack(bytes: usize) -> usize {
        const CHUNK: usize = 16 * 1024;
        let mut frame = [0_u8; CHUNK];
        std::hint::black_box(&mut frame);
        let committed = if bytes > CHUNK {
            use_stack(bytes - CHUNK)
        } else {
            current_thread_committed_stack_bytes().expect("committed stack after use")
        };
        // Keep this frame live across the call so it cannot become a tail call.
        std::hint::black_box(&frame);
        committed
    }

    #[test]
    #[cfg(not(windows))]
    fn other_platforms_do_not_report_a_committed_stack() {
        assert_eq!(current_thread_committed_stack_bytes(), None);
    }
}
