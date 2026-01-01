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

pub struct PullSysFlag;

impl PullSysFlag {
    const FLAG_COMMIT_OFFSET: u32 = 0x1;
    const FLAG_SUSPEND: u32 = 0x1 << 1;
    const FLAG_SUBSCRIPTION: u32 = 0x1 << 2;
    const FLAG_CLASS_FILTER: u32 = 0x1 << 3;
    const FLAG_LITE_PULL_MESSAGE: u32 = 0x1 << 4;

    pub fn build_sys_flag(commit_offset: bool, suspend: bool, subscription: bool, class_filter: bool) -> u32 {
        let mut flag = 0;

        if commit_offset {
            flag |= Self::FLAG_COMMIT_OFFSET;
        }

        if suspend {
            flag |= Self::FLAG_SUSPEND;
        }

        if subscription {
            flag |= Self::FLAG_SUBSCRIPTION;
        }

        if class_filter {
            flag |= Self::FLAG_CLASS_FILTER;
        }

        flag
    }

    pub fn build_sys_flag_with_lite_pull(
        commit_offset: bool,
        suspend: bool,
        subscription: bool,
        class_filter: bool,
        lite_pull: bool,
    ) -> u32 {
        let mut flag = Self::build_sys_flag(commit_offset, suspend, subscription, class_filter);

        if lite_pull {
            flag |= Self::FLAG_LITE_PULL_MESSAGE;
        }

        flag
    }

    pub fn clear_commit_offset_flag(sys_flag: u32) -> u32 {
        sys_flag & (!Self::FLAG_COMMIT_OFFSET)
    }

    pub fn has_commit_offset_flag(sys_flag: u32) -> bool {
        (sys_flag & Self::FLAG_COMMIT_OFFSET) == Self::FLAG_COMMIT_OFFSET
    }

    pub fn has_suspend_flag(sys_flag: u32) -> bool {
        (sys_flag & Self::FLAG_SUSPEND) == Self::FLAG_SUSPEND
    }

    pub fn clear_suspend_flag(sys_flag: u32) -> u32 {
        sys_flag & (!Self::FLAG_SUSPEND)
    }

    pub fn has_subscription_flag(sys_flag: u32) -> bool {
        (sys_flag & Self::FLAG_SUBSCRIPTION) == Self::FLAG_SUBSCRIPTION
    }

    pub fn build_sys_flag_with_subscription(sys_flag: u32) -> u32 {
        sys_flag | Self::FLAG_SUBSCRIPTION
    }

    pub fn has_class_filter_flag(sys_flag: u32) -> bool {
        (sys_flag & Self::FLAG_CLASS_FILTER) == Self::FLAG_CLASS_FILTER
    }

    pub fn has_lite_pull_flag(sys_flag: u32) -> bool {
        (sys_flag & Self::FLAG_LITE_PULL_MESSAGE) == Self::FLAG_LITE_PULL_MESSAGE
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn build_sys_flag_sets_correct_flags() {
        let flag = PullSysFlag::build_sys_flag(true, true, true, true);
        assert_eq!(flag, 0b1111);
    }

    #[test]
    fn build_sys_flag_with_lite_pull_sets_correct_flags() {
        let flag = PullSysFlag::build_sys_flag_with_lite_pull(true, true, true, true, true);
        assert_eq!(flag, 0b11111);
    }

    #[test]
    fn clear_commit_offset_flag_clears_correct_flag() {
        let flag = PullSysFlag::clear_commit_offset_flag(0b1111);
        assert_eq!(flag, 0b1110);
    }

    #[test]
    fn has_commit_offset_flag_returns_correct_result() {
        assert!(PullSysFlag::has_commit_offset_flag(0b1));
        assert!(!PullSysFlag::has_commit_offset_flag(0b10));
    }

    #[test]
    fn has_suspend_flag_returns_correct_result() {
        assert!(PullSysFlag::has_suspend_flag(0b10));
        assert!(!PullSysFlag::has_suspend_flag(0b1));
    }

    #[test]
    fn clear_suspend_flag_clears_correct_flag() {
        let flag = PullSysFlag::clear_suspend_flag(0b1111);
        assert_eq!(flag, 0b1101);
    }

    #[test]
    fn has_subscription_flag_returns_correct_result() {
        assert!(PullSysFlag::has_subscription_flag(0b100));
        assert!(!PullSysFlag::has_subscription_flag(0b1));
    }

    #[test]
    fn build_sys_flag_with_subscription_sets_correct_flag() {
        let flag = PullSysFlag::build_sys_flag_with_subscription(0b1);
        assert_eq!(flag, 0b101);
    }

    #[test]
    fn has_class_filter_flag_returns_correct_result() {
        assert!(PullSysFlag::has_class_filter_flag(0b1000));
        assert!(!PullSysFlag::has_class_filter_flag(0b1));
    }

    #[test]
    fn has_lite_pull_flag_returns_correct_result() {
        assert!(PullSysFlag::has_lite_pull_flag(0b10000));
        assert!(!PullSysFlag::has_lite_pull_flag(0b1));
    }
}
