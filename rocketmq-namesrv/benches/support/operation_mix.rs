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

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum Operation {
    Read,
    Write,
}

impl Operation {
    pub(crate) const fn is_read(self) -> bool {
        matches!(self, Self::Read)
    }

    pub(crate) const fn is_write(self) -> bool {
        matches!(self, Self::Write)
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum OperationMix {
    NinetyTen,
}

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub(crate) struct OperationCounts {
    pub(crate) reads: usize,
    pub(crate) writes: usize,
}

impl OperationMix {
    pub(crate) const fn operation_at(self, index: usize) -> Operation {
        match self {
            Self::NinetyTen if index % 10 == 0 => Operation::Write,
            Self::NinetyTen => Operation::Read,
        }
    }

    pub(crate) fn trace(self, operation_count: usize) -> Vec<Operation> {
        (0..operation_count).map(|index| self.operation_at(index)).collect()
    }

    pub(crate) fn counts(self, operation_count: usize) -> OperationCounts {
        self.trace(operation_count)
            .into_iter()
            .fold(OperationCounts::default(), |mut counts, operation| {
                if operation.is_read() {
                    counts.reads += 1;
                } else {
                    counts.writes += 1;
                }
                counts
            })
    }
}
