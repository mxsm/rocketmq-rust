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

//! Searchable Topic candidates shared by resource workspaces; manual input stays caller-owned.

use gpui::{App, AppContext as _, Entity, Window};
use gpui_component::select::{SearchableVec, Select, SelectState};

pub(crate) type TopicPickerState = SelectState<SearchableVec<String>>;
pub(crate) fn state(window: &mut Window, cx: &mut App) -> Entity<TopicPickerState> {
    cx.new(|cx| SelectState::new(SearchableVec::new(Vec::<String>::new()), None, window, cx).searchable(true))
}
pub(crate) fn set_topics(
    picker: &Entity<TopicPickerState>,
    mut topics: Vec<String>,
    window: &mut Window,
    cx: &mut App,
) {
    topics.sort();
    topics.dedup();
    picker.update(cx, |picker, cx| {
        picker.set_items(SearchableVec::new(topics), window, cx);
        picker.set_selected_index(None, window, cx);
    });
}
pub(crate) fn render(state: &Entity<TopicPickerState>) -> Select<SearchableVec<String>> {
    Select::new(state).placeholder("Choose a discovered Topic")
}
