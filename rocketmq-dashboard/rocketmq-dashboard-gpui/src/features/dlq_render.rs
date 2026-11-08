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

use super::*;
impl Render for DlqView {
    fn render(&mut self, _: &mut Window, cx: &mut Context<Self>) -> impl IntoElement {
        let loading = matches!(self.store.rows, Loadable::InitialLoading | Loadable::Refreshing(_));
        let page = self.store.applied.as_ref().map_or(1, |query| query.page);
        let exact_id = self
            .store
            .applied
            .as_ref()
            .is_some_and(|query| query.message_id.is_some());
        let direct = self.services.connection_state().config.scope == ConnectionScope::NameServer;
        let ready = matches!(self.store.rows, Loadable::Ready(_));
        let next = self
            .store
            .rows
            .value()
            .is_some_and(|page| page.extent == MessagePageExtent::MoreMayExist);
        div().id("dlq-workspace").debug_selector(|| "dlq-workspace".into()).size_full().overflow_y_scrollbar()
            .child(div().w_full().p_4().flex().flex_col().gap_3()
            .child(div().w_full().flex_shrink_0().flex().flex_col().gap_3()
                .child(div().flex().flex_wrap().items_center().gap_2()
                    .child(div().debug_selector(|| "dlq-group-picker".into()).w(px(260.)).child(Select::new(&self.group_picker).placeholder("Choose a discovered Group")))
                    .child(div().flex_1().min_w(px(220.)).debug_selector(|| "dlq-group-field".into()).child(Input::new(&self.group).w_full()))
                    .child(Button::new("dlq-groups-refresh").label("Refresh Groups").outline().debug_selector(|| "dlq-groups-refresh".into()).on_click(cx.listener(|view, _, window, cx| view.load_groups(window, cx)))))
                .child(div().text_xs().debug_selector(|| "dlq-groups-status".into()).child(match &self.groups.state {
                    Loadable::InitialLoading | Loadable::Refreshing(_) => "Loading Consumer Groups… Manual entry is available.",
                    Loadable::Failed { error, .. } => error.summary(),
                    Loadable::Ready(inventory) if inventory.group_observation != ConsumerObservationState::Complete => "Group discovery is partial. Enter any additional Group manually.",
                    _ => "Choose a discovered Group or enter one manually.",
                }.to_string()))
                .child(div().flex().flex_wrap().gap_2()
                    .child(div().flex_1().min_w(px(240.)).debug_selector(|| "dlq-begin-field".into()).child("Begin UTC (YYYY-MM-DD HH:MM:SS)").child(Input::new(&self.begin).w_full()))
                    .child(div().flex_1().min_w(px(240.)).debug_selector(|| "dlq-end-field".into()).child("End UTC (inclusive)").child(Input::new(&self.end).w_full()))
                    .child(div().w(px(95.)).flex_shrink_0().child("Rows / page").child(Input::new(&self.page_size))))
                .child(div().w_full().flex_shrink_0().min_w_0().debug_selector(|| "dlq-id-field".into()).child(Input::new(&self.message_id).w_full()))
                .child(div().flex().flex_wrap().gap_2().items_center()
                    .child(Button::new("dlq-search").label("Search").outline().disabled(!direct).debug_selector(|| "dlq-search".into()).on_click(cx.listener(|view, _, window, cx| view.search(window, cx))))
                    .child(Button::new("dlq-refresh").label("Refresh applied").outline().disabled(loading || self.store.applied.is_none()).debug_selector(|| "dlq-refresh".into()).on_click(cx.listener(|view, _, window, cx| view.refresh(window, cx))))
                    .child(Button::new("dlq-previous").label("Previous").outline().disabled(loading || exact_id || page <= 1).debug_selector(|| "dlq-previous".into()).on_click(cx.listener(|view, _, window, cx| view.page(false, window, cx))))
                    .child(div().child(format!("Page {page}")))
                    .child(Button::new("dlq-next").label("Next").outline().disabled(loading || exact_id || !next).debug_selector(|| "dlq-next".into()).on_click(cx.listener(|view, _, window, cx| view.page(true, window, cx))))
                    .child(Button::new("dlq-recover").label("Review selected…").outline().disabled(!direct || !ready || self.batch_task.is_some() || self.store.selected.is_empty()).debug_selector(|| "dlq-recover".into()).on_click(cx.listener(|view, _, window, cx| view.review_recovery(false, window, cx))))
                    .child(Button::new("dlq-retry-failed").label("Review failed entries…").outline().disabled(!direct || !ready || self.batch_task.is_some() || self.store.results.as_ref().is_none_or(|result| result.retryable_targets().is_empty())).debug_selector(|| "dlq-retry-failed".into()).on_click(cx.listener(|view, _, window, cx| view.review_recovery(true, window, cx))))
                    .child(Button::new("dlq-export").label("Export applied page CSV…").outline().disabled(!direct || !ready || self.export_task.is_some()).debug_selector(|| "dlq-export".into()).on_click(cx.listener(|view, _, window, cx| view.export_page(window, cx)))))
                .child(div().text_xs().child(if direct { "Broker scan pages · No exact total is inferred. A full page may permit an empty next page. Unsafe original identities cannot be selected." } else { "DLQ query and recovery require NameServer Direct scope." }))
                .when(self.store.draft_changed, |this| this.child("Draft changed. Search applies it; Refresh replays the previous applied query."))
                .children(self.store.applied.as_ref().map(|query| div().text_xs().child(format!("Applied Group: {} · UTC {} – {} · page {}. CSV uses exactly this page, with full values or an explicit size error.", query.consumer_group, format_time(query.begin), format_time(query.end), query.page))))
                .children(self.status.as_ref().map(|status| div().text_sm().child(status.clone())))
                .children(match &self.store.rows { Loadable::Failed { error, .. } => Some(div().text_sm().text_color(cx.theme().danger).child(error.summary().to_string())), _ => None })
            )
            .child(div().debug_selector(|| "dlq-results-table".into()).w_full().h(px(320.)).flex_shrink_0().child(Table::new(&self.table)))
            .children(self.store.results.as_ref().map(|result| div().flex_shrink_0().flex().flex_col().gap_2()
                .child("Batch outcomes — unknowns require verification; only failed entries can be reviewed for retry.")
                .children(result.entries.iter().enumerate().map(|(index, entry)| div().debug_selector(move || format!("dlq-result-{index}"))
                    .text_sm().child(format!("{} · {} · Group {} · {}{}", entry.target.original().topic, entry.target.original().message_id,
                        entry.target.consumer_group(), entry.attempt.state.label(), entry.attempt.receipt.as_ref().map_or_else(String::new, |receipt| format!(" · {} · success: {}", receipt.code.label(), receipt.is_success())))))))))
    }
}
