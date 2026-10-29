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

//! Read-only Producer observations and explicit Topic + Group connection query.

use gpui::prelude::FluentBuilder as _;
use gpui::{
    AppContext as _, ClipboardItem, Context, Entity, InteractiveElement as _, IntoElement, ParentElement as _, Render,
    StatefulInteractiveElement as _, Styled as _, Subscription, Task, Window, div, px,
};
use gpui_component::{
    ActiveTheme as _, Disableable as _, Sizable as _, WindowExt as _,
    button::Button,
    input::{Input, InputEvent, InputState},
    select::{SearchableVec, Select, SelectEvent, SelectItem, SelectState},
};
use rocketmq_dashboard_common::{ConsumerClientObservation, ConsumerObservation, ConsumerObservationState};

use crate::{features::producers_store::ProducersStore, services::AppServices, state::Loadable};

#[derive(Clone)]
struct ProducerTopicOption(String);
impl SelectItem for ProducerTopicOption {
    type Value = String;
    fn title(&self) -> gpui::SharedString {
        self.0.clone().into()
    }
    fn value(&self) -> &String {
        &self.0
    }
}

pub struct ProducersView {
    services: AppServices,
    revision: u64,
    pub store: ProducersStore,
    keyword: Entity<InputState>,
    topic: Entity<InputState>,
    group: Entity<InputState>,
    topic_picker: Entity<SelectState<SearchableVec<ProducerTopicOption>>>,
    _subscriptions: Vec<Subscription>,
    inventory_task: Option<Task<()>>,
    topics_task: Option<Task<()>>,
    connections_task: Option<Task<()>>,
    validation_error: Option<&'static str>,
    client_sheet_open: bool,
}

impl ProducersView {
    pub fn new(window: &mut Window, services: AppServices, revision: u64, cx: &mut Context<Self>) -> Self {
        let keyword = cx.new(|cx| InputState::new(window, cx).placeholder("Search Producer group"));
        let topic = cx.new(|cx| InputState::new(window, cx).placeholder("Topic"));
        let group = cx.new(|cx| InputState::new(window, cx).placeholder("Producer group"));
        let topic_picker = cx.new(|cx| {
            SelectState::new(SearchableVec::<ProducerTopicOption>::new(Vec::new()), None, window, cx).searchable(true)
        });
        let subscriptions = vec![
            cx.subscribe_in(&topic_picker, window, |view, _, event, window, cx| {
                if let SelectEvent::Confirm(Some(topic)) = event {
                    view.use_topic(topic.clone(), window, cx);
                }
            }),
            cx.subscribe_in(&keyword, window, |view, input, event: &InputEvent, _, cx| {
                if matches!(event, InputEvent::Change) {
                    view.store.draft_filter.keyword = input.read(cx).value().to_string();
                    cx.notify();
                }
            }),
            cx.subscribe_in(&topic, window, |view, input, event: &InputEvent, _, cx| {
                if matches!(event, InputEvent::Change) {
                    view.store.draft_query.topic = input.read(cx).value().to_string();
                    view.validation_error = None;
                    cx.notify();
                }
            }),
            cx.subscribe_in(&group, window, |view, input, event: &InputEvent, _, cx| {
                if matches!(event, InputEvent::Change) {
                    view.store.draft_query.group = input.read(cx).value().to_string();
                    view.validation_error = None;
                    cx.notify();
                }
            }),
        ];
        Self {
            services,
            revision,
            store: ProducersStore::default(),
            keyword,
            topic,
            group,
            topic_picker,
            _subscriptions: subscriptions,
            inventory_task: None,
            topics_task: None,
            connections_task: None,
            validation_error: None,
            client_sheet_open: false,
        }
    }

    #[cfg(test)]
    pub(crate) fn query_inputs_for_test(&self) -> (Entity<InputState>, Entity<InputState>) {
        (self.topic.clone(), self.group.clone())
    }

    pub fn set_revision(&mut self, revision: u64, cx: &mut Context<Self>) {
        let loaded = !matches!(self.store.inventory.state, Loadable::Idle);
        if revision != self.revision {
            self.revision = revision;
            self.store.clear_for_revision();
        }
        if loaded {
            self.refresh(cx);
        }
    }

    pub fn clear_for_environment(&mut self, revision: u64, window: &mut Window, cx: &mut Context<Self>) {
        self.revision = revision;
        self.inventory_task = None;
        self.topics_task = None;
        self.topic_picker.update(cx, |picker, cx| {
            picker.set_items(SearchableVec::<ProducerTopicOption>::new(Vec::new()), window, cx)
        });
        self.connections_task = None;
        self.store.clear_for_revision();
        self.store.draft_filter = Default::default();
        self.store.applied_filter = Default::default();
        self.store.draft_query = Default::default();
        self.store.page = 1;
        self.validation_error = None;
        self.client_sheet_open = false;
        for input in [&self.keyword, &self.topic, &self.group] {
            input.update(cx, |input, cx| input.set_value("", window, cx));
        }
        cx.notify();
    }

    pub fn ensure_loaded(&mut self, window: &mut Window, cx: &mut Context<Self>) {
        if matches!(self.store.topics.state, Loadable::Idle) {
            self.refresh_topics(window, cx);
        }
        if matches!(self.store.inventory.state, Loadable::Idle) {
            let Some(request) = self.store.begin_inventory(self.revision) else {
                return;
            };
            let services = self.services.clone();
            self.inventory_task = Some(cx.spawn_in(window, async move |this, cx| {
                let result = services.producer_inventory(request.scope).await;
                let _ = this.update(cx, |view, cx| {
                    view.store.finish_inventory(request, view.revision, result);
                    cx.notify();
                });
            }));
        }
    }

    fn refresh(&mut self, cx: &mut Context<Self>) {
        let Some(request) = self.store.begin_inventory(self.revision) else {
            return;
        };
        let services = self.services.clone();
        self.inventory_task = Some(cx.spawn(async move |this, cx| {
            let result = services.producer_inventory(request.scope).await;
            let _ = this.update(cx, |view, cx| {
                view.store.finish_inventory(request, view.revision, result);
                cx.notify();
            });
        }));
    }

    fn refresh_topics(&mut self, window: &mut Window, cx: &mut Context<Self>) {
        let Some(request) = self.store.topics.begin(self.revision) else {
            return;
        };
        let services = self.services.clone();
        self.topics_task = Some(cx.spawn_in(window, async move |this, cx| {
            let result = services
                .topic_inventory(crate::services::topics::TopicRequestScope {
                    revision: request.revision(),
                    epoch: request.epoch(),
                })
                .await;
            let _ = this.update_in(cx, |view, window, cx| {
                let accepted = view.store.topics.finish(
                    request,
                    view.revision,
                    result.map(|inventory| {
                        let mut topics = inventory
                            .items
                            .into_iter()
                            .map(|item| item.identity.as_str().to_owned())
                            .collect::<Vec<_>>();
                        topics.sort();
                        topics.dedup();
                        Some(rocketmq_dashboard_common::ProducerTopicCandidates {
                            topics,
                            observation: match inventory.completeness {
                                rocketmq_dashboard_common::TopicCompleteness::Complete => {
                                    ConsumerObservationState::Complete
                                }
                                rocketmq_dashboard_common::TopicCompleteness::Partial { .. } => {
                                    ConsumerObservationState::Partial
                                }
                            },
                            failed_targets: inventory.failures.len(),
                        })
                    }),
                );
                if accepted {
                    let options: Vec<ProducerTopicOption> = view
                        .store
                        .topics
                        .state
                        .value()
                        .map(|topics| topics.topics.iter().cloned().map(ProducerTopicOption).collect())
                        .unwrap_or_default();
                    view.topic_picker.update(cx, |picker, cx| {
                        picker.set_items(SearchableVec::new(options), window, cx)
                    });
                }
                cx.notify();
            });
        }));
    }

    fn use_topic(&mut self, topic: String, window: &mut Window, cx: &mut Context<Self>) {
        self.store.draft_query.topic = topic.clone();
        self.topic.update(cx, |input, cx| input.set_value(topic, window, cx));
        self.validation_error = None;
        cx.notify();
    }

    fn apply_filter(&mut self, cx: &mut Context<Self>) {
        self.store.apply_filter();
        cx.notify();
    }

    fn use_group(&mut self, group: String, window: &mut Window, cx: &mut Context<Self>) {
        self.store.draft_query.group = group.clone();
        self.group.update(cx, |input, cx| input.set_value(group, window, cx));
        self.validation_error = None;
        cx.notify();
    }

    fn apply_query(&mut self, window: &mut Window, cx: &mut Context<Self>) {
        let query = match self.store.apply_query() {
            Ok(query) => query,
            Err(_) => {
                self.validation_error = Some("Both a valid Topic and Producer group are required before Apply.");
                cx.notify();
                return;
            }
        };
        let Some(request) = self.store.begin_connections(self.revision) else {
            return;
        };
        if self.client_sheet_open {
            window.close_sheet(cx);
            self.client_sheet_open = false;
        }
        self.topic.update(cx, |input, cx| input.focus(window, cx));
        let services = self.services.clone();
        self.connections_task = Some(cx.spawn_in(window, async move |this, cx| {
            let result = services.producer_connections(request.scope, query).await;
            let _ = this.update(cx, |view, cx| {
                view.store.finish_connections(request, view.revision, result);
                cx.notify();
            });
        }));
    }

    fn open_client(&mut self, client: ConsumerClientObservation, window: &mut Window, cx: &mut Context<Self>) {
        if window.has_active_sheet(cx) {
            if self.client_sheet_open {
                window.close_sheet(cx);
            } else {
                return;
            }
        }
        self.store.select_client(client.identity.clone());
        self.client_sheet_open = true;
        let owner = cx.entity().downgrade();
        let apply_owner = owner.clone();
        let topic = self.topic.clone();
        let group = self.group.clone();
        let title = client.identity.as_str().to_owned();
        window.open_sheet(cx, move |sheet, _, _| {
            let owner = owner.clone();
            let apply_owner = apply_owner.clone();
            sheet
                .title(title.clone())
                .size(px(520.))
                .on_close(move |_, window, cx| {
                    let _ = owner.update(cx, |view, cx| {
                        view.client_sheet_open = false;
                        view.store.close_client();
                        view.topic.update(cx, |input, cx| input.focus(window, cx));
                        cx.notify();
                    });
                })
                .child(
                    div()
                        .p_4()
                        .flex()
                        .flex_col()
                        .gap_2()
                        .child(format!("Client: {}", client.identity.as_str()))
                        .child(format!("Address: {}", client.address))
                        .child(format!("Language: {}", client.language))
                        .child(format!("Version: {} ({})", client.version_description, client.version))
                        .children(
                            [
                                (
                                    "producer-copy-client",
                                    "Copy client ID",
                                    client.identity.as_str().to_owned(),
                                ),
                                ("producer-copy-address", "Copy address", client.address.clone()),
                                ("producer-copy-language", "Copy language", client.language.clone()),
                                (
                                    "producer-copy-version",
                                    "Copy version",
                                    format!("{} ({})", client.version_description, client.version),
                                ),
                            ]
                            .into_iter()
                            .map(|(id, label, value)| {
                                Button::new(id)
                                    .debug_selector(move || id.to_owned())
                                    .label(label)
                                    .on_click(move |_, _, cx| {
                                        cx.write_to_clipboard(ClipboardItem::new_string(value.clone()))
                                    })
                            }),
                        )
                        .child(div().h(px(1.)))
                        .child("Apply a different Topic + Group identity")
                        .child(Input::new(&topic))
                        .child(Input::new(&group))
                        .child(
                            Button::new("producer-sheet-apply-query")
                                .label("Apply and close client")
                                .debug_selector(|| "producer-sheet-apply-query".to_owned())
                                .on_click(move |_, window, cx| {
                                    let _ = apply_owner.update(cx, |view, cx| view.apply_query(window, cx));
                                }),
                        )
                        .child(div().whitespace_normal().child(
                            "Read-only observation. No status, TPS, latency, or last-updated value is inferred.",
                        )),
                )
        });
    }

    pub fn close_owned_sheet(&mut self, window: &mut Window, cx: &mut Context<Self>) {
        if self.client_sheet_open {
            window.close_sheet(cx);
            self.client_sheet_open = false;
            self.store.close_client();
            self.topic.update(cx, |input, cx| input.focus(window, cx));
        }
    }

    fn render_connections(&self, cx: &mut Context<Self>) -> gpui::Div {
        let Some(observation) = self.store.connections.state.value() else {
            return match &self.store.connections.state {
                Loadable::Failed { error, .. } => div().child(error.summary().to_owned()),
                Loadable::Idle => div().child("Apply a Topic + Group query to load Producer clients."),
                _ => div().child("Loading Producer clients…"),
            };
        };
        let mut body = div().flex().flex_col().gap_2().child(match observation {
            ConsumerObservation::Complete(_) => "Complete observation",
            ConsumerObservation::Partial { .. } => "Partial observation — missing targets are not assumed empty.",
            ConsumerObservation::Unknown { .. } => "Unknown — no status or zero count is inferred.",
        });
        if let Some(connections) = observation.value() {
            for (index, client) in connections.clients.iter().enumerate() {
                let selected = client.clone();
                body = body.child(
                    div()
                        .flex()
                        .items_center()
                        .gap_2()
                        .child(format!(
                            "{} · {} · {}",
                            client.identity.as_str(),
                            client.language,
                            client.version_description
                        ))
                        .child(
                            Button::new(("producer-open-client", index))
                                .debug_selector(move || format!("producer-open-client-{index}"))
                                .label("Open client")
                                .small()
                                .outline()
                                .on_click(cx.listener(move |view, _, window, cx| {
                                    view.open_client(selected.clone(), window, cx);
                                })),
                        ),
                );
            }
        }
        body
    }
}

impl Render for ProducersView {
    fn render(&mut self, _window: &mut Window, cx: &mut Context<Self>) -> impl IntoElement {
        let page = self.store.page();
        let partial = self
            .store
            .inventory
            .state
            .value()
            .is_some_and(|inventory| inventory.observation == ConsumerObservationState::Partial);
        let direct_scope =
            self.services.connection_state().config.scope == rocketmq_dashboard_common::ConnectionScope::NameServer;
        let apply_enabled = !self.store.draft_query.topic.trim().is_empty()
            && !self.store.draft_query.group.trim().is_empty()
            && direct_scope;
        let mut list = div().flex().flex_col().gap_2();
        for (index, item) in page.items.iter().enumerate() {
            let group = item.identity.as_str().to_owned();
            let selected = group == self.store.draft_query.group;
            list = list.child(
                div()
                    .id(("producer-group-row", index))
                    .debug_selector(move || format!("producer-group-row-{index}"))
                    .on_click({
                        let group = group.clone();
                        cx.listener(move |view, _, window, cx| view.use_group(group.clone(), window, cx))
                    })
                    .flex()
                    .items_center()
                    .gap_3()
                    .p_2()
                    .when(selected, |row| row.bg(cx.theme().primary.opacity(0.12)))
                    .child(group.clone())
                    .child(format!("Observed clients: {}", observed_count(&item.client_count)))
                    .child(
                        Button::new(("producer-use-group", index))
                            .label("Select group")
                            .debug_selector(move || format!("producer-use-group-{index}"))
                            .small()
                            .outline()
                            .on_click(cx.listener(move |view, _, window, cx| {
                                view.use_group(group.clone(), window, cx);
                            })),
                    ),
            );
        }
        div()
            .id("producers-page")
            .size_full()
            .px_6()
            .pb_6()
            .flex()
            .flex_col()
            .gap_3()
            .when(!direct_scope, |body| {
                body.child(
                    "Producer discovery and client inspection require NameServer Direct scope in connection settings.",
                )
            })
            .when(partial, |this| {
                this.child(
                    div()
                        .p_3()
                        .rounded_md()
                        .bg(cx.theme().warning.opacity(0.12))
                        .child("Partial Producer discovery — unavailable targets are not inferred as empty."),
                )
            })
            .child(
                div()
                    .flex()
                    .gap_2()
                    .child(Input::new(&self.keyword).w(px(260.)))
                    .child(
                        Button::new("producer-search")
                            .label("Apply search")
                            .on_click(cx.listener(|view, _, _, cx| view.apply_filter(cx))),
                    )
                    .child(Button::new("producer-refresh").label("Refresh").on_click(cx.listener(
                        |view, _, window, cx| {
                            view.refresh(cx);
                            view.refresh_topics(window, cx);
                        },
                    ))),
            )
            .child(list)
            .child(
                div()
                    .flex()
                    .items_center()
                    .gap_2()
                    .child(
                        Button::new("producer-prev-page")
                            .label("Previous")
                            .disabled(page.page <= 1)
                            .on_click(cx.listener(|view, _, _, cx| {
                                view.store.set_page(view.store.page.saturating_sub(1));
                                cx.notify();
                            })),
                    )
                    .child(format!("Page {} of {}", page.page, page.page_count))
                    .child(
                        Button::new("producer-next-page")
                            .label("Next")
                            .disabled(page.page >= page.page_count)
                            .on_click(cx.listener(|view, _, _, cx| {
                                view.store.set_page(view.store.page.saturating_add(1));
                                cx.notify();
                            })),
                    ),
            )
            .child(div().h(px(1.)).bg(cx.theme().border))
            .when_some(
                match &self.store.inventory.state {
                    Loadable::Failed { error, .. } => Some(error.summary().to_owned()),
                    _ => None,
                },
                |body, error| {
                    body.child(format!(
                        "Producer discovery failed: {error} Enter and validate a Topic + Group below."
                    ))
                },
            )
            .child(
                div()
                    .flex()
                    .flex_wrap()
                    .items_center()
                    .gap_2()
                    .child("Discovered Topics")
                    .child(
                        div()
                            .id("producer-topic-candidates")
                            .debug_selector(|| "producer-topic-candidates".to_owned())
                            .w(px(300.))
                            .child(Select::new(&self.topic_picker)),
                    ),
            )
            .child(match &self.store.topics.state {
                Loadable::Failed { .. } => {
                    "Topic discovery unavailable. Manual Topic + Group entry is supported and validated."
                }
                Loadable::InitialLoading => "Loading real Topic candidates… Manual entry is available.",
                Loadable::Ready(topics) if topics.observation != ConsumerObservationState::Complete => {
                    "Partial Topic discovery. Only returned candidates are shown; manual entry remains available."
                }
                Loadable::Ready(topics) if topics.topics.is_empty() => {
                    "No Topics discovered. Manual entry remains available."
                }
                _ => "Choose a discovered Topic or enter a Topic manually, then Apply the Topic + Group together.",
            })
            .when_some(self.store.applied_query.as_ref(), |body, query| {
                body.child(format!(
                    "Applied query: Topic={} · Group={}",
                    query.topic(),
                    query.group().as_str()
                ))
            })
            .child(
                div()
                    .flex()
                    .gap_2()
                    .child(Input::new(&self.topic).w(px(240.)))
                    .child(Input::new(&self.group).w(px(280.)))
                    .child(
                        Button::new("producer-apply-query")
                            .label("Apply")
                            .disabled(!apply_enabled)
                            .debug_selector(|| "producer-apply-query".to_owned())
                            .on_click(cx.listener(|view, _, window, cx| view.apply_query(window, cx))),
                    ),
            )
            .when_some(self.validation_error, |this, error| this.child(div().child(error)))
            .child(self.render_connections(cx))
    }
}

fn observed_count(observation: &ConsumerObservation<usize>) -> String {
    match observation {
        ConsumerObservation::Complete(value) => value.to_string(),
        ConsumerObservation::Partial { value, .. } => format!("Partial ({value})"),
        ConsumerObservation::Unknown { .. } => "Unknown".into(),
    }
}
