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

/// Reconciles one replay-validated state with a complete, stable namespace inventory.
pub(crate) fn reconcile(
    needs_reconciliation: NeedsReconciliation,
    mut inventory: StableNamespaceInventory,
) -> Result<ReconciliationDisposition, ReconciliationError> {
    let (recovered, writer_frontier) = needs_reconciliation.into_parts();
    if recovered.store_uuid != inventory.store_uuid {
        return Err(ReconciliationError::StoreUuidMismatch);
    }

    let known_paths = collect_known_paths(&recovered)?;
    validate_inventory_coverage(&inventory, &known_paths)?;
    validate_unique_physical_identity(&inventory)?;

    let ticket_by_incarnation = recovered
        .retirements
        .values()
        .map(|ticket| (ticket.entry.incarnation, ticket.entry.ticket_id))
        .collect::<BTreeMap<_, _>>();
    let mut active = BTreeMap::new();
    let mut retired_paths = BTreeSet::new();
    let mut retiring_tickets = BTreeSet::new();
    let mut completed_revalidated = BTreeSet::new();
    let mut actions = Vec::new();

    for incarnation in recovered.incarnations.values() {
        validate_create_artifact(&inventory, incarnation)?;
        if ticket_by_incarnation.contains_key(&incarnation.incarnation) {
            continue;
        }
        match incarnation.phase {
            IncarnationPhase::Allocated => reconcile_allocated(&inventory, incarnation, &mut actions)?,
            IncarnationPhase::Bound => reconcile_bound(&inventory, incarnation, &mut actions)?,
            IncarnationPhase::Published => {
                let physical_key = incarnation
                    .physical_key
                    .expect("validated published snapshot entry has a physical key");
                require_exact_file(
                    &inventory,
                    &incarnation.canonical_path,
                    physical_key,
                    incarnation.expected_file_length,
                )?;
                active.insert(
                    incarnation.canonical_path.clone(),
                    PublishedIncarnationBinding {
                        incarnation: incarnation.incarnation,
                        physical_key,
                        expected_length: incarnation.expected_file_length,
                        segment_offset: incarnation.segment_offset,
                    },
                );
            }
        }
    }

    for ticket in recovered.retirements.values() {
        retired_paths.insert(ticket.entry.canonical_path.clone());
        if ticket.entry.stage == RetirementStage::CompletedRetained {
            completed_revalidated.insert(ticket.entry.ticket_id);
        } else {
            retiring_tickets.insert(ticket.entry.ticket_id);
        }
        analyze_retirement(&inventory, &ticket.entry, &mut actions)?;
    }

    for quarantine in recovered.quarantines.values() {
        validate_quarantine(&inventory, quarantine)?;
    }

    if inventory.requires_retained_files {
        for path in active.keys().chain(retired_paths.iter()) {
            if inventory.entries.contains_key(path) && !inventory.retained_files.contains_key(path) {
                return Err(ReconciliationError::UnsafeNamespaceEntry { path: path.clone() });
            }
        }
    }
    let retained_files = std::mem::take(&mut inventory.retained_files);

    if actions.is_empty() {
        Ok(ReconciliationDisposition::Ready(ReconciledLedgerState {
            recovered,
            writer_frontier,
            active,
            retired_paths,
            retiring_tickets,
            completed_revalidated,
            retained_files,
        }))
    } else {
        Ok(ReconciliationDisposition::RecoveryRequired(ReconciliationPlan {
            recovered,
            writer_frontier,
            actions,
            retained_files,
        }))
    }
}

fn reconcile_allocated(
    inventory: &StableNamespaceInventory,
    incarnation: &IncarnationSnapshotEntry,
    actions: &mut Vec<ReconciliationAction>,
) -> Result<(), ReconciliationError> {
    if inventory.observe(&incarnation.canonical_path)?.is_some() {
        return Err(ReconciliationError::AllocatedCanonicalPresent {
            incarnation: incarnation.incarnation,
        });
    }
    let Some(object) = inventory.observe(&incarnation.create_file_path)? else {
        actions.push(ReconciliationAction::ResumeAllocation(incarnation.incarnation));
        return Ok(());
    };
    let NamespaceObject::RegularFile {
        physical_key, length, ..
    } = object
    else {
        return Err(ReconciliationError::UnsafeNamespaceEntry {
            path: incarnation.create_file_path.clone(),
        });
    };
    if *length != incarnation.expected_file_length {
        return Err(ReconciliationError::LengthMismatch {
            path: incarnation.create_file_path.clone(),
            expected: incarnation.expected_file_length,
            actual: *length,
        });
    }
    actions.push(ReconciliationAction::RecordBound {
        incarnation: incarnation.incarnation,
        physical_key: *physical_key,
    });
    actions.push(ReconciliationAction::PublishBoundIncarnation(incarnation.incarnation));
    Ok(())
}

fn reconcile_bound(
    inventory: &StableNamespaceInventory,
    incarnation: &IncarnationSnapshotEntry,
    actions: &mut Vec<ReconciliationAction>,
) -> Result<(), ReconciliationError> {
    let expected_key = incarnation
        .physical_key
        .expect("validated bound snapshot entry has a physical key");
    let create = observe_expected(
        inventory,
        &incarnation.create_file_path,
        expected_key,
        incarnation.expected_file_length,
    )?;
    let canonical = observe_expected(
        inventory,
        &incarnation.canonical_path,
        expected_key,
        incarnation.expected_file_length,
    )?;
    match (create, canonical) {
        (ObservedExpected::Expected, ObservedExpected::Missing) => {
            actions.push(ReconciliationAction::PublishBoundIncarnation(incarnation.incarnation))
        }
        (ObservedExpected::Missing, ObservedExpected::Expected) => {
            actions.push(ReconciliationAction::RecordPublished(incarnation.incarnation));
        }
        (ObservedExpected::Missing, ObservedExpected::Missing) => {
            return Err(ReconciliationError::BoundIncarnationMissing {
                incarnation: incarnation.incarnation,
            });
        }
        (ObservedExpected::Other(actual), _) => {
            return Err(ReconciliationError::PhysicalKeyMismatch {
                path: incarnation.create_file_path.clone(),
                expected: expected_key,
                actual,
            });
        }
        (_, ObservedExpected::Other(actual)) => {
            return Err(ReconciliationError::PhysicalKeyMismatch {
                path: incarnation.canonical_path.clone(),
                expected: expected_key,
                actual,
            });
        }
        (ObservedExpected::Expected, ObservedExpected::Expected) => {
            return Err(ReconciliationError::DuplicatePhysicalIdentity {
                physical_key: expected_key,
                first: incarnation.create_file_path.clone(),
                second: incarnation.canonical_path.clone(),
            });
        }
    }
    Ok(())
}

pub(super) fn collect_known_paths(
    state: &RecoveredLedgerState,
) -> Result<BTreeSet<StoreRelativePath>, ReconciliationError> {
    let mut known = BTreeSet::new();
    for incarnation in state.incarnations.values() {
        known.insert(incarnation.canonical_path.clone());
        known.insert(incarnation.create_file_path.clone());
    }
    for ticket in state.retirements.values() {
        known.insert(ticket.entry.canonical_path.clone());
        let expected = expected_tombstone_path(&ticket.entry)?;
        if let Some(persisted) = &ticket.entry.tombstone_path {
            if persisted != &expected {
                return Err(ReconciliationError::TombstonePathMismatch {
                    ticket_id: ticket.entry.ticket_id,
                });
            }
        }
        known.insert(expected);
    }
    for quarantine in state.quarantines.values() {
        known.insert(quarantine.source_path.clone());
        if let Some(destination) = &quarantine.destination_path {
            known.insert(destination.clone());
        }
    }
    Ok(known)
}

fn validate_inventory_coverage(
    inventory: &StableNamespaceInventory,
    known_paths: &BTreeSet<StoreRelativePath>,
) -> Result<(), ReconciliationError> {
    for path in known_paths {
        let _ = inventory.observe(path)?;
    }
    for path in inventory.entries.keys() {
        if !known_paths.contains(path) {
            return Err(ReconciliationError::UntrackedNamespaceEntry { path: path.clone() });
        }
    }
    Ok(())
}

fn validate_unique_physical_identity(inventory: &StableNamespaceInventory) -> Result<(), ReconciliationError> {
    let mut paths_by_key = BTreeMap::new();
    for (path, object) in &inventory.entries {
        let NamespaceObject::RegularFile { physical_key, .. } = object else {
            continue;
        };
        if let Some(first) = paths_by_key.insert(*physical_key, path.clone()) {
            return Err(ReconciliationError::DuplicatePhysicalIdentity {
                physical_key: *physical_key,
                first,
                second: path.clone(),
            });
        }
    }
    Ok(())
}

fn validate_create_artifact(
    inventory: &StableNamespaceInventory,
    incarnation: &IncarnationSnapshotEntry,
) -> Result<(), ReconciliationError> {
    if incarnation.phase == IncarnationPhase::Published && inventory.observe(&incarnation.create_file_path)?.is_some() {
        return Err(ReconciliationError::UnexpectedCreateArtifact {
            path: incarnation.create_file_path.clone(),
        });
    }
    Ok(())
}

fn analyze_retirement(
    inventory: &StableNamespaceInventory,
    ticket: &RetirementTicketSnapshotEntry,
    actions: &mut Vec<ReconciliationAction>,
) -> Result<(), ReconciliationError> {
    let canonical = observe_expected(
        inventory,
        &ticket.canonical_path,
        ticket.target_key,
        ticket.expected_file_length,
    )?;
    let tombstone_path = expected_tombstone_path(ticket)?;
    let tombstone = observe_expected(
        inventory,
        &tombstone_path,
        ticket.target_key,
        ticket.expected_file_length,
    )?;
    if let ObservedExpected::Other(_) = tombstone {
        return Err(ReconciliationError::TombstoneCollision {
            ticket_id: ticket.ticket_id,
        });
    }
    if canonical == ObservedExpected::Expected && tombstone == ObservedExpected::Expected {
        return Err(ReconciliationError::TombstoneCollision {
            ticket_id: ticket.ticket_id,
        });
    }

    let replacement_key = match canonical {
        ObservedExpected::Other(key) => Some(key),
        ObservedExpected::Missing | ObservedExpected::Expected => None,
    };
    if let Some(key) = replacement_key {
        if !ticket.superseded_path_observed {
            actions.push(ReconciliationAction::RecordSupersededPath {
                ticket_id: ticket.ticket_id,
                replacement_key: key,
            });
        }
    }

    match ticket.stage {
        RetirementStage::IntentDurable => match (canonical, tombstone) {
            (ObservedExpected::Expected, ObservedExpected::Missing) => {}
            (ObservedExpected::Missing | ObservedExpected::Other(_), ObservedExpected::Expected) => {
                actions.push(ReconciliationAction::RecordLogicalRemoved(ticket.ticket_id));
                actions.push(ReconciliationAction::RecordTombstoned(ticket.ticket_id));
            }
            (ObservedExpected::Missing | ObservedExpected::Other(_), ObservedExpected::Missing) => {
                actions.push(ReconciliationAction::RecordLogicalRemoved(ticket.ticket_id));
                push_absence_and_completion(actions, ticket.ticket_id, replacement_key);
            }
            _ => {
                return Err(ReconciliationError::DurableStageContradiction {
                    ticket_id: ticket.ticket_id,
                })
            }
        },
        RetirementStage::LogicalRemoved => match (canonical, tombstone) {
            (ObservedExpected::Expected, ObservedExpected::Missing) => {}
            (ObservedExpected::Missing | ObservedExpected::Other(_), ObservedExpected::Expected) => {
                actions.push(ReconciliationAction::RecordTombstoned(ticket.ticket_id));
            }
            (ObservedExpected::Missing | ObservedExpected::Other(_), ObservedExpected::Missing) => {
                push_absence_and_completion(actions, ticket.ticket_id, replacement_key);
            }
            _ => {
                return Err(ReconciliationError::DurableStageContradiction {
                    ticket_id: ticket.ticket_id,
                })
            }
        },
        RetirementStage::Tombstoned => match (canonical, tombstone) {
            (ObservedExpected::Missing | ObservedExpected::Other(_), ObservedExpected::Expected) => {}
            (ObservedExpected::Missing | ObservedExpected::Other(_), ObservedExpected::Missing) => {
                push_absence_and_completion(actions, ticket.ticket_id, replacement_key);
            }
            _ => {
                return Err(ReconciliationError::DurableStageContradiction {
                    ticket_id: ticket.ticket_id,
                })
            }
        },
        RetirementStage::NamespaceAbsent => match (canonical, tombstone) {
            (ObservedExpected::Missing | ObservedExpected::Other(_), ObservedExpected::Missing) => {
                actions.push(ReconciliationAction::RecordCompleted(ticket.ticket_id));
            }
            _ => {
                return Err(ReconciliationError::DurableStageContradiction {
                    ticket_id: ticket.ticket_id,
                })
            }
        },
        RetirementStage::CompletedRetained => match (canonical, tombstone) {
            (ObservedExpected::Expected, _) => {
                return Err(ReconciliationError::CompletedTargetReappeared {
                    ticket_id: ticket.ticket_id,
                    path: ticket.canonical_path.clone(),
                });
            }
            (ObservedExpected::Missing | ObservedExpected::Other(_), ObservedExpected::Missing) => {}
            _ => {
                return Err(ReconciliationError::DurableStageContradiction {
                    ticket_id: ticket.ticket_id,
                })
            }
        },
    }
    Ok(())
}

fn push_absence_and_completion(
    actions: &mut Vec<ReconciliationAction>,
    ticket_id: TicketId,
    replacement_key: Option<PhysicalFileKey>,
) {
    actions.push(ReconciliationAction::RecordNamespaceAbsent {
        ticket_id,
        replacement_key,
    });
    actions.push(ReconciliationAction::RecordCompleted(ticket_id));
}

fn validate_quarantine(
    inventory: &StableNamespaceInventory,
    quarantine: &QuarantineSnapshotEntry,
) -> Result<(), ReconciliationError> {
    let path = quarantine.destination_path.as_ref().unwrap_or(&quarantine.source_path);
    if quarantine.destination_path.is_some() && inventory.observe(&quarantine.source_path)?.is_some() {
        return Err(ReconciliationError::QuarantineMismatch {
            path: quarantine.source_path.clone(),
        });
    }
    let Some(object) = inventory.observe(path)? else {
        return Err(ReconciliationError::QuarantineMismatch { path: path.clone() });
    };
    let NamespaceObject::RegularFile {
        physical_key,
        content_fingerprint,
        ..
    } = object
    else {
        return Err(ReconciliationError::UnsafeNamespaceEntry { path: path.clone() });
    };
    if quarantine
        .physical_key
        .is_some_and(|expected| expected != *physical_key)
        || quarantine
            .content_fingerprint
            .is_some_and(|expected| Some(expected) != *content_fingerprint)
    {
        return Err(ReconciliationError::QuarantineMismatch { path: path.clone() });
    }
    Ok(())
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ObservedExpected {
    Missing,
    Expected,
    Other(PhysicalFileKey),
}

fn observe_expected(
    inventory: &StableNamespaceInventory,
    path: &StoreRelativePath,
    expected_key: PhysicalFileKey,
    expected_length: u64,
) -> Result<ObservedExpected, ReconciliationError> {
    let Some(object) = inventory.observe(path)? else {
        return Ok(ObservedExpected::Missing);
    };
    let NamespaceObject::RegularFile {
        physical_key, length, ..
    } = object
    else {
        return Err(ReconciliationError::UnsafeNamespaceEntry { path: path.clone() });
    };
    if *physical_key != expected_key {
        return Ok(ObservedExpected::Other(*physical_key));
    }
    if *length != expected_length {
        return Err(ReconciliationError::LengthMismatch {
            path: path.clone(),
            expected: expected_length,
            actual: *length,
        });
    }
    Ok(ObservedExpected::Expected)
}

fn require_exact_file(
    inventory: &StableNamespaceInventory,
    path: &StoreRelativePath,
    expected_key: PhysicalFileKey,
    expected_length: u64,
) -> Result<(), ReconciliationError> {
    match observe_expected(inventory, path, expected_key, expected_length)? {
        ObservedExpected::Expected => Ok(()),
        ObservedExpected::Missing => Err(ReconciliationError::MissingPublishedFile { path: path.clone() }),
        ObservedExpected::Other(actual) => Err(ReconciliationError::PhysicalKeyMismatch {
            path: path.clone(),
            expected: expected_key,
            actual,
        }),
    }
}

fn expected_tombstone_path(ticket: &RetirementTicketSnapshotEntry) -> Result<StoreRelativePath, ReconciliationError> {
    ticket
        .canonical_path
        .tombstone_path(
            ticket.ticket_id,
            ticket.incarnation,
            ticket.segment_offset,
            ticket.mapping_generation,
            &ticket.retirement_nonce,
        )
        .map_err(|_| ReconciliationError::TombstonePathMismatch {
            ticket_id: ticket.ticket_id,
        })
}

pub(super) fn parent_directory(path: &StoreRelativePath) -> &str {
    path.as_str().rsplit_once('/').map_or("", |(parent, _)| parent)
}
