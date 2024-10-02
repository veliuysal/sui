// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use crate::config::RetentionPolicies;
use crate::errors::IndexerError;
use crate::models::watermarks::StoredWatermark;
use crate::store::pg_partition_manager::PgPartitionManager;
use crate::store::PgIndexerStore;
use crate::{metrics::IndexerMetrics, store::IndexerStore, types::IndexerResult};
use mysten_metrics::spawn_monitored_task;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::time::Duration;
use strum_macros;
use tokio_util::sync::CancellationToken;
use tracing::info;

pub struct Pruner {
    pub store: PgIndexerStore,
    pub partition_manager: PgPartitionManager,
    pub retention_policies: RetentionPolicies,
    pub metrics: IndexerMetrics,
}

/// Enum representing tables that the pruner is allowed to prune. The pruner will ignore any table
/// that is not listed here.
#[derive(
    Debug,
    Eq,
    PartialEq,
    strum_macros::Display,
    strum_macros::EnumString,
    strum_macros::EnumIter,
    strum_macros::AsRefStr,
    Hash,
    Serialize,
    Deserialize,
    Clone,
)]
#[strum(serialize_all = "snake_case")]
#[serde(rename_all = "snake_case")]
pub enum PrunableTable {
    ObjectsHistory,
    Transactions,
    Events,

    EventEmitPackage,
    EventEmitModule,
    EventSenders,
    EventStructInstantiation,
    EventStructModule,
    EventStructName,
    EventStructPackage,

    TxAffectedAddresses,
    TxAffectedObjects,
    TxCallsPkg,
    TxCallsMod,
    TxCallsFun,
    TxChangedObjects,
    TxDigests,
    TxInputObjects,
    TxKinds,
    TxRecipients,
    TxSenders,

    Checkpoints,
    PrunerCpWatermark,
}

impl PrunableTable {
    /// Given a committer's report of the latest written checkpoint and tx, return the value that
    /// corresponds to the variant's unit to be used by readers.
    pub fn map_to_reader_unit(&self, cp: u64, tx: u64) -> u64 {
        match self {
            PrunableTable::ObjectsHistory
            | PrunableTable::Checkpoints
            | PrunableTable::PrunerCpWatermark => cp,
            _ => tx,
        }
    }
}

impl Pruner {
    /// Instantiates a pruner with default retention and overrides. Pruner will finalize the
    /// retention policies so there is a value for every prunable table.
    pub fn new(
        store: PgIndexerStore,
        retention_policies: RetentionPolicies,
        metrics: IndexerMetrics,
    ) -> Result<Self, IndexerError> {
        let partition_manager = PgPartitionManager::new(store.pool())?;

        Ok(Self {
            store,
            partition_manager,
            retention_policies: retention_policies.finalize(),
            metrics,
        })
    }

    pub async fn start(&self, cancel: CancellationToken) -> IndexerResult<()> {
        let store_clone = self.store.clone();
        let retention_policies = self.retention_policies.policies.clone();
        let cancel_clone = cancel.clone();
        spawn_monitored_task!(update_watermarks_lower_bounds_task(
            store_clone,
            retention_policies,
            cancel_clone
        ));

        while !cancel.is_cancelled() {
            let watermarks = self.store.get_watermarks().await?;
            // Not all partitioned tables are epoch-partitioned, so we need to filter them out.
            let table_partitions: HashMap<_, _> = self
                .partition_manager
                .get_table_partitions()
                .await?
                .into_iter()
                .filter(|(table_name, _)| {
                    self.partition_manager
                        .get_strategy(table_name)
                        .is_epoch_partitioned()
                })
                .collect();

            for watermark in watermarks.iter() {
                tokio::time::sleep(Duration::from_millis(watermark.prune_delay(1000))).await;

                // Prune as an epoch-partitioned table
                if table_partitions.get(watermark.entity.as_ref()).is_some() {
                    let mut prune_start = watermark.pruner_lo();
                    while prune_start < watermark.epoch_lo {
                        if cancel.is_cancelled() {
                            info!("Pruner task cancelled.");
                            return Ok(());
                        }
                        self.partition_manager
                            .drop_table_partition(
                                watermark.entity.as_ref().to_string(),
                                prune_start,
                            )
                            .await?;
                        info!(
                            "Batch dropped table partition {} epoch {}",
                            watermark.entity, prune_start
                        );
                        prune_start += 1;

                        // Then need to update the `pruned_lo`
                        self.store
                            .update_watermark_latest_pruned(watermark.entity.clone(), prune_start)
                            .await?;
                    }
                } else {
                    // Dealing with an unpartitioned table
                    if watermark.is_prunable() {
                        match watermark.entity {
                            PrunableTable::ObjectsHistory
                            | PrunableTable::Transactions
                            | PrunableTable::Events => {}
                            PrunableTable::EventEmitPackage
                            | PrunableTable::EventEmitModule
                            | PrunableTable::EventSenders
                            | PrunableTable::EventStructInstantiation
                            | PrunableTable::EventStructModule
                            | PrunableTable::EventStructName
                            | PrunableTable::EventStructPackage => {
                                self.store
                                    .prune_event_indices_table(
                                        watermark.pruner_lo(),
                                        watermark.reader_lo - 1,
                                    )
                                    .await?;
                            }
                            PrunableTable::TxAffectedAddresses
                            | PrunableTable::TxAffectedObjects
                            | PrunableTable::TxCallsPkg
                            | PrunableTable::TxCallsMod
                            | PrunableTable::TxCallsFun
                            | PrunableTable::TxChangedObjects
                            | PrunableTable::TxDigests
                            | PrunableTable::TxInputObjects
                            | PrunableTable::TxKinds
                            | PrunableTable::TxRecipients
                            | PrunableTable::TxSenders => {
                                self.store
                                    .prune_tx_indices_table(
                                        watermark.pruner_lo(),
                                        watermark.reader_lo - 1,
                                    )
                                    .await?;
                            }
                            PrunableTable::Checkpoints => {
                                self.store
                                    .prune_cp_tx_table(
                                        watermark.pruner_lo(),
                                        watermark.reader_lo - 1,
                                    )
                                    .await?;
                            }
                            PrunableTable::PrunerCpWatermark => {
                                self.store
                                    .prune_cp_tx_table(
                                        watermark.pruner_lo(),
                                        watermark.reader_lo - 1,
                                    )
                                    .await?;
                            }
                        }
                        self.store
                            .update_watermark_latest_pruned(
                                watermark.entity.clone(),
                                watermark.reader_lo - 1,
                            )
                            .await?;
                    }
                }
            }
        }
        info!("Pruner task cancelled.");
        Ok(())
    }
}

/// Task to periodically query the `watermarks` table and update the lower bounds for all watermarks
/// if the entry exceeds epoch-level retention policy.
async fn update_watermarks_lower_bounds_task(
    store: PgIndexerStore,
    retention_policies: HashMap<PrunableTable, u64>,
    cancel: CancellationToken,
) -> IndexerResult<()> {
    let mut interval = tokio::time::interval(Duration::from_secs(5));
    loop {
        tokio::select! {
            _ = cancel.cancelled() => {
                info!("Pruner watermark lower bound update task cancelled.");
                return Ok(());
            }
            _ = interval.tick() => {
                update_watermarks_lower_bounds(&store, &retention_policies, &cancel).await?;
            }
        }
    }
}

/// Fetches all entries from the `watermarks` table, and updates the lower bounds for all watermarks
/// if the entry's epoch range exceeds the respective retention policy.
async fn update_watermarks_lower_bounds(
    store: &PgIndexerStore,
    retention_policies: &HashMap<PrunableTable, u64>,
    cancel: &CancellationToken,
) -> IndexerResult<()> {
    let watermarks = store.get_watermarks().await?;
    let mut lower_bound_updates = vec![];

    for watermark in watermarks.iter() {
        if cancel.is_cancelled() {
            info!("Pruner watermark lower bound update task cancelled.");
            return Ok(());
        }

        let Some(epochs_to_keep) = retention_policies.get(&watermark.entity) else {
            continue;
        };

        if watermark.epoch_lo + epochs_to_keep <= watermark.epoch_hi {
            let new_inclusive_epoch_lower_bound =
                watermark.epoch_hi.saturating_sub(epochs_to_keep - 1);

            // TODO: (wlmyng) now that epochs table is not pruned, we can add `first_tx_seq_num` or
            // something and use it as a lookup table.
            let (min_cp, _) = store
                .get_checkpoint_range_for_epoch(new_inclusive_epoch_lower_bound)
                .await?;
            let (min_tx, _) = store.get_transaction_range_for_checkpoint(min_cp).await?;

            lower_bound_updates.push(StoredWatermark::from_lower_bound_update(
                watermark.entity.as_ref(),
                new_inclusive_epoch_lower_bound,
                watermark.entity.map_to_reader_unit(min_cp, min_tx),
            ))
        }
    }

    if !lower_bound_updates.is_empty() {
        store
            .update_watermarks_lower_bound(lower_bound_updates)
            .await?;
        info!("Finished updating lower bounds for watermarks");
    }

    Ok(())
}
