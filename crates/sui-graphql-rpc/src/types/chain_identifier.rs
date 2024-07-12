// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use std::sync::Arc;

use crate::{
    data::{Db, DbConnection, QueryExecutor},
    error::Error,
};
use async_graphql::*;
use diesel::{ExpressionMethods, OptionalExtension, QueryDsl};
use sui_indexer::schema::checkpoints;
use sui_protocol_config::Chain;
use sui_types::{
    digests::ChainIdentifier as NativeChainIdentifier, messages_checkpoint::CheckpointDigest,
};
use tokio::sync::RwLock;

pub(crate) struct ChainIdentifier;

impl ChainIdentifier {
    /// Query the Chain Identifier from the DB.
    pub(crate) async fn query(db: &Db) -> Result<Option<NativeChainIdentifier>, Error> {
        use checkpoints::dsl;

        let Some(digest_bytes) = db
            .execute(move |conn| {
                conn.first(move || {
                    dsl::checkpoints
                        .select(dsl::checkpoint_digest)
                        .order_by(dsl::sequence_number.asc())
                })
                .optional()
            })
            .await
            .map_err(|e| Error::Internal(format!("Failed to fetch genesis digest: {e}")))?
        else {
            return Ok(None);
        };

        let native_identifier = Self::from_bytes(digest_bytes)?;

        Ok(Some(native_identifier))
    }

    /// Treat `bytes` as a checkpoint digest and extract a chain identifier from it.
    pub(crate) fn from_bytes(bytes: Vec<u8>) -> Result<NativeChainIdentifier, Error> {
        let genesis_digest = CheckpointDigest::try_from(bytes)
            .map_err(|e| Error::Internal(format!("Failed to deserialize genesis digest: {e}")))?;
        Ok(NativeChainIdentifier::from(genesis_digest))
    }
}

#[derive(Clone, Default)]
pub(crate) struct ChainIdentifierLock(pub(crate) Arc<RwLock<ChainId>>);

#[derive(Clone, Copy, Debug, Default)]
pub(crate) struct ChainId {
    pub(crate) chain_identifier: NativeChainIdentifier,
    pub(crate) chain: Chain,
}

/// ChainId wraps `chain_identifier` and `chain` for quick access,
/// without having to re-calculate the "chain" every time.
impl ChainId {
    pub(crate) async fn new(lock: ChainIdentifierLock) -> Self {
        let w = lock.0.read().await;

        Self {
            chain_identifier: w.chain_identifier,
            chain: w.chain,
        }
    }

    pub(crate) fn chain(&self) -> &Chain {
        &self.chain
    }

    pub(crate) fn chain_identifier(&self) -> &NativeChainIdentifier {
        &self.chain_identifier
    }
}

impl From<NativeChainIdentifier> for ChainId {
    fn from(chain_identifier: NativeChainIdentifier) -> Self {
        let chain = chain_identifier.chain();
        Self {
            chain_identifier,
            chain,
        }
    }
}
