// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

mod execution;
pub use execution::EffectsFinality;
pub use execution::ExecuteTransaction;
pub use execution::ExecuteTransactionQueryParameters;
pub use execution::SimulateTransaction;
pub use execution::SimulateTransactionQueryParameters;
pub use execution::TransactionExecutionResponse;
pub use execution::TransactionSimulationResponse;

mod resolve;
pub use resolve::ResolveTransaction;
pub use resolve::ResolveTransactionQueryParameters;
pub use resolve::ResolveTransactionResponse;

use axum::extract::{Path, Query, State};
use axum::http::StatusCode;
use sui_sdk_types::types::CheckpointSequenceNumber;
use sui_sdk_types::types::Transaction;
use sui_sdk_types::types::{
    TransactionDigest, TransactionEffects, TransactionEvents, UserSignature,
};
use tap::Pipe;

use crate::accept::AcceptJsonProtobufBcs;
use crate::openapi::ApiEndpoint;
use crate::openapi::OperationBuilder;
use crate::openapi::ResponseBuilder;
use crate::openapi::RouteHandler;
use crate::proto;
use crate::proto::ListTransactionsResponse;
use crate::reader::StateReader;
use crate::response::JsonProtobufBcs;
use crate::Direction;
use crate::PageCursor;
use crate::RestError;
use crate::RestService;
use crate::Result;

pub struct GetTransaction;

impl ApiEndpoint<RestService> for GetTransaction {
    fn method(&self) -> axum::http::Method {
        axum::http::Method::GET
    }

    fn path(&self) -> &'static str {
        "/transactions/{transaction}"
    }

    fn operation(
        &self,
        generator: &mut schemars::gen::SchemaGenerator,
    ) -> openapiv3::v3_1::Operation {
        OperationBuilder::new()
            .tag("Transactions")
            .operation_id("GetTransaction")
            .path_parameter::<TransactionDigest>("transaction", generator)
            .response(
                200,
                ResponseBuilder::new()
                    .json_content::<TransactionResponse>(generator)
                    .protobuf_content()
                    .bcs_content()
                    .build(),
            )
            .response(404, ResponseBuilder::new().build())
            .build()
    }

    fn handler(&self) -> RouteHandler<RestService> {
        RouteHandler::new(self.method(), get_transaction)
    }
}

async fn get_transaction(
    Path(transaction_digest): Path<TransactionDigest>,
    accept: AcceptJsonProtobufBcs,
    State(state): State<StateReader>,
) -> Result<JsonProtobufBcs<TransactionResponse, proto::GetTransactionResponse, TransactionResponse>>
{
    let response = state.get_transaction_response(transaction_digest)?;

    match accept {
        AcceptJsonProtobufBcs::Json => JsonProtobufBcs::Json(response),
        AcceptJsonProtobufBcs::Protobuf => JsonProtobufBcs::Protobuf(response.try_into()?),
        AcceptJsonProtobufBcs::Bcs => JsonProtobufBcs::Bcs(response),
    }
    .pipe(Ok)
}

#[serde_with::serde_as]
#[derive(Debug, serde::Serialize, serde::Deserialize, schemars::JsonSchema)]
pub struct TransactionResponse {
    pub digest: TransactionDigest,
    pub transaction: Transaction,
    pub signatures: Vec<UserSignature>,
    pub effects: TransactionEffects,
    pub events: Option<TransactionEvents>,
    #[serde_as(
        as = "Option<sui_types::sui_serde::Readable<sui_types::sui_serde::BigInt<u64>, _>>"
    )]
    #[schemars(with = "Option<crate::_schemars::U64>")]
    pub checkpoint: Option<u64>,
    #[serde_as(
        as = "Option<sui_types::sui_serde::Readable<sui_types::sui_serde::BigInt<u64>, _>>"
    )]
    #[schemars(with = "Option<crate::_schemars::U64>")]
    pub timestamp_ms: Option<u64>,
}

#[derive(Debug)]
pub struct TransactionNotFoundError(pub TransactionDigest);

impl std::fmt::Display for TransactionNotFoundError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "Transaction {} not found", self.0)
    }
}

impl std::error::Error for TransactionNotFoundError {}

impl From<TransactionNotFoundError> for crate::RestError {
    fn from(value: TransactionNotFoundError) -> Self {
        Self::new(axum::http::StatusCode::NOT_FOUND, value.to_string())
    }
}

pub struct ListTransactions;

impl ApiEndpoint<RestService> for ListTransactions {
    fn method(&self) -> axum::http::Method {
        axum::http::Method::GET
    }

    fn path(&self) -> &'static str {
        "/transactions"
    }

    fn operation(
        &self,
        generator: &mut schemars::gen::SchemaGenerator,
    ) -> openapiv3::v3_1::Operation {
        OperationBuilder::new()
            .tag("Transactions")
            .operation_id("ListTransactions")
            .query_parameters::<ListTransactionsQueryParameters>(generator)
            .response(
                200,
                ResponseBuilder::new()
                    .json_content::<Vec<TransactionResponse>>(generator)
                    .protobuf_content()
                    .bcs_content()
                    .header::<String>(crate::types::X_SUI_CURSOR, generator)
                    .build(),
            )
            .response(410, ResponseBuilder::new().build())
            .build()
    }

    fn handler(&self) -> RouteHandler<RestService> {
        RouteHandler::new(self.method(), list_transactions)
    }
}

async fn list_transactions(
    Query(parameters): Query<ListTransactionsQueryParameters>,
    accept: AcceptJsonProtobufBcs,
    State(state): State<StateReader>,
) -> Result<(
    PageCursor<TransactionCursor>,
    JsonProtobufBcs<Vec<TransactionResponse>, ListTransactionsResponse, Vec<TransactionResponse>>,
)> {
    let latest_checkpoint = state.inner().get_latest_checkpoint()?.sequence_number;
    let oldest_checkpoint = state.inner().get_lowest_available_checkpoint()?;
    let limit = parameters.limit();
    let start = parameters.start(latest_checkpoint);
    let direction = parameters.direction();

    if start.checkpoint < oldest_checkpoint {
        return Err(RestError::new(
            StatusCode::GONE,
            "Old transactions have been pruned",
        ));
    }

    let mut next_cursor = None;
    let transactions = state
        .transaction_iter(direction, (start.checkpoint, start.index))
        .take(limit)
        .map(|entry| {
            let (cursor_info, digest) = entry?;
            next_cursor = cursor_info.next_cursor;
            state
                .get_transaction(digest.into())
                .map(|(transaction, effects, events)| TransactionResponse {
                    digest: transaction.transaction.digest(),
                    transaction: transaction.transaction,
                    signatures: transaction.signatures,
                    effects,
                    events,
                    checkpoint: Some(cursor_info.checkpoint),
                    timestamp_ms: Some(cursor_info.timestamp_ms),
                })
        })
        .collect::<Result<Vec<_>, _>>()?;

    let cursor = next_cursor.and_then(|(checkpoint, index)| {
        if checkpoint < oldest_checkpoint {
            None
        } else {
            Some(TransactionCursor { checkpoint, index })
        }
    });

    match accept {
        AcceptJsonProtobufBcs::Json => JsonProtobufBcs::Json(transactions),
        AcceptJsonProtobufBcs::Protobuf => {
            let proto = ListTransactionsResponse {
                transactions: transactions
                    .into_iter()
                    .map(TryInto::try_into)
                    .collect::<Result<_, _>>()?,
            };
            JsonProtobufBcs::Protobuf(proto)
        }
        AcceptJsonProtobufBcs::Bcs => JsonProtobufBcs::Bcs(transactions),
    }
    .pipe(|entries| (PageCursor(cursor), entries))
    .pipe(Ok)
}

/// A Cursor that points at a specific transaction in history.
///
/// Has the format of: `<checkpoint>[.<index>]`
/// where `<checkpoint>` is the sequence number of a checkpoint and `<index>` is the index of a
/// transaction in the particular checkpoint.
///
/// `index` is optional and if omitted iteration will start at the first or last transaction in a
/// checkpoint based on the provided `Direction`:
///   - Direction::Ascending - first
///   - Direction::Descending - last
#[derive(Debug, Copy, Clone)]
pub struct TransactionCursor {
    checkpoint: CheckpointSequenceNumber,
    index: Option<usize>,
}

impl std::fmt::Display for TransactionCursor {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.checkpoint)?;
        if let Some(index) = self.index {
            write!(f, ".{index}")?;
        }
        Ok(())
    }
}

impl std::str::FromStr for TransactionCursor {
    type Err = std::num::ParseIntError;

    fn from_str(s: &str) -> std::result::Result<Self, Self::Err> {
        if let Some((checkpoint, index)) = s.split_once('.') {
            Self {
                checkpoint: checkpoint.parse()?,
                index: Some(index.parse()?),
            }
        } else {
            Self {
                checkpoint: s.parse()?,
                index: None,
            }
        }
        .pipe(Ok)
    }
}

impl<'de> serde::Deserialize<'de> for TransactionCursor {
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        use serde_with::DeserializeAs;
        serde_with::DisplayFromStr::deserialize_as(deserializer)
    }
}

impl serde::Serialize for TransactionCursor {
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        use serde_with::SerializeAs;
        serde_with::DisplayFromStr::serialize_as(self, serializer)
    }
}

#[derive(Debug, Default, serde::Serialize, serde::Deserialize, schemars::JsonSchema)]
pub struct ListTransactionsQueryParameters {
    pub limit: Option<u32>,
    #[schemars(with = "Option<String>")]
    pub start: Option<TransactionCursor>,
    pub direction: Option<Direction>,
}

impl ListTransactionsQueryParameters {
    pub fn limit(&self) -> usize {
        self.limit
            .map(|l| (l as usize).clamp(1, crate::MAX_PAGE_SIZE))
            .unwrap_or(crate::DEFAULT_PAGE_SIZE)
    }

    pub fn start(&self, default: CheckpointSequenceNumber) -> TransactionCursor {
        self.start.unwrap_or(TransactionCursor {
            checkpoint: default,
            index: None,
        })
    }

    pub fn direction(&self) -> Direction {
        self.direction.unwrap_or(Direction::Descending)
    }
}
