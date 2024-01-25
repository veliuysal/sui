// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use super::checkpoint::Checkpoint;
use super::cursor::{self, Page, RawPaginated, Target};
use super::{big_int::BigInt, move_type::MoveType, sui_address::SuiAddress};
use crate::data::{Db, DbConnection, QueryExecutor};
use crate::error::Error;
use crate::raw_query::RawQuery;
use crate::{filter, query};
use async_graphql::connection::{Connection, CursorType, Edge};
use async_graphql::*;
use diesel::{
    sql_types::{BigInt as SqlBigInt, Nullable, Text},
    OptionalExtension, QueryableByName,
};
use std::str::FromStr;
use sui_types::{parse_sui_type_tag, TypeTag};

/// The total balance for a particular coin type.
#[derive(Clone, Debug, SimpleObject)]
pub(crate) struct Balance {
    /// Coin type for the balance, such as 0x2::sui::SUI
    pub(crate) coin_type: MoveType,
    /// How many coins of this type constitute the balance
    pub(crate) coin_object_count: Option<u64>,
    /// Total balance across all coin objects of the coin type
    pub(crate) total_balance: Option<BigInt>,
}

/// Representation of a row of balance information from the DB. We read the balance as a `String` to
/// deal with the large (bigger than 2^63 - 1) balances.
#[derive(QueryableByName)]
pub struct StoredBalance {
    #[diesel(sql_type = Nullable<Text>)]
    pub balance: Option<String>,
    #[diesel(sql_type = Nullable<SqlBigInt>)]
    pub count: Option<i64>,
    #[diesel(sql_type = Text)]
    pub coin_type: String,
}

pub(crate) type Cursor = cursor::JsonCursor<String>;

impl Balance {
    /// Query for the balance of coins owned by `address`, of coins with type `coin_type`. Note that
    /// `coin_type` is the type of `0x2::coin::Coin`'s type parameter, not the full type of the coin
    /// object.
    pub(crate) async fn query(
        db: &Db,
        address: SuiAddress,
        coin_type: TypeTag,
        checkpoint_viewed_at: Option<u64>,
    ) -> Result<Option<Balance>, Error> {
        let stored: Option<StoredBalance> = db
            .execute_repeatable(move |conn| {
                let (lhs, mut rhs) = Checkpoint::available_range(conn)?;

                if let Some(checkpoint_viewed_at) = checkpoint_viewed_at {
                    if checkpoint_viewed_at < lhs || rhs < checkpoint_viewed_at {
                        return Ok(None);
                    }
                    rhs = checkpoint_viewed_at;
                }

                conn.result(move || {
                    balance_query(address, Some(coin_type.clone()), lhs as i64, rhs as i64)
                        .into_boxed()
                })
                .optional()
            })
            .await?;

        stored.map(Balance::try_from).transpose()
    }

    /// Query the database for a `page` of coin balances. Each balance represents the total balance
    /// for a particular coin type, owned by `address`.
    pub(crate) async fn paginate(
        db: &Db,
        page: Page<Cursor>,
        address: SuiAddress,
        checkpoint_viewed_at: Option<u64>,
    ) -> Result<Connection<String, Balance>, Error> {
        let response = db
            .execute_repeatable(move |conn| {
                let (lhs, mut rhs) = Checkpoint::available_range(conn)?;

                if let Some(checkpoint_viewed_at) = checkpoint_viewed_at {
                    if checkpoint_viewed_at < lhs || rhs < checkpoint_viewed_at {
                        return Ok(None);
                    }
                    rhs = checkpoint_viewed_at;
                }

                page.paginate_raw_query::<StoredBalance>(
                    conn,
                    balance_query(address, None, lhs as i64, rhs as i64),
                )
                .map(Some)
            })
            .await?;

        let mut conn = Connection::new(false, false);

        let Some((prev, next, results)) = response else {
            return Ok(conn);
        };

        conn.has_previous_page = prev;
        conn.has_next_page = next;

        for stored in results {
            let cursor = stored.cursor().encode_cursor();
            let balance = Balance::try_from(stored)?;
            conn.edges.push(Edge::new(cursor, balance));
        }

        Ok(conn)
    }
}

impl RawPaginated<Cursor> for StoredBalance {
    fn filter_ge(cursor: &Cursor, query: RawQuery) -> RawQuery {
        // Specify candidates to help disambiguate
        filter!(query, "coin_type >= {}", (**cursor).clone())
    }

    fn filter_le(cursor: &Cursor, query: RawQuery) -> RawQuery {
        // Specify candidates to help disambiguate
        filter!(query, "coin_type <= {}", (**cursor).clone())
    }

    fn order(asc: bool, query: RawQuery) -> RawQuery {
        if asc {
            return query.order_by("coin_type ASC");
        }
        query.order_by("coin_type DESC")
    }
}

impl Target<Cursor> for StoredBalance {
    fn cursor(&self) -> Cursor {
        Cursor::new(self.coin_type.clone())
    }
}

impl TryFrom<StoredBalance> for Balance {
    type Error = Error;

    fn try_from(s: StoredBalance) -> Result<Self, Error> {
        let StoredBalance {
            balance,
            count,
            coin_type,
        } = s;
        let total_balance = balance
            .map(|b| BigInt::from_str(&b))
            .transpose()
            .map_err(|_| Error::Internal("Failed to read balance.".to_string()))?;

        let coin_object_count = count.map(|c| c as u64);

        let coin_type = MoveType::new(
            parse_sui_type_tag(&coin_type)
                .map_err(|e| Error::Internal(format!("Failed to parse coin type: {e}")))?,
        );

        Ok(Balance {
            coin_type,
            coin_object_count,
            total_balance,
        })
    }
}

/// Query the database for a `page` of coin balances. Each balance represents the total balance for
/// a particular coin type, owned by `address`. This function is meant to be called within a thunk
/// and returns a RawQuery that can be converted into a BoxedSqlQuery with `.into_boxed()`.
fn balance_query(address: SuiAddress, coin_type: Option<TypeTag>, lhs: i64, rhs: i64) -> RawQuery {
    // Construct the filtered inner query - apply the same filtering criteria to both
    // objects_snapshot and objects_history tables.
    let mut snapshot_objs = query!("SELECT * FROM objects_snapshot");
    snapshot_objs = filter(snapshot_objs, address, coin_type.clone());

    // Additionally filter objects_history table for results between the available range, or
    // checkpoint_viewed_at, if provided.
    let mut history_objs = query!("SELECT * FROM objects_history");
    history_objs = filter(history_objs, address, coin_type.clone());
    history_objs = filter!(
        history_objs,
        format!(r#"checkpoint_sequence_number BETWEEN {} AND {}"#, lhs, rhs)
    );

    // Combine the two queries, and select the most recent version of each object.
    let candidates = query!(
        r#"SELECT DISTINCT ON (object_id) * FROM (({}) UNION ({})) o"#,
        snapshot_objs,
        history_objs
    )
    .order_by("object_id")
    .order_by("object_version DESC");

    // Objects that fulfill the filtering criteria may not be the most recent version available.
    // Left join the candidates table on newer to filter out any objects that have a newer
    // version.
    let mut newer = query!("SELECT object_id, object_version FROM objects_history");
    newer = filter!(
        newer,
        format!(r#"checkpoint_sequence_number BETWEEN {} AND {}"#, lhs, rhs)
    );
    let final_ = query!(
        r#"SELECT
            CAST(SUM(coin_balance) AS TEXT) as balance,
            COUNT(*) as count,
            coin_type
        FROM ({}) candidates
        LEFT JOIN ({}) newer
        ON (
            candidates.object_id = newer.object_id
            AND candidates.object_version < newer.object_version
        )"#,
        candidates,
        newer
    );

    // Additionally for balance's query, group coins by coin_type.
    filter!(final_, "newer.object_version IS NULL").group_by("coin_type")
}

/// Applies the filtering criteria for balances to the input `RawQuery` and returns a new
/// `RawQuery`.
fn filter(mut query: RawQuery, owner: SuiAddress, coin_type: Option<TypeTag>) -> RawQuery {
    query = filter!(query, "coin_type IS NOT NULL");

    query = filter!(
        query,
        format!("owner_id = '\\x{}'::bytea", hex::encode(owner.into_vec()))
    );

    if let Some(coin_type) = coin_type {
        query = filter!(
            query,
            "coin_type = {}",
            coin_type.to_canonical_display(/* with_prefix */ true)
        );
    };

    query
}
