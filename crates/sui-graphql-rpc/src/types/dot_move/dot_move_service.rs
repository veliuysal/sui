// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use async_graphql::Context;
use move_core_types::ident_str;
use serde::{Deserialize, Serialize};
use sui_protocol_config::Chain;
use sui_types::{base_types::{ObjectID, SuiAddress}, collection_types::VecMap, id::ID};

use crate::{config::DotMoveConfig, error::Error};

use super::chain_identifier::{ChainId, ChainIdentifier};

const DOT_MOVE_MODULE: &IdentStr = ident_str!("name");
const DOT_MOVE_TYPE: &IdentStr = ident_str!("Name");

#[derive(thiserror::Error, Debug, Serialize, Deserialize, Clone, Eq, PartialEq)]
pub enum DotMoveServiceError {
    // The chain identifier is not available, so we cannot determine where to look for the name.
    #[error("Dot Move: Cannot determine which chain to query.")]
    ChainIdentifierUnavailable,
    // The name was not found in the DotMove service.
    #[error("Dot Move: The requested name {0} was not found.")]
    NameNotFound(String),
    // The name was found in the DotMove service, but it is not a valid name.
    #[error("Dot Move: The request name {0} is malformed.")]
    InvalidName(String),

    #[error("Dot Move: Mainnet API url is unavailable.")]
    MainnetApiUrlUnavailable,
}

pub(crate) struct DotMoveService;

impl DotMoveService {

    async fn is_mainnet(ctx: &Context<'_>) -> Result<bool, Error> {
        Ok(ChainIdentifier::get_chain_id(ctx.data_unchecked())
            .await
            .unwrap_or_default()
            .identifier()
            .chain()
            == Chain::Mainnet)
    }

    pub(crate) async fn query_package_by_name(
        ctx: &Context<'_>,
        name: String,
    ) -> Result<Option<String>, Error> {
        Ok(Some(name))
    }

    pub(crate) async fn type_by_name(
        ctx: &Context<'_>,
        name: String,
    ) -> Result<Option<bool>, Error> {

        let is_mainnet = Self::is_mainnet(ctx).await?;

        // Err(Error::DotMove(DotMoveServiceError::ChainIdentifierUnavailable))

        Ok(Some(is_mainnet))
    }
}


#[derive(Debug, Serialize, Deserialize, Clone, Eq, PartialEq)]
pub struct Name {
    labels: Vec<String>,
    normalized: String
}

impl Name {
    pub fn type_(package_address: SuiAddress) -> StructTag {
        StructTag {
            address: package_address.into(),
            module: DOT_MOVE_MODULE.to_owned(),
            name: DOT_MOVE_TYPE.to_owned(),
            type_params: vec![],
        }
    }

    pub fn to_dynamic_field_id(&self, config: &DotMoveConfig) -> ObjectID {
        let domain_type_tag = Self::type_(config.package_address);
        let domain_bytes = bcs::to_bytes(&self).unwrap();

        sui_types::dynamic_field::derive_dynamic_field_id(
            self.registry_id,
            &TypeTag::Struct(Box::new(domain_type_tag)),
            &domain_bytes,
        )
        .unwrap()
    }
}

/// An AppRecord entry in the DotMove service.
#[derive(Debug, Serialize, Deserialize, Clone, Eq, PartialEq)]
pub struct AppRecord {
    pub id: ObjectID,
    pub app_cap_id: ID,
    pub app_info: Option<AppInfo>,
    pub networks: VecMap<String, AppInfo>,
    pub metadata: VecMap<String, String>,
}

#[derive(Debug, Serialize, Deserialize, Clone, Eq, PartialEq)]
pub struct AppInfo {
    pub package_info_id: Option<ID>,
    pub package_address: Option<SuiAddress>,
    pub upgrade_cap_id: Option<ID>
}
