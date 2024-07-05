// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use std::str::FromStr;

use async_graphql::{Context, ScalarType};
use move_core_types::{ident_str, identifier::IdentStr, language_storage::StructTag};
use serde::{Deserialize, Serialize};
use sui_protocol_config::Chain;
use sui_types::{
    base_types::{ObjectID, SuiAddress},
    collection_types::VecMap,
    id::ID,
};

use crate::{
    error::Error,
    types::{base64::Base64, chain_identifier::ChainIdentifier},
};

use super::dot_move_api_data_loader::{DotMoveDataLoader, MainnetNamesLoader};

const DOT_MOVE_MODULE: &IdentStr = ident_str!("name");
const DOT_MOVE_TYPE: &IdentStr = ident_str!("Name");
const DOT_MOVE_PACKAGE: &str = "0x1a841abe817c38221596856bc975b3b84f2f68692191e9247e185213d3d02fd8";
const DOT_MOVE_REGISTRY: &str =
    "0x250b60446b8e7b8d9d7251600a7228dbfda84ccb4b23a56a700d833e221fae4f";
const DEFAULT_PAGE_LIMIT: u16 = 50;

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
#[serde(rename_all = "kebab-case")]
pub struct DotMoveConfig {
    pub(crate) mainnet_api_url: Option<String>,
    pub(crate) page_limit: u16,
    pub(crate) package_address: SuiAddress,
    pub(crate) registry_id: ObjectID,
}

impl DotMoveConfig {
    pub fn new(
        mainnet_api_url: Option<String>,
        page_limit: u16,
        package_address: SuiAddress,
        registry_id: ObjectID,
    ) -> Self {
        Self {
            mainnet_api_url,
            page_limit,
            package_address,
            registry_id,
        }
    }
}

impl Default for DotMoveConfig {
    fn default() -> Self {
        Self {
            mainnet_api_url: None,
            page_limit: DEFAULT_PAGE_LIMIT,
            package_address: SuiAddress::from_str(DOT_MOVE_PACKAGE).unwrap(),
            registry_id: ObjectID::from_str(DOT_MOVE_REGISTRY).unwrap(),
        }
    }
}

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

    #[error("Dot Move: Failed to read mainnet API response.")]
    FailedToReadMainnetResponse,

    #[error("Dot Move: Failed to parse mainnet API response.")]
    FailedToParseMainnetResponse,
}

pub(crate) struct DotMoveService;

impl DotMoveService {
    // Check if the active chain is mainnet.
    async fn is_mainnet(ctx: &Context<'_>) -> bool {
        ChainIdentifier::get_chain_id(ctx.data_unchecked())
            .await
            .unwrap_or_default()
            .identifier()
            .chain()
            == Chain::Mainnet
    }

    pub(crate) async fn query_package_by_name(
        ctx: &Context<'_>,
        name: String,
    ) -> Result<Option<AppInfo>, Error> {
        let DotMoveDataLoader(loader) = &ctx.data_unchecked();

        let chain_name = Name::from_str(&name)?;

        let Some(result) = loader.load_one(chain_name).await.ok() else {
            return Ok(None);
        };

        Ok(result.map_or(None, |x| x.app_info))
    }

    pub(crate) async fn type_by_name(
        ctx: &Context<'_>,
        name: String,
    ) -> Result<Option<bool>, Error> {
        let is_mainnet = Self::is_mainnet(ctx).await;

        Ok(Some(is_mainnet))
    }
}

#[derive(Debug, Serialize, Deserialize, Hash, Clone, Eq, PartialEq)]
pub struct Name {
    pub labels: Vec<String>,
    pub normalized: String,
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

    pub fn to_bytes(&self) -> Vec<u8> {
        bcs::to_bytes(&self).unwrap()
    }

    pub fn to_base64_string(&self) -> String {
        Base64::from(self.to_bytes()).to_value().to_string()
    }

    pub fn to_dynamic_field_id(&self, config: &DotMoveConfig) -> ObjectID {
        let domain_type_tag = Self::type_(config.package_address);

        sui_types::dynamic_field::derive_dynamic_field_id(
            config.registry_id,
            &sui_types::TypeTag::Struct(Box::new(domain_type_tag)),
            &self.to_bytes(),
        )
        .unwrap()
    }
}

impl FromStr for Name {
    type Err = DotMoveServiceError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let labels = s.split('@').rev().map(|x| x.to_string()).collect();
        // TODO: Add validation on labels etc.

        Ok(Self {
            labels,
            normalized: s.to_string(),
        })
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
    pub upgrade_cap_id: Option<ID>,
}
