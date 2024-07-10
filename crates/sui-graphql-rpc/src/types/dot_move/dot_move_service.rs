// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use std::str::FromStr;

use async_graphql::{Context, ScalarType};
use move_core_types::{ident_str, identifier::IdentStr, language_storage::StructTag};
use serde::{Deserialize, Serialize};
use sui_protocol_config::Chain;
use sui_types::{
    base_types::{ObjectID, SuiAddress}, collection_types::VecMap, dynamic_field::Field, id::ID, object::MoveObject as NativeMoveObject
};

use crate::{
    error::Error,
    types::{
        base64::Base64, chain_identifier::ChainIdentifier, move_object::MoveObject, move_package::{MovePackage, PackageLookup}, object::Object
    },
};

use super::dot_move_api_data_loader::DotMoveDataLoader;

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
    #[serde(default = "default_page_limit")]
    pub(crate) page_limit: u16,
    #[serde(default = "default_package_address")]
    pub(crate) package_address: SuiAddress,
    #[serde(default = "default_registry_id")]
    pub(crate) registry_id: ObjectID,
}

impl DotMoveConfig {
    pub(crate) fn new(
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

fn default_package_address() -> SuiAddress {
    SuiAddress::from_str(DOT_MOVE_PACKAGE).unwrap()
}

fn default_registry_id() -> ObjectID {
    ObjectID::from_str(DOT_MOVE_REGISTRY).unwrap()
}

fn default_page_limit() -> u16 {
    DEFAULT_PAGE_LIMIT
}

impl Default for DotMoveConfig {
    fn default() -> Self {
        Self::new(
            None,
            DEFAULT_PAGE_LIMIT,
            default_package_address(),
            default_registry_id(),
        )
    }
}

#[derive(thiserror::Error, Debug, Serialize, Deserialize, Clone, Eq, PartialEq)]
pub enum DotMoveServiceError {
    // The chain identifier is not available, so we cannot determine where to look for the name.
    #[error("Dot Move: Cannot determine which chain to query due to an internal error.")]
    ChainIdentifierUnavailable,
    // The name was found in the DotMove service, but it is not a valid name.
    #[error("Dot Move: The request name {0} is malformed.")]
    InvalidName(String),

    #[error("Dot Move: Mainnet API url is not available so resolution is not on this RPC.")]
    MainnetApiUrlUnavailable,

    #[error("Dot Move Internal Error: Failed to query mainnet API due to an internal error.")]
    FailedToQueryMainnetApi,

    #[error("Dot Move Internal Error: Failed to parse mainnet's API response.")]
    FailedToParseMainnetResponse,

    #[error("Dot Move Internal Error: Failed to deserialize DotMove record ${0}.")]
    FailedToDeserializeDotMoveRecord(ObjectID),
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
        checkpoint_viewed_at: u64,
    ) -> Result<Option<AppInfo>, Error> {
        let chain_id = ChainIdentifier::get_chain_id(ctx.data_unchecked())
            .await
            .unwrap_or_default();
            // .ok_or(DotMoveServiceError::ChainIdentifierUnavailable)?;

        // Non-mainnet handling for name resolution (uses mainnet api to resolve names).
        if chain_id.identifier().chain() != Chain::Mainnet {
            Self::query_package_by_name_non_mainnet(ctx, &name).await
        } else {
            Self::query_package_by_name_mainnet(ctx, &name, checkpoint_viewed_at).await?;
            Ok(None)
        }
    }

    pub(crate) async fn type_by_name(
        ctx: &Context<'_>,
        name: String,
    ) -> Result<Option<bool>, Error> {
        let is_mainnet = Self::is_mainnet(ctx).await;

        Ok(Some(is_mainnet))
    }

    async fn query_package_by_name_non_mainnet(
        ctx: &Context<'_>,
        name: &str,
    ) -> Result<Option<AppInfo>, Error> {
        let DotMoveDataLoader(loader) = &ctx.data_unchecked();

        let chain_name = Name::from_str(&name)?;
        let Some(result) = loader.load_one(chain_name).await? else {
            return Ok(None);
        };

        // TODO: Use this AppRecord to query the package...

        Ok(result.app_info)
    }

    async fn query_package_by_name_mainnet(
        ctx: &Context<'_>,
        name: &str,
        checkpoint_viewed_at: u64,
    ) -> Result<Option<AppInfo>, Error> {
        let config: &DotMoveConfig = ctx.data_unchecked();
        let versioned = VersionedName::from_str(&name)?;

        let Some(df) = MoveObject::query(
            ctx,
            versioned.name.to_dynamic_field_id(config).into(),
            Object::latest_at(checkpoint_viewed_at),
        )
        .await?
        else {
            return Ok(None);
        };

        let app_record = AppRecord::try_from(df.native)?;

        let Some(app_info) = app_record.app_info else {
            return Ok(None);
        };

        // TODO: Decide if this is an error or just an option return.
        let Some(package_address) = app_info.package_address else {
            return Ok(None);
        };

        // let's now find the package at a specified version (or latest)
        let Some(package_at_version) = MovePackage::query(
            ctx,
            package_address.into(),
            if versioned.version.is_some() {
                MovePackage::by_version(versioned.version.unwrap(), checkpoint_viewed_at)
            } else {
                MovePackage::latest_at(checkpoint_viewed_at)
            },
        ).await? else {
            return Ok(None);
        };

        let _id = package_at_version.native.id();

        Ok(None)
    }
}

#[derive(Debug, Serialize, Deserialize, Clone, Eq, PartialEq)]
pub(crate) struct VersionedName {
    /// A version name defaults at None, which means we need the latest version.
    pub version: Option<u64>,
    pub name: Name,
}

#[derive(Debug, Serialize, Deserialize, Hash, Clone, Eq, PartialEq)]
pub(crate) struct Name {
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

impl FromStr for VersionedName {
    type Err = DotMoveServiceError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        // TODO: Parse version
        Ok(Self {
            version: None,
            name: Name::from_str(s)?,
        })
    }
}

/// An AppRecord entry in the DotMove service.
#[derive(Debug, Serialize, Deserialize, Clone, Eq, PartialEq)]
pub(crate) struct AppRecord {
    pub id: ObjectID,
    pub app_cap_id: ID,
    pub app_info: Option<AppInfo>,
    pub networks: VecMap<String, AppInfo>,
    pub metadata: VecMap<String, String>,
}

#[derive(Debug, Serialize, Deserialize, Clone, Eq, PartialEq)]
pub(crate) struct AppInfo {
    pub package_info_id: Option<ID>,
    pub package_address: Option<SuiAddress>,
    pub upgrade_cap_id: Option<ID>,
}

impl TryFrom<NativeMoveObject> for AppRecord {
    type Error = DotMoveServiceError;

    fn try_from(object: NativeMoveObject) -> Result<Self, DotMoveServiceError> {
        object
            .to_rust::<Field<Name, Self>>()
            .map(|record| record.value)
            .ok_or_else(|| DotMoveServiceError::FailedToDeserializeDotMoveRecord(object.id()))
    }
}
