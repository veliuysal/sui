// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use std::str::FromStr;

use async_graphql::ScalarType;
use move_core_types::{ident_str, identifier::IdentStr, language_storage::StructTag};
use once_cell::sync::Lazy;
use regex::Regex;
use serde::{Deserialize, Serialize};
use sui_types::{
    base_types::{ObjectID, SuiAddress},
    collection_types::VecMap,
    dynamic_field::Field,
    id::ID,
    object::MoveObject as NativeMoveObject,
};

use crate::types::base64::Base64;

const MAX_LABEL_LENGTH: usize = 63;

const VERSIONED_NAME_UNBOUND_REGEX: &str =
    r"([a-z0-9]+(?:-[a-z0-9]+)*)@([a-z0-9]+(?:-[a-z0-9]+)*)(?:\/v(\d+))?";

// Versioned name regex is much more strict, as it accepts a versioned OR unversioned name.
// This name has to be in the format `app@org/v1`.
// The version is optional, and if it is not present, we default to the latest version.
// The version is a number, and it has to be a positive integer.
// The name and org parts can only be lower case, numbers, and dashes, while dashes cannot be
// at the beginning or end of the name, nor can there be continuous dashes.
const VERSIONED_NAME_REGEX: &str =
    r"^([a-z0-9]+(?:-[a-z0-9]+)*)@([a-z0-9]+(?:-[a-z0-9]+)*)(?:\/v(\d+))?$";

// A regular expression that catches all possible dot move names in a type tag.
pub(crate) static VERSIONED_NAME_UNBOUND_REG: Lazy<Regex> =
    Lazy::new(|| Regex::new(VERSIONED_NAME_UNBOUND_REGEX).unwrap());

// A regular expression that catches a name in the format `app@org/v1`.
// This regex is used to parse the name and version from the type tag.
// This has a bound (needs to start and end with this format) compared to the unbound regex,
// which is used to parse all names from a type tag.
pub(crate) static VERSIONED_NAME_REG: Lazy<Regex> =
    Lazy::new(|| Regex::new(VERSIONED_NAME_REGEX).unwrap());

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

    #[error("Dot Move: The name {0} was not found.")]
    NameNotFound(String),

    #[error("Dot Move: Invalid version number: {0}")]
    InvalidVersion(String),
}

#[derive(Debug, Serialize, Deserialize, Clone, Eq, PartialEq)]
pub struct VersionedName {
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
    pub fn new(app_name: &str, org_name: &str) -> Self {
        let normalized = format!("{}@{}", app_name, org_name);
        let labels = vec![org_name.to_string(), app_name.to_string()];
        Self { labels, normalized }
    }

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

impl FromStr for VersionedName {
    type Err = DotMoveServiceError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        if let Some(caps) = VERSIONED_NAME_REG.captures(s) {
            let Some(app_name) = caps.get(1).map(|x| x.as_str()) else {
                return Err(DotMoveServiceError::InvalidName(s.to_string()));
            };
            let Some(org_name) = caps.get(2).map(|x| x.as_str()) else {
                return Err(DotMoveServiceError::InvalidName(s.to_string()));
            };

            if (org_name.len() > MAX_LABEL_LENGTH) || (app_name.len() > MAX_LABEL_LENGTH) {
                return Err(DotMoveServiceError::InvalidName(s.to_string()));
            };

            let version = if let Some(v) = caps.get(3).map(|x| x.as_str()) {
                Some(
                    v.parse::<u64>()
                        .map_err(|_| DotMoveServiceError::InvalidVersion(v.to_string()))?,
                )
            } else {
                None
            };

            Ok(Self {
                version,
                name: Name::new(app_name, org_name),
            })
        } else {
            Err(DotMoveServiceError::InvalidName(s.to_string()))
        }
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

// Config / constants of the service.

const DOT_MOVE_MODULE: &IdentStr = ident_str!("name");
const DOT_MOVE_TYPE: &IdentStr = ident_str!("Name");
const DOT_MOVE_PACKAGE: &str = "0x1a841abe817c38221596856bc975b3b84f2f68692191e9247e185213d3d02fd8";
const DOT_MOVE_REGISTRY: &str =
    "0x250b60446b8e7b8d9d7251600a7228dbfda84ccb4b23a56a700d833e221fae4f";
const DEFAULT_PAGE_LIMIT: u16 = 50;

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
pub(crate) enum ResolutionType {
    Internal,
    External,
}

#[derive(Clone, Debug, Serialize, Deserialize, Eq, PartialEq)]
#[serde(rename_all = "kebab-case")]
pub(crate) struct DotMoveConfig {
    pub(crate) mainnet_api_url: Option<String>,
    #[serde(default = "default_resolution_type")]
    pub(crate) resolution_type: ResolutionType,
    #[serde(default = "default_page_limit")]
    pub(crate) page_limit: u16,
    #[serde(default = "default_package_address")]
    pub(crate) package_address: SuiAddress,
    #[serde(default = "default_registry_id")]
    pub(crate) registry_id: ObjectID,
}

impl DotMoveConfig {
    pub(crate) fn new(
        resolution_type: ResolutionType,
        mainnet_api_url: Option<String>,
        page_limit: u16,
        package_address: SuiAddress,
        registry_id: ObjectID,
    ) -> Self {
        Self {
            resolution_type,
            mainnet_api_url,
            page_limit,
            package_address,
            registry_id,
        }
    }
}

fn default_resolution_type() -> ResolutionType {
    ResolutionType::Internal
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

// TODO: Keeping the values as is, because we'll remove the default getters
// when we refactor to use `[GraphqlConfig]` macro.
impl Default for DotMoveConfig {
    fn default() -> Self {
        Self::new(
            ResolutionType::Internal,
            None,
            DEFAULT_PAGE_LIMIT,
            SuiAddress::from_str(DOT_MOVE_PACKAGE).unwrap(),
            ObjectID::from_str(DOT_MOVE_REGISTRY).unwrap(),
        )
    }
}

#[cfg(test)]
mod tests {
    use super::VersionedName;
    use std::str::FromStr;

    #[test]
    fn parse_some_names() {
        let versioned = VersionedName::from_str("app@org/v1").unwrap();
        assert_eq!(versioned.name.normalized, "app@org");
        assert!(versioned.version.is_some_and(|x| x == 1));

        assert!(VersionedName::from_str("app@org/v34")
            .unwrap()
            .version
            .is_some_and(|x| x == 34));
        assert!(VersionedName::from_str("app@org")
            .unwrap()
            .version
            .is_none());

        let ok_names = vec!["1-app@org/v1", "1-app@org/v34", "1-app@org"];

        let composite_ok_names = vec![
            format!("{}@org/v1", generate_fixed_string(63)),
            format!("{}-app@org/v34", generate_fixed_string(59)),
            format!(
                "{}@{}",
                generate_fixed_string(63),
                generate_fixed_string(63)
            ),
            format!(
                "{}@{}-{}",
                generate_fixed_string(63),
                generate_fixed_string(30),
                generate_fixed_string(30)
            ),
        ];

        for name in ok_names {
            assert!(VersionedName::from_str(name).is_ok());
        }
        for name in composite_ok_names {
            assert!(VersionedName::from_str(&name).is_ok());
        }

        let not_ok_names = vec![
            "-app@org",
            "1.app@org",
            "1--app@org",
            "app-@org",
            "app--@org",
            "app@org/",
            "app@org/v",
            "app@org/veh",
            "@org",
            "app@/veh",
            "app",
            "@",
            "ap@@org",
            "ap!org",
            "ap#org",
            "ap#org@org",
            "app%org",
            "",
            " ",
        ];
        let composite_err_names = vec![
            format!(
                "{}--{}@{}",
                generate_fixed_string(10),
                generate_fixed_string(10),
                generate_fixed_string(63)
            ),
            format!(
                "--{}-{}@{}",
                generate_fixed_string(10),
                generate_fixed_string(10),
                generate_fixed_string(63)
            ),
            format!(
                "{}@{}--{}",
                generate_fixed_string(63),
                generate_fixed_string(30),
                generate_fixed_string(30)
            ),
        ];

        for name in not_ok_names {
            assert!(VersionedName::from_str(name).is_err());
        }
        for name in composite_err_names {
            assert!(VersionedName::from_str(&name).is_err());
        }
    }

    fn generate_fixed_string(len: usize) -> String {
        // Define the characters to use in the string
        let chars = "abcdefghijklmnopqrstuvwxyz0123456789";
        // Repeat the characters to ensure we have at least 63 characters
        let repeated_chars = chars.repeat(3);

        // Take the first 63 characters to form the string
        let fixed_string = &repeated_chars[0..len];

        fixed_string.to_string()
    }
}
