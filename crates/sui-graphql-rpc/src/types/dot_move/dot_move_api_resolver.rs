use std::{collections::HashMap, str::FromStr};

use async_graphql::InputType;
use serde::{Deserialize, Serialize};

use super::dot_move_service::{AppRecord, DotMoveConfig, DotMoveServiceError, Name};
use crate::{error::Error, types::base64::Base64};

const QUERY_FRAGMENT: &str =
    "fragment RECORD_VALUES on DynamicField { value { ... on MoveValue { bcs } } }";

#[derive(Serialize)]
struct GraphQLRequest {
    query: String,
    variables: serde_json::Value,
}

#[derive(Deserialize, Debug)]
struct GraphQLResponse<T> {
    data: T,
}
#[derive(Deserialize, Debug)]
struct Owner {
    owner: FetchElements,
}

#[derive(Deserialize, Debug)]
struct FetchElements {
    #[serde(flatten)]
    fetch_elements: HashMap<String, OwnerValue>,
}

#[derive(Deserialize, Debug)]
struct OwnerValue {
    value: NameBCS,
}

#[derive(Deserialize, Debug)]
struct NameBCS {
    bcs: String,
}

// This is used for resolving dot move names using an external API endpoint.
// This is not used on mainnet, only on testnet/devnet etc.
pub(crate) struct DotMoveExternalResolution;

impl DotMoveExternalResolution {
    pub(crate) async fn query_from_mainnet(
        config: &DotMoveConfig,
        name: String,
    ) -> Result<Option<AppRecord>, Error> {
        if config.mainnet_api_url.is_none() {
            return Err(Error::DotMove(
                DotMoveServiceError::MainnetApiUrlUnavailable,
            ));
        };

        let client = reqwest::Client::new();

        let mut mapping: HashMap<String, usize> = HashMap::new();

        // Construct the name object
        let chain_name = Name {
            labels: name
                .split('@')
                .rev()
                .map(|x| x.to_string())
                .collect::<Vec<_>>(),
            normalized: name.to_string(),
        };

        let request_body = GraphQLRequest {
            query: Self::construct_names_graphql_query(config, vec![&chain_name], &mut mapping)?,
            variables: serde_json::Value::Null,
        };

        let res = client
            .post("https://sui-mainnet.mystenlabs.com/graphql")
            .json(&request_body)
            .send()
            .await
            .map_err(|_| Error::DotMove(DotMoveServiceError::FailedToReadMainnetResponse))?;

        // Check if the response status is success
        if res.status().is_success() {
            let response_json: GraphQLResponse<Owner> = res
                .json()
                .await
                .map_err(|e| Error::DotMove(DotMoveServiceError::FailedToReadMainnetResponse))?;

            let current_idx: &usize = mapping
                .get(&chain_name.normalized)
                .ok_or_else(|| Error::DotMove(DotMoveServiceError::FailedToParseMainnetResponse))?;

            let bcs = response_json
                .data
                .owner
                .fetch_elements
                .get(&format!("fetch_{}", current_idx))
                .ok_or_else(|| Error::DotMove(DotMoveServiceError::FailedToParseMainnetResponse))?;

            let bytes = Base64::from_str(&bcs.value.bcs)
                .map_err(|_| Error::DotMove(DotMoveServiceError::FailedToParseMainnetResponse))?;

            let app_record = bcs::from_bytes::<AppRecord>(&bytes.0)
                .map_err(|_| Error::DotMove(DotMoveServiceError::FailedToParseMainnetResponse));

            println!("{:#?}", app_record);
        } else {
            println!("GraphQL request failed: {}", res.status());
        }

        Ok(None)
    }



    /// Constructs the GraphQL Query to query the names on a mainnet graphql endpoint.
    pub(crate) fn construct_names_graphql_query(
        config: &DotMoveConfig,
        names: Vec<&Name>,
        mapping: &mut HashMap<String, usize>,
    ) -> Result<String, Error> {
        let mut result = format!(r#"{{ owner(address: "{}") {{"#, config.registry_id);

        for (index, name) in names.iter().enumerate() {
            let bcs_base64 = name.to_base64_string();

            print!("{:#?}", name);

            // retain the mapping here (id to bcs representation, so we can pick the right response later on)
            mapping.insert(name.normalized.clone(), index);

            let field_str = format!(
                r#"fetch_{}: dynamicField(name: {{ type: "{}::name::Name", bcs: {} }}) {{ ...RECORD_VALUES }}"#,
                index, config.package_address, bcs_base64
            );

            result.push_str(&field_str);
        }

        result.push_str("}} ");

        result.push_str(QUERY_FRAGMENT);

        println!("{}", result);

        Ok(result)
    }
}
