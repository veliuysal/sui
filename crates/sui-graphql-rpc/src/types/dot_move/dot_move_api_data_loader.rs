use std::{collections::HashMap, str::FromStr, sync::Arc};

use async_graphql::dataloader::{DataLoader, Loader};
use serde::{Deserialize, Serialize};

use crate::{error::Error, types::base64::Base64};

use super::dot_move_service::{AppRecord, DotMoveConfig, DotMoveServiceError, Name};

const QUERY_FRAGMENT: &str =
    "fragment RECORD_VALUES on DynamicField { value { ... on MoveValue { bcs } } }";

pub(crate) struct MainnetNamesLoader {
    client: reqwest::Client,
    config: DotMoveConfig,
}

impl MainnetNamesLoader {
    pub(crate) fn new(config: &DotMoveConfig) -> Self {
        Self {
            client: reqwest::Client::new(),
            config: config.clone(),
        }
    }

    /// Constructs the GraphQL Query to query the names on a mainnet graphql endpoint.
    pub(crate) fn construct_names_graphql_query(
        &self,
        names: &[Name],
        mapping: &mut HashMap<Name, usize>,
    ) -> Result<String, Error> {
        let mut result = format!(r#"{{ owner(address: "{}") {{"#, self.config.registry_id);

        // we create the GraphQL query keys with a `fetch_{id}` prefix, which is accepted on graphql fields
        // querying.
        for (index, name) in names.iter().enumerate() {
            let bcs_base64 = name.to_base64_string();

            print!("{:#?}", name);

            // retain the mapping here (id to bcs representation, so we can pick the right response later on)
            mapping.insert(name.clone(), index);

            let field_str = format!(
                r#"fetch_{}: dynamicField(name: {{ type: "{}::name::Name", bcs: {} }}) {{ ...RECORD_VALUES }}"#,
                index, self.config.package_address, bcs_base64
            );

            result.push_str(&field_str);
        }

        result.push_str("}} ");
        result.push_str(QUERY_FRAGMENT);

        println!("{}", result);

        Ok(result)
    }
}

impl Default for MainnetNamesLoader {
    fn default() -> Self {
        Self {
            client: reqwest::Client::new(),
            config: DotMoveConfig::default(),
        }
    }
}

#[async_trait::async_trait]
impl Loader<Name> for MainnetNamesLoader {
    type Value = AppRecord;
    type Error = Error;

    async fn load(&self, keys: &[Name]) -> Result<HashMap<Name, AppRecord>, Error> {
        let mut results: HashMap<Name, AppRecord> = HashMap::new();
        let mut mapping: HashMap<Name, usize> = HashMap::new();

        let request_body = GraphQLRequest {
            query: self.construct_names_graphql_query(keys, &mut mapping)?,
            variables: serde_json::Value::Null,
        };

        let res = self
            .client
            .post(self.config.mainnet_api_url.as_ref().unwrap())
            .json(&request_body)
            .send()
            .await
            .map_err(|_| Error::DotMove(DotMoveServiceError::FailedToReadMainnetResponse))?;

        // Check if the response status is success
        if res.status().is_success() {
            let response_json: GraphQLResponse<Owner> = res
                .json()
                .await
                .map_err(|_| Error::DotMove(DotMoveServiceError::FailedToReadMainnetResponse))?;

            let names = response_json.data.owner.names;

            mapping.keys().for_each(|k| {
                let idx = mapping.get(k).unwrap();

                let Some(bcs) = names.get(&format!("fetch_{}", idx)) else {
                    return;
                };

                let Some(bytes) = Base64::from_str(&bcs.value.bcs).ok() else {
                    return;
                };

                let Some(app_record) = bcs::from_bytes::<AppRecord>(&bytes.0).ok() else {
                    return;
                };

                results.insert(k.clone(), app_record);
            });
        } else {
            println!("GraphQL request failed: {}", res.status());
        }

        Ok(results)
    }
}

/// Helper types for accessing a shared `DataLoader` instance.
#[derive(Clone)]
pub(crate) struct DotMoveDataLoader(pub Arc<DataLoader<MainnetNamesLoader>>);

impl DotMoveDataLoader {
    pub(crate) fn new(config: &DotMoveConfig) -> Self {
        let data_loader = DataLoader::new(MainnetNamesLoader::new(config), tokio::spawn)
            .max_batch_size(config.page_limit as usize);
        Self(Arc::new(data_loader))
    }
}

// GraphQL Request and Response types
// for querying the names on the mainnet graphql endpoint.
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
    owner: Names,
}

#[derive(Deserialize, Debug)]
struct Names {
    #[serde(flatten)]
    names: HashMap<String, OwnerValue>,
}

#[derive(Deserialize, Debug)]
struct OwnerValue {
    value: NameBCS,
}

#[derive(Deserialize, Debug)]
struct NameBCS {
    bcs: String,
}
