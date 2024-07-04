


const QUERY_FRAGMENT: &str = "fragment RECORD_VALUES on DynamicField {
    value {
      ... on MoveValue {
        bcs
      }
    }
}";

// This is used for resolving dot move names using an external API endpoint.
// This is not used on mainnet, only on testnet/devnet etc.
pub(crate) struct ExternalResolutionDotMoveService;

impl ExternalResolutionDotMoveService {

    async fn query_from_mainnet(config: &DotMoveConfig, name: String) -> Result<Option<AppRecord>, Error> {
        if config.mainnet_api_url.is_none() {
            return Err(Error::DotMove(DotMoveServiceError::MainnetApiUrlUnavailable));
        };

        Ok(None)
    }

    /// Constructs the GraphQL Query to query the names on a mainnet graphql endpoint.
    fn construct_get_names_query(config: &DotMoveConfig, names: Vec<String>) -> String {
        let mut result = format!(r#"{{ owner(address: "{}") {{"#, config.registry_id.unwrap());

        for name in names {
            let field_str = format!(
                r#"
                {}: dynamicField(
                name: {{type: "{}::name::Name", bcs: "{}"}}
        ) {{
          ...RECORD_VALUES
        }}"#,
                bcs, config.package_address
            );
            
        };

        result.push_str("}}");

        result
    }
}

