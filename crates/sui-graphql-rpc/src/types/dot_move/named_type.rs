use std::collections::HashMap;

use async_graphql::Context;
use futures::future;
use move_core_types::parser::parse_type_tag;
use sui_types::base_types::ObjectID;

use crate::error::Error;

use super::{
    config::{DotMoveServiceError, PLAIN_NAME_REG},
    named_move_package::NamedMovePackage,
};

pub(crate) struct NamedType;

impl NamedType {
    pub(crate) async fn query(
        ctx: &Context<'_>,
        name: &str,
        checkpoint_viewed_at: u64,
    ) -> Result<String, Error> {
        // we do not de-duplicate the names here, as the dataloader will do this for us.
        let names = Self::parse_type(name)?;

        // Gather all the requests to resolve the names.
        let names_to_resolve = names
            .iter()
            .map(|x| NamedMovePackage::query(ctx, x, checkpoint_viewed_at))
            .collect::<Vec<_>>();

        let mut results = future::try_join_all(names_to_resolve).await?;

        // now let's create a hashmap with {name: MovePackage}
        let mut name_package_id_mapping = HashMap::new();

        for name in names.into_iter().rev() {
            // safe unwrap: we know that the amount of results has to equal the amount of names.
            let Some(package) = results.pop().unwrap() else {
                return Err(Error::DotMove(DotMoveServiceError::NameNotFound(name)));
            };

            name_package_id_mapping.insert(name, package.move_package.native.id());
        }

        let correct_type_tag = Self::replace_names(name, &name_package_id_mapping);

        // now we query the names with futures to utilize data loader
        Ok(correct_type_tag)
    }

    // TODO: Should we introduce some overall string limit length here?
    // Is this already caught by the global limits?
    fn parse_type(name: &str) -> Result<Vec<String>, Error> {
        let mut names = vec![];

        // Regex checks for 1-63 characters (A-Z, a-z, 0-9, -) before and after the @ symbol.
        // If the name is invalid, it'll error our here (as it won't detect the proper name).
        // Versions are not accepted on type tags, so we do not check for `/v${version}` part of name.
        let struct_tag = PLAIN_NAME_REG.replace_all(name, |m: &regex::Captures| {
            names.push(m[0].to_string());
            "0x0".to_string()
        });

        // We attempt to parse the type_tag with these replacements, to make sure there are no other
        // errors in the type tag (apart from the move names). That protects us from unnecessary
        // queries to resolve .move names, for a type tag that will be invalid anyway.
        parse_type_tag(&struct_tag).map_err(|e| Error::Client(e.to_string()))?;

        Ok(names)
    }

    fn replace_names(type_name: &str, names: &HashMap<String, ObjectID>) -> String {
        let struct_tag_str = PLAIN_NAME_REG.replace_all(type_name, |m: &regex::Captures| {
            let addr = names.get(&m[0]).unwrap();
            addr.to_string()
        });

        struct_tag_str.to_string()
    }
}
