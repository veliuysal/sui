use std::str::FromStr;

use async_graphql::{Context, SimpleObject};

use crate::{
    error::Error,
    types::{
        address::Address, chain_identifier::ChainIdentifier, move_object::MoveObject,
        move_package::MovePackage, object::Object,
    },
};

use super::{
    data_loader::DotMoveDataLoader,
    dot_move_service::{
        AppInfo, AppRecord, DotMoveConfig, DotMoveServiceError, ResolutionType, VersionedName,
    },
};

#[derive(SimpleObject)]
pub(crate) struct NamedMovePackage {
    pub package_info_id: Address,
    pub move_package: MovePackage,
}

impl NamedMovePackage {
    pub(crate) async fn query(
        ctx: &Context<'_>,
        name: String,
        checkpoint_viewed_at: u64,
    ) -> Result<Option<Self>, Error> {
        let config: &DotMoveConfig = ctx.data_unchecked();
        let versioned = VersionedName::from_str(&name)?;

        // Non-mainnet handling for name resolution (uses mainnet api to resolve names).
        if config.resolution_type == ResolutionType::Internal {
            Self::query_external(ctx, config, versioned, checkpoint_viewed_at).await
        } else {
            Self::query_internal(ctx, config, versioned, checkpoint_viewed_at).await
        }
    }

    async fn query_external(
        ctx: &Context<'_>,
        config: &DotMoveConfig,
        versioned: VersionedName,
        checkpoint_viewed_at: u64,
    ) -> Result<Option<Self>, Error> {
        if config.mainnet_api_url.is_none() {
            return Err(DotMoveServiceError::MainnetApiUrlUnavailable.into());
        }

        let chain_id: ChainIdentifier = *ctx
            .data()
            .map_err(|_| DotMoveServiceError::ChainIdentifierUnavailable)?;

        let DotMoveDataLoader(loader) = &ctx.data_unchecked();

        let Some(result) = loader.load_one(versioned.name).await? else {
            return Ok(None);
        };

        let Some(app_info) = result.networks.get(&chain_id.0.to_string()) else {
            return Ok(None);
        };

        Self::package_from_app_info(
            ctx,
            app_info.clone(),
            versioned.version,
            checkpoint_viewed_at,
        )
        .await
    }

    async fn query_internal(
        ctx: &Context<'_>,
        config: &DotMoveConfig,
        versioned: VersionedName,
        checkpoint_viewed_at: u64,
    ) -> Result<Option<Self>, Error> {
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

        Self::package_from_app_info(ctx, app_info, versioned.version, checkpoint_viewed_at).await
    }

    async fn package_from_app_info(
        ctx: &Context<'_>,
        app_info: AppInfo,
        version: Option<u64>,
        checkpoint_viewed_at: u64,
    ) -> Result<Option<Self>, Error> {
        let Some(package_address) = app_info.package_address else {
            return Ok(None);
        };

        let Some(package_info_id) = app_info.package_info_id else {
            return Ok(None);
        };

        // let's now find the package at a specified version (or latest)
        let Some(package_at_version) = MovePackage::query(
            ctx,
            package_address.into(),
            version.map_or(MovePackage::latest_at(checkpoint_viewed_at), |v| {
                MovePackage::by_version(v, checkpoint_viewed_at)
            }),
        )
        .await?
        else {
            return Ok(None);
        };

        Ok(Some(NamedMovePackage {
            package_info_id: Address {
                address: package_info_id.bytes.into(),
                checkpoint_viewed_at,
            },
            move_package: package_at_version,
        }))
    }
}
