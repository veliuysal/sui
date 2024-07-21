// #[cfg(feature = "pg_integration")]
mod tests {
    use std::{path::PathBuf, time::Duration};

    use sui_graphql_rpc::{
        config::{ConnectionConfig, ServiceConfig},
        test_infra::cluster::{
            start_graphql_server_with_fn_rpc, wait_for_graphql_server,
        },
    };
    use sui_graphql_rpc_client::simple_client::SimpleClient;
    use sui_json_rpc_types::{ObjectChange, SuiTransactionBlockEffectsAPI};
    use sui_move_build::BuildConfig;
    use sui_types::{
        base_types::{ObjectID, SequenceNumber},
        digests::ObjectDigest,
        move_package::UpgradePolicy,
        object::Owner,
        programmable_transaction_builder::ProgrammableTransactionBuilder,
        transaction::{CallArg, ObjectArg},
        Identifier, SUI_FRAMEWORK_PACKAGE_ID,
    };
    use tokio::time::sleep;
    use tokio_util::sync::CancellationToken;

    const DOT_MOVE_PKG: &str = "tests/dot_move/dot_move/";
    const DEMO_PKG: &str = "tests/dot_move/demo/";
    const DEMO_PKG_V2: &str = "tests/dot_move/demo_v2/";
    const DEMO_TYPE: &str = "::demo::DemoType";

    struct Object(ObjectID, SequenceNumber, ObjectDigest);

    #[tokio::test]
    async fn test_dot_move_e2e() {
        let internal_res_cluster = sui_graphql_rpc::test_infra::cluster::start_cluster(
            ConnectionConfig::ci_integration_test_cfg(),
            None,
            None,
        )
        .await;

        internal_res_cluster
            .wait_for_checkpoint_catchup(1, Duration::from_secs(10))
            .await;
        let val =
            execute_query_short(&internal_res_cluster, r"{ chainIdentifier }".to_string()).await;

        // we'll use this chain_id to register the same package, just with external resolution.
        let external_network_chain_id =
            val["data"]["chainIdentifier"].as_str().unwrap().to_string();

        println!("External chain id: {:?}", external_network_chain_id);

        // // publish the dot move package in the internal resolution cluster.
        let (pkg_id, registry_id) = publish_dot_move_package(&internal_res_cluster).await;

        let demo_pkg_id_internal = publish_demo_pkg(&internal_res_cluster).await;

        let internal_client = init_dot_move_gql(
            internal_res_cluster
                .validator_fullnode_handle
                .rpc_url()
                .to_string(),
            8001,
            9185,
            ServiceConfig::dot_move_test_defaults(
                false,
                None,
                Some(pkg_id.into()),
                Some(registry_id.0),
            ),
        )
        .await;

        let external_client = init_dot_move_gql(
            internal_res_cluster
                .validator_fullnode_handle
                .rpc_url()
                .to_string(),
            8002,
            9186,
            ServiceConfig::dot_move_test_defaults(
                true,
                Some(internal_client.url()),
                Some(pkg_id.into()),
                Some(registry_id.0),
            ),
        )
        .await;

        // now we need to restart gql service using that setup.
        println!("{:?}", (pkg_id, registry_id));
        // // println!("{:?}", (demo_pkg_id_internal, demo_pkg_id_external));

        let name = "app@org".to_string();

        // // registers the package including the external chain one.
        register_pkg(
            &internal_res_cluster,
            pkg_id,
            registry_id,
            demo_pkg_id_internal.clone(),
            name.clone(),
            None,
        )
        .await;

        register_pkg(
            &internal_res_cluster,
            pkg_id,
            registry_id,
            demo_pkg_id_internal,
            name.clone(),
            Some(external_network_chain_id.clone()),
        )
        .await;

        // // Wait for the transactions to be committed and indexed
        sleep(Duration::from_secs(5)).await;

        // // // Query the package from the internal resolver first.
        // // println!("Validating dot move queries both internally and externally on version 1");

        let query = format!(
            r#"{{ valid_latest: {}, valid_with_version: {}, invalid: {}, type: {} }}"#,
            name_query(&name),
            name_query(&format!("{}{}", &name, "/v1")),
            name_query(&format!("{}{}", &name, "/v2")),
            type_query(&format!(
                "{}{}",
                &demo_pkg_id_internal.to_string(),
                DEMO_TYPE
            ))
        );

        let resolution = internal_client
            .execute(query.clone(), vec![])
            .await
            .unwrap();

        println!("{:?}", resolution);

        assert_eq!(
            resolution["data"]["valid_latest"]["address"]
                .as_str()
                .unwrap(),
            demo_pkg_id_internal.to_string()
        );

        assert_eq!(
            resolution["data"]["valid_with_version"]["address"]
                .as_str()
                .unwrap(),
            demo_pkg_id_internal.to_string()
        );

        assert_eq!(
            resolution["data"]["type"]["layout"]["struct"]["type"]
                .as_str()
                .unwrap(),
            format!("{}{}", demo_pkg_id_internal.to_string(), DEMO_TYPE)
        );

        // // v2 does not exist!
        assert!(resolution["data"]["invalid"].is_null());

        let resolution2 = external_client
            .execute(format!(r#"{{ {} }}"#, name_query(&name)), vec![])
            .await
            .unwrap();

        assert_eq!(
            resolution2["data"]["packageByName"]["address"]
                .as_str()
                .unwrap(),
            demo_pkg_id_internal.to_string()
        );

        println!("Tests are finished successfully now!");
    }

    async fn init_dot_move_gql(
        fullnode_rpc_url: String,
        gql_port: u16,
        prom_port: u16,
        config: ServiceConfig,
    ) -> SimpleClient {
        let secondary_cancellation_token = CancellationToken::new();

        let cfg = ConnectionConfig::ci_integration_test_cfg_with_db_name(
            "sui_indexer".to_string(),
            gql_port,
            prom_port,
        );

        let _secondary_gql_service = start_graphql_server_with_fn_rpc(
            cfg.clone(),
            Some(fullnode_rpc_url),
            Some(secondary_cancellation_token),
            Some(config),
        )
        .await;

        let server_url = format!("http://{}:{}/", cfg.host(), cfg.port());

        // Starts graphql client
        let client = SimpleClient::new(server_url);
        wait_for_graphql_server(&client).await;
        client
    }

    async fn execute_query_short(
        cluster: &sui_graphql_rpc::test_infra::cluster::Cluster,
        query: String,
    ) -> serde_json::Value {
        cluster.graphql_client.execute(query, vec![]).await.unwrap()
    }

    async fn register_pkg(
        cluster: &sui_graphql_rpc::test_infra::cluster::Cluster,
        dot_move_package_id: ObjectID,
        registry_id: (ObjectID, SequenceNumber),
        package_id: ObjectID,
        name: String,
        chain_id: Option<String>,
    ) {
        let is_network_call = chain_id.is_some();
        let function = if is_network_call {
            "set_network"
        } else {
            "add_record"
        };

        let mut args = vec![
            CallArg::Object(ObjectArg::SharedObject {
                id: registry_id.0,
                initial_shared_version: registry_id.1,
                mutable: true,
            }),
            CallArg::from(&name.as_bytes().to_vec()),
            CallArg::Pure(bcs::to_bytes(&package_id).unwrap()),
        ];

        if let Some(ref chain_id) = chain_id {
            args.push(CallArg::from(&chain_id.as_bytes().to_vec()));
        };

        let tx = cluster
            .validator_fullnode_handle
            .test_transaction_builder()
            .await
            .move_call(dot_move_package_id, "dotmove", function, args)
            .build();

        let sig = cluster
            .validator_fullnode_handle
            .wallet
            .sign_transaction(&tx);

        let executed = cluster
            .validator_fullnode_handle
            .execute_transaction(sig)
            .await;

        if executed.effects.unwrap().status().is_err() {
            panic!("Failed to add record: {:?}", (name, chain_id));
        };

        println!("Added record successfully: {:?}", (name, chain_id));
    }

    // publishes the DEMO_PKG on the given cluster and returns the package id.
    async fn publish_demo_pkg(cluster: &sui_graphql_rpc::test_infra::cluster::Cluster) -> ObjectID {
        let tx = cluster
            .validator_fullnode_handle
            .test_transaction_builder()
            .await
            .publish(PathBuf::from(DEMO_PKG))
            .build();

        let sig = cluster
            .validator_fullnode_handle
            .wallet
            .sign_transaction(&tx);

        let executed = cluster
            .validator_fullnode_handle
            .execute_transaction(sig)
            .await;

        let object_changes = executed.object_changes.unwrap();

        let v1_id = object_changes
            .iter()
            .find_map(|object| {
                if let ObjectChange::Published { package_id, .. } = object {
                    Some(*package_id)
                } else {
                    None
                }
            })
            .unwrap();

        let upgrade_cap = object_changes
            .iter()
            .find_map(|object| {
                if let ObjectChange::Created {
                    object_id,
                    object_type,
                    digest,
                    version,
                    ..
                } = object
                {
                    if object_type.module.as_str() == "package"
                        && object_type.name.as_str() == "UpgradeCap"
                    {
                        Some(Object(*object_id, *version, *digest))
                    } else {
                        None
                    }
                } else {
                    None
                }
            })
            .unwrap();

        let mut builder = ProgrammableTransactionBuilder::new();

        let compiled_package = BuildConfig::new_for_testing()
            .build(PathBuf::from(DEMO_PKG_V2))
            .unwrap();
        let digest = compiled_package.get_package_digest(false);
        let modules = compiled_package.get_package_bytes(false);
        let dependencies = compiled_package.get_dependency_original_package_ids();

        let cap = builder
            .obj(ObjectArg::ImmOrOwnedObject((
                upgrade_cap.0,
                upgrade_cap.1,
                upgrade_cap.2,
            )))
            .unwrap();

        let policy = builder.pure(UpgradePolicy::Compatible as u8).unwrap();

        let digest = builder.pure(digest.to_vec()).unwrap();

        let ticket = builder.programmable_move_call(
            SUI_FRAMEWORK_PACKAGE_ID,
            Identifier::new("package").unwrap(),
            Identifier::new("authorize_upgrade").unwrap(),
            vec![],
            vec![cap, policy, digest],
        );

        let receipt = builder.upgrade(v1_id, ticket, dependencies, modules);

        builder.programmable_move_call(
            SUI_FRAMEWORK_PACKAGE_ID,
            Identifier::new("package").unwrap(),
            Identifier::new("commit_upgrade").unwrap(),
            vec![],
            vec![cap, receipt],
        );

        v1_id
        // now let's also upgrade the same pkg.
    }

    // async fn upgrade_pkg(
    //     cluster: &sui_graphql_rpc::test_infra::cluster::Cluster,
    //     path: PathBuf,
    // ) {

    // }

    async fn publish_dot_move_package(
        cluster: &sui_graphql_rpc::test_infra::cluster::Cluster,
    ) -> (ObjectID, (ObjectID, SequenceNumber)) {
        let package_path = PathBuf::from(DOT_MOVE_PKG);
        let tx = cluster
            .validator_fullnode_handle
            .test_transaction_builder()
            .await
            .publish(package_path)
            .build();

        let sig = cluster
            .validator_fullnode_handle
            .wallet
            .sign_transaction(&tx);

        let executed = cluster
            .validator_fullnode_handle
            .execute_transaction(sig)
            .await;

        let (mut pkg_id, mut obj_id) = (None, None);

        for object in executed.object_changes.unwrap() {
            match object {
                ObjectChange::Published { package_id, .. } => {
                    pkg_id = Some(package_id);
                }
                ObjectChange::Created {
                    object_id,
                    object_type,
                    owner,
                    ..
                } => {
                    if object_type.module.as_str() == "dotmove"
                        && object_type.name.as_str() == "AppRegistry"
                    {
                        let initial_shared_version = match owner {
                            Owner::Shared {
                                initial_shared_version,
                            } => initial_shared_version,
                            _ => panic!("AppRegistry should be shared"),
                        };

                        if !owner.is_shared() {
                            panic!("AppRegistry should be shared");
                        };

                        obj_id = Some((object_id, initial_shared_version));
                    }
                }
                _ => {}
            }
        }

        (pkg_id.unwrap(), obj_id.unwrap())
    }

    fn name_query(name: &str) -> String {
        format!(r#"packageByName(name: "{}") {{ address, version }}"#, name)
    }

    fn type_query(named_type: &str) -> String {
        format!(r#"typeByName(name: "{}") {{ layout }}"#, named_type)
    }
}
