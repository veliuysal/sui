#[cfg(feature = "pg_integration")]
mod tests {
    use std::{path::PathBuf, time::Duration};

    use serial_test::serial;
    use sui_graphql_rpc::config::{ConnectionConfig, ServiceConfig};
    use sui_json_rpc_types::{ObjectChange, SuiTransactionBlockEffectsAPI};
    use sui_types::{
        base_types::{ObjectID, SequenceNumber},
        object::Owner,
        transaction::{CallArg, ObjectArg},
    };
    use tokio::time::sleep;

    const DOT_MOVE_PKG: &str = "tests/dot_move/dot_move/";
    const DEMO_PKG: &str = "tests/dot_move/demo/";

    #[tokio::test]
    #[serial]
    async fn test_dot_move_e2e() {
        let cfg2 = ConnectionConfig::new(
            Some(8001),
            None,
            Some("postgres://postgres:postgrespw@localhost:5432/sui_indexer_2".to_string()),
            None,
            None,
            None,
        );

        let mut internal_res_cluster = sui_graphql_rpc::test_infra::cluster::start_cluster(
            ConnectionConfig::ci_integration_test_cfg(),
            None,
            None,
        )
        .await;

        let mut external_res_cluster =
            sui_graphql_rpc::test_infra::cluster::start_cluster(cfg2, None, None).await;

        internal_res_cluster
            .wait_for_checkpoint_catchup(1, Duration::from_secs(10))
            .await;
        external_res_cluster
            .wait_for_checkpoint_catchup(1, Duration::from_secs(10))
            .await;

        let val =
            execute_query_short(&external_res_cluster, r"{ chainIdentifier }".to_string()).await;

        let external_network_chain_id =
            val["data"]["chainIdentifier"].as_str().unwrap().to_string();

        println!("External chain id: {:?}", external_network_chain_id);

        // publish the dot move package in the internal resolution cluster.
        let (pkg_id, registry_id) = publish_dot_move_package(&internal_res_cluster).await;

        let demo_pkg_id_internal = publish_demo_pkg(&internal_res_cluster).await;
        let demo_pkg_id_external = publish_demo_pkg(&external_res_cluster).await;

        // configure internal_res_cluster to be the "internal" resolution cluster.
        internal_res_cluster
            .restart_graphql_service(Some(ServiceConfig::dot_move_test_defaults(
                false,
                None,
                Some(pkg_id.into()),
                Some(registry_id.0),
            )))
            .await;

        // configure external_res_cluster
        external_res_cluster
            .restart_graphql_service(Some(ServiceConfig::dot_move_test_defaults(
                true,
                Some("http://localhost:8000".to_string()),
                Some(pkg_id.into()),
                Some(registry_id.0),
            )))
            .await;

        // now we need to restart gql service using that setup.
        println!("{:?}", (pkg_id, registry_id));
        println!("{:?}", (demo_pkg_id_internal, demo_pkg_id_external));

        let name = "app@org".to_string();

        // registers the package including the external chain one.
        register_pkg(
            &internal_res_cluster,
            pkg_id,
            registry_id,
            demo_pkg_id_internal,
            name.clone(),
            None,
        )
        .await;
        register_pkg(
            &internal_res_cluster,
            pkg_id,
            registry_id,
            demo_pkg_id_external,
            name.clone(),
            Some(external_network_chain_id.clone()),
        )
        .await;

        // Wait for the transactions to be committed and indexed
        sleep(Duration::from_secs(15)).await;

        // Query the package from the internal resolver first.
        println!("Validating dot move queries both internally and externally on version 1");

        let query = format!(
            r#"{{ valid_latest: {}, valid_with_version: {}, invalid: {} }}"#,
            name_query(&name),
            name_query(&format!("{}{}", &name, "/v1")),
            name_query(&format!("{}{}", &name, "/v2"))
        );
        let resolution = execute_query_short(&internal_res_cluster, query.clone()).await;

        // let resolution = execute_query_short(&internal_res_cluster, name_query(&name)).await;
        // let same_resolution = execute_query_short(
        //     &internal_res_cluster,
        //     name_query(&format!("{}{}", &name, "/v1")),
        // )
        // .await;
        // let invalid_version_resolution = execute_query_short(
        //     &internal_res_cluster,
        //     name_query(&format!("{}{}", &name, "/v2")),
        // )
        // .await;

        println!("{:?}", query.clone());
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

        // v2 does not exist!
        assert!(resolution["data"]["invalid"].is_null());

        let resolution2 = execute_query_short(&external_res_cluster, format!(r#"{{ {} }}"#, name_query(&name))).await;

        assert_eq!(
            resolution2["data"]["packageByName"]["address"]
                .as_str()
                .unwrap(),
            demo_pkg_id_external.to_string()
        );

        internal_res_cluster.graphql_cancellation_token.cancel();
        external_res_cluster.graphql_cancellation_token.cancel();
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
        let package_path = PathBuf::from(DEMO_PKG);
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

        executed
            .object_changes
            .unwrap()
            .iter()
            .find_map(|object| {
                if let ObjectChange::Published { package_id, .. } = object {
                    Some(*package_id)
                } else {
                    None
                }
            })
            .unwrap()
    }

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
        format!(
            r#"packageByName(name: "{}") {{ address, version }}"#,
            name
        )
    }

    fn type_query(named_type: &str) -> String {
        format!(
            r#"typeByName(name: "{}") {{ layout }}"#,
            named_type
        )
    }
}
