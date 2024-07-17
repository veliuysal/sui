
// #[cfg(feature = "pg_integration")]
mod tests {
    use std::{io::Read, path::PathBuf, time::Duration};

    use serial_test::serial;
    use sui_graphql_rpc::config::{ConnectionConfig, ServiceConfig};
    use sui_json_rpc_types::ObjectChange;
    use sui_types::base_types::{ObjectID, SuiAddress};

    const DOT_MOVE_PKG: &str = "tests/dot_move/dot_move/";

    #[tokio::test]
    #[serial]
    async fn test_dot_move_e2e() {
        let cfg1 = ConnectionConfig::ci_integration_test_cfg();
        let cfg2 = ConnectionConfig::new(
            Some(8001),
            None,
            Some("postgres://postgres:postgrespw@localhost:5432/sui_indexer_2".to_string()),
            None,
            None,
            None,
        );

        let mut cluster_1 = sui_graphql_rpc::test_infra::cluster::start_cluster(
            cfg1,
            None,
            Some(
                ServiceConfig::dot_move_test_defaults(
                true,
                Some("http://localhost:8001".to_string()),
                None,
                None
            )),
        )
        .await;

        let cluster_2 = sui_graphql_rpc::test_infra::cluster::start_cluster(cfg2, None, None).await;

        cluster_1
            .wait_for_checkpoint_catchup(1, Duration::from_secs(10))
            .await;
        cluster_2
            .wait_for_checkpoint_catchup(1, Duration::from_secs(10))
            .await;

        let chain_id = cluster_1
            .graphql_client
            .execute(
                r"{
            chainIdentifier
        }"
                .to_string(),
                vec![],
            )
            .await
            .unwrap();

        let chain_id2 = cluster_2
            .graphql_client
            .execute(
                r"{
            chainIdentifier
        }"
                .to_string(),
                vec![],
            )
            .await
            .unwrap();

        let (pkg_id, registry_id) = publish_dot_move_package(&cluster_1).await;

        cluster_1.restart_graphql_service(Some(ServiceConfig::dot_move_test_defaults(
            false,
            Some("http://localhost:8001".to_string()),
            Some(pkg_id.into()),
            Some(registry_id),
        ))).await;

        let fresh_chain_id = cluster_1
            .graphql_client
            .execute(
                r"{
            chainIdentifier
        }"
                .to_string(),
                vec![],
            )
            .await
            .unwrap();


        // cluster_1.

        // now we need to restart gql service using that setup.
        println!("{:?}", (pkg_id, registry_id));

        println!("{:?}", chain_id);
        println!("{:?}", fresh_chain_id);
        println!("{:?}", chain_id2);
        assert!(true);
    }

    async fn publish_dot_move_package(cluster: &sui_graphql_rpc::test_infra::cluster::Cluster) -> (ObjectID, ObjectID) {
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

        let (mut pkg_id, mut  obj_id) = (None, None);

        for object in executed.object_changes.unwrap() {
            match object {
                ObjectChange::Published { package_id, .. } => {
                    pkg_id = Some(package_id);
                },
                ObjectChange::Created { object_id, object_type, .. } => {
                    if object_type.module.as_str() == "dotmove" && object_type.name.as_str() == "AppRegistry" {
                        obj_id = Some(object_id);
                    }
                },
                _ => {}
            }
        }

        (pkg_id.unwrap(), obj_id.unwrap())
    }
}
