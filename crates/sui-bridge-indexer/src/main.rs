// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use anyhow::Result;
use clap::*;
use std::collections::HashSet;
use std::env;
use std::net::IpAddr;
use std::net::{Ipv4Addr, SocketAddr};
use std::path::PathBuf;
use std::sync::Arc;
use sui_bridge::eth_client::EthClient;
use sui_bridge::metered_eth_provider::MeteredEthHttpProvier;
use tokio::task::JoinHandle;
use tracing::info;

use mysten_metrics::metered_channel::channel;
use mysten_metrics::spawn_logged_monitored_task;
use mysten_metrics::start_prometheus_server;

use sui_bridge::metrics::BridgeMetrics;
use sui_bridge_indexer::config::IndexerConfig;
use sui_bridge_indexer::metrics::BridgeIndexerMetrics;
use sui_bridge_indexer::postgres_manager::{get_connection_pool, read_sui_progress_store};
use sui_bridge_indexer::sui_transaction_handler::handle_sui_transactions_loop;
use sui_bridge_indexer::sui_transaction_queries::start_sui_tx_polling_task;
use sui_bridge_indexer::{
    create_eth_subscription_indexer, create_eth_sync_indexer, create_sui_indexer,
};
use sui_config::Config;
use sui_data_ingestion_core::DataIngestionMetrics;
use sui_sdk::SuiClientBuilder;

#[derive(Parser, Clone, Debug)]
struct Args {
    /// Path to a yaml config
    #[clap(long, short)]
    config_path: Option<PathBuf>,
}

#[tokio::main]
async fn main() -> Result<()> {
    let _guard = telemetry_subscribers::TelemetryConfig::new()
        .with_env()
        .init();

    let args = Args::parse();

    // load config
    let config_path = if let Some(path) = args.config_path {
        path
    } else {
        env::current_dir()
            .expect("Couldn't get current directory")
            .join("config.yaml")
    };
    let config = IndexerConfig::load(&config_path)?;

    // Init metrics server
    let metrics_address =
        SocketAddr::new(IpAddr::V4(Ipv4Addr::new(0, 0, 0, 0)), config.metric_port);
    let registry_service = start_prometheus_server(metrics_address);
    let registry = registry_service.default_registry();
    mysten_metrics::init_metrics(&registry);
    info!("Metrics server started at port {}", config.metric_port);

    let indexer_meterics = BridgeIndexerMetrics::new(&registry);
    let ingestion_metrics = DataIngestionMetrics::new(&registry);
    let bridge_metrics = Arc::new(BridgeMetrics::new(&registry));

    let connection_pool = get_connection_pool(config.db_url.clone()).await;

    let eth_client = Arc::new(
        EthClient::<MeteredEthHttpProvier>::new(
            &config.eth_rpc_url,
            HashSet::from_iter(vec![]), // dummy
            bridge_metrics.clone(),
        )
        .await?,
    );

    let mut tasks = vec![];
    if Some(true) == config.disable_eth {
        info!("Eth indexer is disabled");
    } else {
        let eth_subscription_indexer = create_eth_subscription_indexer(
            connection_pool.clone(),
            indexer_meterics.clone(),
            &config,
            eth_client.clone(),
        )
        .await?;
        tasks.push(spawn_logged_monitored_task!(
            eth_subscription_indexer.start()
        ));

        let eth_sync_indexer = create_eth_sync_indexer(
            connection_pool.clone(),
            indexer_meterics.clone(),
            bridge_metrics,
            &config,
            eth_client.clone(),
        )
        .await?;
        tasks.push(spawn_logged_monitored_task!(eth_sync_indexer.start()));
    }

    let indexer = create_sui_indexer(
        connection_pool.clone(),
        indexer_meterics.clone(),
        ingestion_metrics.clone(),
        &config,
    )
    .await?;
    tasks.push(spawn_logged_monitored_task!(indexer.start()));

    // Wait for tasks in `tasks` to finish. Return when anyone of them returns an error.
    futures::future::try_join_all(tasks).await?;
    unreachable!("Indexer tasks finished unexpectedly");
}

#[allow(unused)]
async fn start_processing_sui_checkpoints_by_querying_txns(
    sui_rpc_url: String,
    db_url: String,
    indexer_metrics: BridgeIndexerMetrics,
) -> Result<Vec<JoinHandle<()>>> {
    let pg_pool = get_connection_pool(db_url.clone()).await;
    let (tx, rx) = channel(
        100,
        &mysten_metrics::get_metrics()
            .unwrap()
            .channel_inflight
            .with_label_values(&["sui_transaction_processing_queue"]),
    );
    let mut handles = vec![];
    let cursor = read_sui_progress_store(&pg_pool)
        .await
        .expect("Failed to read cursor from sui progress store");
    let sui_client = SuiClientBuilder::default().build(sui_rpc_url).await?;
    handles.push(spawn_logged_monitored_task!(
        start_sui_tx_polling_task(sui_client, cursor, tx),
        "start_sui_tx_polling_task"
    ));
    handles.push(spawn_logged_monitored_task!(
        handle_sui_transactions_loop(pg_pool.clone(), rx, indexer_metrics.clone()),
        "handle_sui_transcations_loop"
    ));
    Ok(handles)
}
