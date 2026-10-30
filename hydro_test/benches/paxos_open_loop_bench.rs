use std::time::{Duration, Instant};

use clap::Parser;
use futures::StreamExt;
use hdrhistogram::Histogram;
use hydro_deploy::Deployment;
use hydro_lang::deploy::TrybuildHost;
use hydro_lang::prelude::*;
use hydro_std::bench_client::{
    aggregate_bench_results, compute_throughput_latency, open_loop_bench_client,
};
use hydro_test::cluster::paxos::{CorePaxos, PaxosConfig};
use hydro_test::cluster::paxos_bench::{Aggregator, kv_op_workload_generator, paxos_kv};
use stageleft::q;

#[derive(Parser, Debug)]
struct Args {
    // Split evenly across client machines
    #[arg(long, default_value_t = 10000.0)]
    rate: f64,

    #[arg(long, default_value_t = 1)]
    clients: usize,

    #[arg(long, default_value_t = 0)]
    read_percent: u64,

    #[arg(long, default_value_t = 4)]
    value_bytes: usize,

    #[arg(long, default_value_t = 0)]
    seed: u64,

    #[arg(long, default_value_t = 15)]
    duration_secs: u64,

    #[arg(long, default_value_t = 5)]
    warmup_secs: u64,

    #[arg(long, hide = true)]
    bench: bool,
}

/// Bounds the key space
const NUM_KEYS: u32 = 1024;

#[tokio::main]
async fn main() {
    let args = Args::parse();
    let mut deployment = Deployment::new();
    let localhost = deployment.Localhost();

    let mut builder = FlowBuilder::new();
    let external = builder.external::<()>();

    let f = 1;
    let checkpoint_frequency = 1000;
    let i_am_leader_send_timeout = 5;
    let i_am_leader_check_timeout = 10;
    let i_am_leader_check_timeout_delay_multiplier = 15;
    let print_result_frequency = 1000; // millis

    let proposers = builder.cluster();
    let acceptors = builder.cluster();
    let clients = builder.cluster();
    let client_aggregator = builder.process::<Aggregator>();
    let replicas = builder.cluster();

    let paxos = CorePaxos {
        proposers: proposers.clone(),
        acceptors: acceptors.clone(),
        paxos_config: PaxosConfig {
            f,
            i_am_leader_send_timeout,
            i_am_leader_check_timeout,
            i_am_leader_check_timeout_delay_multiplier,
        },
    };

    let latencies = open_loop_bench_client(
        &clients,
        q!(std::env::var("REQUESTS_PER_SEC_PER_NODE")
            .unwrap()
            .parse::<f64>()
            .unwrap()),
        q!(std::env::var("SEED").unwrap().parse::<u64>().unwrap()),
        |request_ids| {
            kv_op_workload_generator(request_ids, NUM_KEYS, args.read_percent, args.value_bytes)
        },
        |input| {
            paxos_kv(
                checkpoint_frequency,
                f,
                f + 1,
                paxos,
                &clients,
                &replicas,
                input,
            )
        },
    )
    .entries()
    .map(q!(|(_id, (_output, latency))| latency));

    let bench_results = compute_throughput_latency(
        &clients,
        latencies,
        print_result_frequency / 10,
        nondet!(/** bench */),
    );
    let aggregate_results =
        aggregate_bench_results(bench_results, &client_aggregator, print_result_frequency);
    let throughput_handle = aggregate_results
        .throughput
        .send_bincode_external(&external);
    let latency_handle = aggregate_results
        .latency_histogram
        .map(q!(|histogram| {
            hydro_std::bench_client::SerializableHistogramWrapper { histogram }
        }))
        .send_bincode_external(&external);

    let rustflags = "-C opt-level=3 -C codegen-units=1 -C strip=none -C debuginfo=2 -C lto=off";
    let host = || {
        TrybuildHost::new(localhost.clone())
            .rustflags(rustflags)
            .feature("tokio")
    };

    let nodes = builder
        .with_cluster(&proposers, (0..f + 1).map(|_| host()))
        .with_cluster(&acceptors, (0..2 * f + 1).map(|_| host()))
        .with_cluster(
            &clients,
            (0..args.clients).map(|_| {
                host()
                    .env(
                        "REQUESTS_PER_SEC_PER_NODE",
                        (args.rate / args.clients as f64).to_string(),
                    )
                    .env("SEED", args.seed.to_string())
            }),
        )
        .with_process(&client_aggregator, host())
        .with_cluster(&replicas, (0..f + 1).map(|_| host()))
        .with_external(&external, localhost.clone())
        .deploy(&mut deployment);

    deployment.deploy().await.unwrap();

    let mut throughput_recv = nodes.connect(throughput_handle).await;
    let mut latency_recv = nodes.connect(latency_handle).await;

    deployment.start().await.unwrap();

    let start = Instant::now();
    let measurement_start = start + Duration::from_secs(args.warmup_secs);
    let deadline = start + Duration::from_secs(args.duration_secs);
    let mut throughputs = Vec::new();
    let mut latencies = Histogram::<u64>::new(3).unwrap();

    loop {
        tokio::select! {
            () = tokio::time::sleep_until(deadline.into()) => break,
            Some(throughput) = throughput_recv.next() => {
                eprintln!("Throughput: {throughput} requests/s");
                if Instant::now() >= measurement_start {
                    throughputs.push(throughput as f64);
                }
            }
            Some(wrapper) = latency_recv.next() => {
                if Instant::now() >= measurement_start {
                    latencies.add(&*wrapper.histogram.borrow()).unwrap();
                }
            }
            else => break,
        }
    }

    if throughputs.is_empty() {
        eprintln!("No throughput samples received after warm-up");
        std::process::exit(1);
    }

    let mean = throughputs.iter().sum::<f64>() / throughputs.len() as f64;
    let variance =
        throughputs.iter().map(|t| (t - mean).powi(2)).sum::<f64>() / throughputs.len() as f64;
    let latency_us = |quantile| latencies.value_at_quantile(quantile) as f64 / 1000.0;

    println!(
        "{},{},{},{:.2},{:.2},{:.1},{:.1},{:.1}",
        args.rate,
        args.read_percent,
        args.value_bytes,
        mean,
        variance.sqrt(),
        latency_us(0.5),
        latency_us(0.99),
        latency_us(0.999),
    );
}
