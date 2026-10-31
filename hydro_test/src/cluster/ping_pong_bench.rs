use bytes::Bytes;
use hydro_lang::live_collections::stream::NoOrder;
use hydro_lang::prelude::*;
use hydro_std::quorum::collect_quorum;

pub struct Client;
pub struct Responder;
pub struct Aggregator;

pub fn ping_pong<'a>(
    clients: &Cluster<'a, Client>,
    responders: &Cluster<'a, Responder>,
    num_responders: usize,
    quorum: usize,
    reply_bytes: usize,
    pings: KeyedStream<u32, Bytes, Cluster<'a, Client>, Unbounded, NoOrder>,
) -> KeyedStream<u32, (), Cluster<'a, Client>, Unbounded, NoOrder> {
    assert!(
        (1..=num_responders).contains(&quorum),
        "Quorum must be between 1 and the number of responders"
    );

    let pongs = pings
        .entries()
        .broadcast(
            responders,
            TCP.fail_stop().bincode(),
            nondet!(/** responders are fixed at deployment */),
        )
        .entries()
        .map(q!(move |(client, (request_id, _ping))| (
            client,
            (
                request_id,
                std::iter::repeat_n(0u8, reply_bytes).collect::<Bytes>()
            )
        )))
        .into_keyed()
        .demux(clients, TCP.fail_stop().bincode())
        .values()
        .map(q!(|(request_id, _pong)| (request_id, Ok::<(), ()>(()))));

    collect_quorum(pongs, quorum, num_responders)
        .0
        .map(q!(|request_id| (request_id, ())))
        .into_keyed()
}

pub fn ping_workload_generator<'a, Client>(
    request_ids: KeyedStream<u32, (), Cluster<'a, Client>, Unbounded, NoOrder>,
    request_bytes: usize,
) -> KeyedStream<u32, Bytes, Cluster<'a, Client>, Unbounded, NoOrder> {
    request_ids.map(q!(
        move |()| std::iter::repeat_n(0u8, request_bytes).collect()
    ))
}
