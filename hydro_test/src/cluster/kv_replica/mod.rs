use std::collections::HashMap;
use std::fmt::Debug;
use std::hash::Hash;

use hydro_lang::live_collections::stream::NoOrder;
use hydro_lang::prelude::*;
use serde::de::DeserializeOwned;
use serde::{Deserialize, Serialize};

mod sequence_payloads;

pub struct Replica {}

pub trait KvKey: Serialize + DeserializeOwned + Hash + Eq + Clone + Debug {}
impl<K: Serialize + DeserializeOwned + Hash + Eq + Clone + Debug> KvKey for K {}

pub trait KvValue: Serialize + DeserializeOwned + Eq + Clone + Debug {}
impl<V: Serialize + DeserializeOwned + Eq + Clone + Debug> KvValue for V {}

#[derive(Serialize, Deserialize, PartialEq, Eq, Clone, Debug)]
pub struct KvPayload<K, V> {
    pub key: K,
    pub value: V,
}

#[derive(Serialize, Deserialize, PartialEq, Eq, Clone, Debug)]
pub struct SequencedKv<K, V> {
    // Note: Important that seq is the first member of the struct for sorting
    pub seq: usize,
    pub kv: Option<KvPayload<K, V>>,
}

impl<K: KvKey, V: KvValue> Ord for SequencedKv<K, V> {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        self.seq.cmp(&other.seq)
    }
}

impl<K: KvKey, V: KvValue> PartialOrd for SequencedKv<K, V> {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

#[derive(Serialize, Deserialize, PartialEq, Eq, Clone, Debug)]
pub enum KvOp<V> {
    Get,
    Put(V),
}

pub fn kv_replica<'a, K: KvKey, V: KvValue>(
    replicas: &Cluster<'a, Replica>,
    p_to_replicas: impl Into<
        Stream<(usize, Option<KvPayload<K, V>>), Cluster<'a, Replica>, Unbounded, NoOrder>,
    >,
    checkpoint_frequency: usize,
) -> (
    Stream<usize, Cluster<'a, Replica>, Unbounded>,
    Stream<KvPayload<K, V>, Cluster<'a, Replica>, Unbounded>,
) {
    let (checkpoints, payloads) = sequenced_replica(replicas, p_to_replicas, checkpoint_frequency);
    let applied_payloads = payloads.scan(
        q!(|| HashMap::new()),
        q!(|kv_store, payload| {
            kv_store.insert(payload.key.clone(), payload.value.clone());
            Some(payload)
        }),
    );
    (checkpoints, applied_payloads)
}

/// Like [`kv_replica`], but applies tagged [`KvOp`]s, allowing reading and writing. Only returns the tag
pub fn kv_op_replica<'a, K: KvKey, Tag: KvValue, V: KvValue>(
    replicas: &Cluster<'a, Replica>,
    p_to_replicas: impl Into<
        Stream<
            (usize, Option<KvPayload<K, (Tag, KvOp<V>)>>),
            Cluster<'a, Replica>,
            Unbounded,
            NoOrder,
        >,
    >,
    checkpoint_frequency: usize,
) -> (
    Stream<usize, Cluster<'a, Replica>, Unbounded>,
    Stream<(Tag, Option<V>), Cluster<'a, Replica>, Unbounded>,
) {
    let (checkpoints, payloads) = sequenced_replica(replicas, p_to_replicas, checkpoint_frequency);
    let values_read = payloads.scan(
        q!(|| HashMap::new()),
        q!(|kv_store, payload| {
            let (tag, op) = payload.value;
            let value_read = match op {
                KvOp::Get => kv_store.get(&payload.key).cloned(),
                KvOp::Put(value) => {
                    kv_store.insert(payload.key, value);
                    None
                }
            };
            Some((tag, value_read))
        }),
    );
    (checkpoints, values_read)
}


/// Orders the operations decided by Paxos so they can be applied. 
fn sequenced_replica<'a, K: KvKey, V: KvValue>(
    replicas: &Cluster<'a, Replica>,
    p_to_replicas: impl Into<
        Stream<(usize, Option<KvPayload<K, V>>), Cluster<'a, Replica>, Unbounded, NoOrder>,
    >,
    checkpoint_frequency: usize,
) -> (
    Stream<usize, Cluster<'a, Replica>, Unbounded>,
    Stream<KvPayload<K, V>, Cluster<'a, Replica>, Unbounded>,
) {
    let p_to_replicas = p_to_replicas
        .into()
        .map(q!(|(slot, kv)| SequencedKv { seq: slot, kv }));

    let replica_tick = replicas.tick();

    let (r_processable_payloads, r_next_slot_complete_cycle) =
        sequence_payloads::sequence_payloads(&replica_tick, p_to_replicas);

    let r_next_slot = r_processable_payloads.clone().across_ticks(|s| {
        s.fold(
            q!(|| 0),
            q!(|next_slot, payload| {
                *next_slot = payload.seq + 1;
            }),
        )
    });
    r_next_slot_complete_cycle.complete_next_tick(r_next_slot.clone());

    // Send checkpoints to the acceptors when we've processed enough payloads
    let (r_checkpointed_seqs_complete_cycle, r_checkpointed_seqs) =
        replica_tick.cycle::<Optional<usize, _, _>, _>();
    let r_max_checkpointed_seq = r_checkpointed_seqs
        .into_stream()
        .across_ticks(|s| s.max())
        .into_singleton();
    let r_checkpoint_seq_new = r_max_checkpointed_seq
        .zip(
            Optional::from(r_next_slot)
                .defer_tick()
                .unwrap_or(replica_tick.singleton(q!(0))),
        )
        .filter_map(q!(
            move |(max_checkpointed_seq, next_slot)| if max_checkpointed_seq
                .map(|m| next_slot - m >= checkpoint_frequency)
                .unwrap_or(true)
            {
                Some(next_slot)
            } else {
                None
            }
        ));
    r_checkpointed_seqs_complete_cycle.complete_next_tick(r_checkpoint_seq_new.clone());

    let r_payloads = r_processable_payloads
        .filter_map(q!(|payload| payload.kv))
        .all_ticks();
    (r_checkpoint_seq_new.all_ticks(), r_payloads)
}
