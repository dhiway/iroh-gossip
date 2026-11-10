//! Native lifecycle regressions. Every constructed Gossip owns its actual actor
//! task; timeouts bound failures, while barriers and joins prove completion.
use super::*;

use std::{
    future::{poll_fn, Future},
    sync::{
        atomic::{AtomicBool, Ordering},
        RwLock,
    },
    time::Duration,
};

use crate::api::GossipReceiver;
use iroh::{
    address_lookup::memory::MemoryLookup, endpoint::presets, protocol::Router, RelayMode, SecretKey,
};
use n0_future::TryStreamExt;

const LIMIT: Duration = Duration::from_secs(20);
const PROBE_ALPN: &[u8] = b"/gossip-shutdown-test/probe";

type ActorParts = (
    Actor,
    mpsc::Sender<RpcMessage>,
    mpsc::Sender<LocalActorMessage>,
);

async fn endpoint() -> Endpoint {
    Endpoint::builder(presets::Minimal)
        .relay_mode(RelayMode::Disabled)
        .alpns(vec![GOSSIP_ALPN.to_vec(), PROBE_ALPN.to_vec()])
        .bind()
        .await
        .unwrap()
}

fn actor(endpoint: &Endpoint) -> ActorParts {
    Actor::new(
        endpoint.clone(),
        proto::Config::default(),
        Arc::new(Metrics::default()),
        None,
        GossipAddressLookup::default(),
    )
}

fn gossip_with_task(
    parts: ActorParts,
    before: Option<oneshot::Receiver<()>>,
    after: Option<(oneshot::Sender<()>, oneshot::Receiver<()>)>,
) -> Gossip {
    let (actor, rpc_tx, local_tx) = parts;
    let stop = actor.stop.clone();
    let metrics = actor.metrics.clone();
    let max_message_size = actor.state.max_message_size();
    let task = task::spawn(async move {
        if let Some(before) = before {
            before.await.unwrap();
        }
        let result = actor.run().await;
        if let Some((ended, release)) = after {
            ended.send(()).unwrap();
            release.await.unwrap();
        }
        result
    });
    Gossip {
        inner: Arc::new(Inner {
            api: GossipApi::local(rpc_tx),
            local_tx,
            stop,
            completion: Mutex::new(ActorCompletion {
                handle: Some(AbortOnDropHandle::new(task)),
                result: None,
            }),
            max_message_size,
            metrics,
        }),
    }
}

async fn assert_pending<F: Future>(mut future: Pin<&mut F>) {
    assert!(poll_fn(|cx| Poll::Ready(future.as_mut().poll(cx)))
        .await
        .is_pending());
}

struct DropWitness(Arc<AtomicBool>);
impl Drop for DropWitness {
    fn drop(&mut self) {
        self.0.store(true, Ordering::SeqCst);
    }
}

async fn pair(a: &Endpoint, b: &Endpoint, alpn: &[u8]) -> (Connection, Connection) {
    let (outgoing, incoming) = tokio::join!(a.connect(b.addr(), alpn), async {
        b.accept().await.unwrap().accept().unwrap().await.unwrap()
    });
    (outgoing.unwrap(), incoming)
}

fn join_message(from: EndpointId, to: EndpointId, topic: TopicId) -> ProtoMessage {
    let mut state = proto::State::new(
        from,
        PeerData::default(),
        proto::Config::default(),
        StdRng::seed_from_u64(46),
    );
    let message = state
        .handle(
            InEvent::Command(topic, ProtoCommand::Join(vec![to])),
            Instant::now(),
            None,
        )
        .find_map(|event| match event {
            OutEvent::SendMessage(peer, message) if peer == to => Some(message),
            _ => None,
        })
        .expect("native join must send a message");
    message
}

async fn received(receiver: &mut GossipReceiver, expected: &[u8]) {
    loop {
        match TryStreamExt::try_next(receiver).await.unwrap() {
            Some(Event::Received(message)) => {
                assert_eq!(message.content.as_ref(), expected);
                return;
            }
            Some(_) => {}
            None => panic!("gossip receiver closed before the expected message"),
        }
    }
}

#[tokio::test]
async fn public_shutdown_retains_actual_actor_join_across_dropped_and_concurrent_waiters() {
    tokio::time::timeout(LIMIT, async {
        let endpoint = endpoint().await;
        let (ended_tx, ended_rx) = oneshot::channel();
        let (release_tx, release_rx) = oneshot::channel();
        let gossip = gossip_with_task(actor(&endpoint), None, Some((ended_tx, release_rx)));
        let retained = gossip.clone();
        let mut first = Box::pin(gossip.shutdown());
        assert_pending(first.as_mut()).await;
        ended_rx.await.unwrap();
        drop(first);
        {
            let completion = gossip.inner.completion.lock().await;
            assert!(completion.result.is_none());
            assert!(completion.handle.is_some());
        }
        let mut second = Box::pin(gossip.shutdown());
        let mut third = Box::pin(retained.shutdown());
        assert_pending(second.as_mut()).await;
        assert_pending(third.as_mut()).await;
        release_tx.send(()).unwrap();
        let (second, third) = tokio::join!(second, third);
        second.unwrap();
        third.unwrap();
        let completion = gossip.inner.completion.lock().await;
        assert_eq!(completion.result, Some(Ok(())));
        assert!(completion.handle.is_none());
        drop(completion);
        gossip.shutdown().await.unwrap();
        endpoint.close().await;
    })
    .await
    .expect("actor join custody timed out");
}

#[tokio::test]
async fn shutdown_interrupts_full_active_sender_and_discards_queued_old_commands() {
    tokio::time::timeout(LIMIT, async {
        let endpoint = endpoint().await;
        let (mut actor, rpc, local) = actor(&endpoint);
        let peer = SecretKey::from_bytes(&[47; 32]).public();
        let topic = TopicId::from([46; 32]);
        let (send, mut held_receiver) = mpsc::channel(1);
        send.send(join_message(endpoint.id(), peer, topic))
            .await
            .unwrap();
        actor.peers.insert(
            peer,
            PeerState::Active {
                active_send_tx: send,
                active_conn_id: 46,
                other_conns: vec![],
            },
        );
        let (input, input_rx) = mpsc::channel(1);
        actor.in_event_tx = input.clone();
        actor.in_event_rx = input_rx;
        input
            .send(InEvent::Command(topic, ProtoCommand::Join(vec![peer])))
            .await
            .unwrap();
        let gossip = gossip_with_task((actor, rpc, local), None, None);
        // Capacity is returned only after the real actor consumes this command.
        // Its first native output then blocks on the full active sender above.
        drop(input.reserve().await.unwrap());
        let old_api = gossip.inner.api.clone();
        let (old_sender, _old_receiver) =
            gossip.subscribe(topic, vec![peer]).await.unwrap().split();
        old_sender.join_peers(vec![peer]).await.unwrap();
        old_sender
            .broadcast(Bytes::from_static(b"must not run"))
            .await
            .unwrap();
        gossip.shutdown().await.unwrap();
        assert!(
            held_receiver.recv().await.is_some(),
            "held queue was never drained by a connection"
        );
        assert!(
            held_receiver.recv().await.is_none(),
            "interrupted native output was not enqueued"
        );
        assert!(old_sender
            .broadcast(Bytes::from_static(b"stale"))
            .await
            .is_err());
        assert!(old_api.subscribe(topic, vec![peer]).await.is_err());
        endpoint.close().await;
    })
    .await
    .expect("full actor sender prevented shutdown");
}

#[tokio::test]
async fn pending_native_dial_is_destroyed_and_completed_unreaped_dial_connection_is_closed() {
    tokio::time::timeout(LIMIT, async {
        let a = endpoint().await;
        let b = endpoint().await;
        let c = endpoint().await;
        let (mut actor, rpc, local) = actor(&a);
        let canceled = Arc::new(AtomicBool::new(false));
        let witness = DropWitness(canceled.clone());
        let pending_id = b.id();
        let cancel = CancellationToken::new();
        actor
            .dialer
            .pending_dials
            .insert(pending_id, cancel.clone());
        let dial_endpoint = a.clone();
        let address = b.addr();
        actor.dialer.pending.spawn(async move {
            let _witness = witness;
            let outcome = tokio::select! {
                biased;
                _ = cancel.cancelled() => None,
                connection = dial_endpoint.connect(address, GOSSIP_ALPN) => Some(connection),
            };
            (pending_id, outcome)
        });
        // A real incoming handshake is held before server acceptance; this is
        // an actual native dial, not a synthetic pending task.
        let held_incoming = b.accept().await.unwrap();
        assert!(!canceled.load(Ordering::SeqCst));
        let (completed, remote) = pair(&a, &c, GOSSIP_ALPN).await;
        let retained = completed.clone();
        let done_id = c.id();
        actor
            .dialer
            .pending_dials
            .insert(done_id, CancellationToken::new());
        let completed_task = actor
            .dialer
            .pending
            .spawn(async move { (done_id, Some(Ok(completed))) });
        while !completed_task.is_finished() {
            tokio::task::yield_now().await;
        }
        // Stop before run so neither completed result can be reaped by the
        // normal dial branch; teardown owns the exact native result.
        actor.stop.cancel();
        let gossip = gossip_with_task((actor, rpc, local), None, None);
        gossip.shutdown().await.unwrap();
        assert!(
            canceled.load(Ordering::SeqCst),
            "dial future destruction was not observed"
        );
        assert!(
            retained.close_reason().is_some(),
            "unreaped successful dial connection remained live"
        );
        remote.closed().await;
        drop(held_incoming);
        a.close().await;
        b.close().await;
        c.close().await;
    })
    .await
    .expect("native dial retirement timed out");
}

#[tokio::test]
async fn suspended_full_queue_handoff_remains_caller_owned_until_explicit_drop() {
    tokio::time::timeout(LIMIT, async {
        let a = endpoint().await;
        let b = endpoint().await;
        let (queued, queued_remote) = pair(&b, &a, GOSSIP_ALPN).await;
        let (pending, pending_remote) = pair(&b, &a, GOSSIP_ALPN).await;
        let retained = pending.clone();
        let (start_tx, start_rx) = oneshot::channel();
        let gossip = gossip_with_task(actor(&a), Some(start_rx), None);
        let capacity = gossip.inner.local_tx.capacity();
        for _ in 0..capacity {
            gossip.handle_connection(queued.clone()).await.unwrap();
        }
        assert_eq!(gossip.inner.local_tx.capacity(), 0);
        let mut handoff = Box::pin(gossip.handle_connection(pending));
        assert_pending(handoff.as_mut()).await;
        let mut shutdown = Box::pin(gossip.shutdown());
        assert_pending(shutdown.as_mut()).await;
        start_tx.send(()).unwrap();
        shutdown.await.unwrap();
        assert!(
            queued.close_reason().is_some(),
            "queued actor-owned connection was not closed"
        );
        assert!(
            retained.close_reason().is_none(),
            "an unpolled caller handoff is outside actor custody"
        );
        // This is the required host-owner step, not an implicit standalone
        // Gossip shutdown guarantee for a suspended caller-owned future.
        drop(handoff);
        assert!(retained.close_reason().is_some());
        queued_remote.closed().await;
        pending_remote.closed().await;
        let (after, after_remote) = pair(&b, &a, GOSSIP_ALPN).await;
        let after_clone = after.clone();
        assert!(gossip.handle_connection(after).await.is_err());
        assert!(after_clone.close_reason().is_some());
        after_remote.closed().await;
        a.close().await;
        b.close().await;
    })
    .await
    .expect("caller handoff retirement timed out");
}

#[tokio::test]
async fn shutdown_destroys_a_topic_forwarder_held_in_real_bounded_event_send() {
    tokio::time::timeout(LIMIT, async {
        let endpoint = endpoint().await;
        let (mut actor, rpc, local) = actor(&endpoint);
        let topic = TopicId::from([48; 32]);
        let (events, event_rx) = broadcast::channel(1);
        let (sender, mut held_receiver) = irpc::channel::mpsc::channel(1);
        sender.send(Event::Lagged).await.unwrap();
        let ended = Arc::new(AtomicBool::new(false));
        let witness = DropWitness(ended.clone());
        let (entered_tx, entered_rx) = oneshot::channel();
        events.send(ProtoEvent::NeighborUp(endpoint.id())).unwrap();
        actor.topic_event_forwarders.spawn(async move {
            let _witness = witness;
            let mut forwarding = Box::pin(topic_subscriber_loop(sender, event_rx));
            assert_pending(forwarding.as_mut()).await;
            entered_tx.send(()).unwrap();
            forwarding.await;
            topic
        });
        entered_rx.await.unwrap();
        assert!(!ended.load(Ordering::SeqCst));
        let gossip = gossip_with_task((actor, rpc, local), None, None);
        gossip.shutdown().await.unwrap();
        assert!(
            ended.load(Ordering::SeqCst),
            "forwarder was not destroyed before acknowledgment"
        );
        assert!(matches!(
            held_receiver.recv().await.unwrap(),
            Some(Event::Lagged)
        ));
        assert!(held_receiver.recv().await.unwrap().is_none());
        endpoint.close().await;
    })
    .await
    .expect("held topic forwarder prevented shutdown");
}

#[tokio::test]
async fn opposite_active_senders_preserve_old_receive_halves_until_explicit_stop() {
    tokio::time::timeout(LIMIT, async {
        let a = endpoint().await;
        let b = endpoint().await;
        let (a_first, b_first) = pair(&a, &b, GOSSIP_ALPN).await;
        let (b_second, a_second) = pair(&b, &a, GOSSIP_ALPN).await;
        let connections = [a_first.clone(), b_first.clone(), a_second.clone(), b_second.clone()];
        let (mut actor_a, rpc_a, local_a) = actor(&a);
        actor_a.handle_connection(b.id(), ConnOrigin::Dial, a_first);
        actor_a.handle_connection(b.id(), ConnOrigin::Accept, a_second.clone());
        let (mut actor_b, rpc_b, local_b) = actor(&b);
        actor_b.handle_connection(a.id(), ConnOrigin::Dial, b_second);
        actor_b.handle_connection(a.id(), ConnOrigin::Accept, b_first.clone());
        assert!(matches!(actor_a.peers.get(&b.id()), Some(PeerState::Active {active_conn_id, ..}) if *active_conn_id == a_second.stable_id()));
        assert!(matches!(actor_b.peers.get(&a.id()), Some(PeerState::Active {active_conn_id, ..}) if *active_conn_id == b_first.stable_id()));
        let ga = gossip_with_task((actor_a, rpc_a, local_a), None, None);
        let gb = gossip_with_task((actor_b, rpc_b, local_b), None, None);
        let topic = TopicId::from([49; 32]);
        let (send_a, mut recv_a) = ga.subscribe(topic, vec![b.id()]).await.unwrap().split();
        let (send_b, mut recv_b) = gb.subscribe(topic, vec![a.id()]).await.unwrap().split();
        let (joined_a, joined_b) = tokio::join!(recv_a.joined(), recv_b.joined());
        joined_a.unwrap();
        joined_b.unwrap();
        send_a.broadcast(Bytes::from_static(b"A on second")).await.unwrap();
        received(&mut recv_b, b"A on second").await;
        send_b.broadcast(Bytes::from_static(b"B on first")).await.unwrap();
        received(&mut recv_a, b"B on first").await;
        assert!(connections.iter().all(|conn| conn.close_reason().is_none()), "successful send-half completion must preserve opposite receive halves");
        let (stopped_a, stopped_b) = tokio::join!(ga.shutdown(), gb.shutdown());
        stopped_a.unwrap();
        stopped_b.unwrap();
        assert!(connections.iter().all(|conn| conn.close_reason().is_some()));
        a.close().await;
        b.close().await;
    })
    .await
    .expect("opposite native send paths did not remain usable");
}

#[derive(Debug, Clone)]
struct CurrentGossip(Arc<RwLock<Gossip>>);
impl ProtocolHandler for CurrentGossip {
    async fn accept(&self, connection: Connection) -> Result<(), AcceptError> {
        let gossip = self.0.read().unwrap().clone();
        gossip
            .handle_connection(connection)
            .await
            .map_err(AcceptError::from_err)
    }
}

#[derive(Debug, Clone)]
struct EchoProbe;
impl ProtocolHandler for EchoProbe {
    async fn accept(&self, connection: Connection) -> Result<(), AcceptError> {
        while let Ok((mut send, mut recv)) = connection.accept_bi().await {
            let body = recv.read_to_end(64).await.map_err(AcceptError::from_err)?;
            send.write_all(&body).await.map_err(AcceptError::from_err)?;
            send.finish().map_err(AcceptError::from_err)?;
        }
        Ok(())
    }
}

async fn probe(connection: &Connection, payload: &[u8]) {
    let (mut send, mut recv) = connection.open_bi().await.unwrap();
    send.write_all(payload).await.unwrap();
    send.finish().unwrap();
    assert_eq!(recv.read_to_end(64).await.unwrap(), payload);
}

#[tokio::test]
async fn same_endpoint_recreates_gossip_while_existing_other_alpn_connection_stays_usable() {
    tokio::time::timeout(LIMIT, async {
        let a = endpoint().await;
        let b = endpoint().await;
        let lookup = MemoryLookup::new();
        lookup.add_endpoint_info(a.addr());
        lookup.add_endpoint_info(b.addr());
        a.address_lookup().unwrap().add(lookup.clone());
        b.address_lookup().unwrap().add(lookup);
        let old = Gossip::builder().spawn(a.clone());
        let gb = Gossip::builder().spawn(b.clone());
        let current = CurrentGossip(Arc::new(RwLock::new(old.clone())));
        let router_a = Router::builder(a.clone())
            .accept(GOSSIP_ALPN, current.clone())
            .accept(PROBE_ALPN, EchoProbe)
            .spawn();
        let router_b = Router::builder(b.clone())
            .accept(GOSSIP_ALPN, gb.clone())
            .spawn();
        let live_probe = b.connect(a.addr(), PROBE_ALPN).await.unwrap();
        probe(&live_probe, b"before stop").await;
        let topic = TopicId::from([50; 32]);
        let old_api = old.inner.api.clone();
        let (old_send, mut old_recv) = old.subscribe(topic, vec![b.id()]).await.unwrap().split();
        let (send_b, mut recv_b) = gb.subscribe(topic, vec![a.id()]).await.unwrap().split();
        let (joined_a, joined_b) = tokio::join!(old_recv.joined(), recv_b.joined());
        joined_a.unwrap();
        joined_b.unwrap();
        old_send
            .broadcast(Bytes::from_static(b"before retirement"))
            .await
            .unwrap();
        received(&mut recv_b, b"before retirement").await;
        old.shutdown().await.unwrap();
        probe(&live_probe, b"after stop").await;
        assert!(old_api.subscribe(topic, vec![b.id()]).await.is_err());
        assert!(old_send.join_peers(vec![b.id()]).await.is_err());
        let replacement = Gossip::builder().spawn(a.clone());
        *current.0.write().unwrap() = replacement.clone();
        let (new_send, mut new_recv) = replacement
            .subscribe(topic, vec![b.id()])
            .await
            .unwrap()
            .split();
        new_recv.joined().await.unwrap();
        new_send
            .broadcast(Bytes::from_static(b"new actor same endpoint"))
            .await
            .unwrap();
        received(&mut recv_b, b"new actor same endpoint").await;
        send_b
            .broadcast(Bytes::from_static(b"reply to replacement"))
            .await
            .unwrap();
        received(&mut new_recv, b"reply to replacement").await;
        probe(&live_probe, b"after recreation").await;
        old.shutdown().await.unwrap();
        let (stop_a, stop_b) = tokio::join!(replacement.shutdown(), gb.shutdown());
        stop_a.unwrap();
        stop_b.unwrap();
        live_probe.close(0u32.into(), b"test complete");
        router_a.shutdown().await.unwrap();
        router_b.shutdown().await.unwrap();
    })
    .await
    .expect("same Endpoint gossip replacement timed out");
}
