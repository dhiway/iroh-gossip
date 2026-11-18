use super::*;
use crate::net::{OwnedConnection, TaskCompletion};
use iroh::address_lookup::AddressLookup;
use n0_future::future::poll_once;

fn spawn_gossip(actor: Actor, to_actor: mpsc::Sender<ToActor>) -> Gossip {
    let stop = actor.stop.clone();
    let lookup = actor.state.address_lookup.clone();
    let max_message_size = actor.proto.max_message_size();
    let metrics = actor.metrics.clone();
    Gossip::new(
        to_actor,
        stop,
        AbortOnDropHandle::new(spawn(actor.run())),
        lookup,
        max_message_size,
        metrics,
    )
}

/// A close handles the messages queued before it, not those that arrive meanwhile.
///
/// Other connections can refill the queue as fast as the actor handles it.
#[tokio::test(flavor = "multi_thread")]
#[traced_test]
async fn close_does_not_wait_for_an_empty_queue() -> Result {
    let pair = TestPair::new(1).await?;
    let (conn, _peer_conn) = pair.connect_back().await?;
    let (mut actor, _to_actor) = Actor::new(
        pair.ep1.clone(),
        ActorConfig {
            proto: Default::default(),
            ..Default::default()
        },
        Default::default(),
        Default::default(),
    );
    conn.close(0u32.into(), b"done");
    actor.add_connection(conn);
    let (peer, conn, result) = actor
        .connection_tasks
        .join_next()
        .await
        .expect("one task")
        .expect("no panic");

    // Another connection: it sends ten full queues, as fast as the actor takes them.
    let in_event_tx = actor.in_event_tx.clone();
    let other = SecretKey::generate().public();
    let producer = AbortOnDropHandle::new(spawn(async move {
        for _ in 0..10 * IN_EVENT_CAP {
            in_event_tx
                .send(InEvent::PeerDisconnected(other))
                .await
                .expect("actor alive");
        }
    }));
    while actor.in_event_rx.len() < IN_EVENT_CAP {
        tokio::task::yield_now().await;
    }
    actor.on_conn_task_finished(peer, conn, result);
    assert!(
        !producer.is_finished(),
        "the close waited for an empty queue"
    );
    Ok(())
}

/// A busy publisher can neither delay the stop request nor make a dropped
/// shutdown waiter release the actor's join handle.
#[tokio::test]
#[traced_test]
async fn shutdown_waits_for_the_actor_across_cancelled_and_concurrent_waiters() -> Result {
    let pair = TestPair::new(1).await?;
    let (mut actor, to_actor) = Actor::new(
        pair.ep1.clone(),
        Default::default(),
        Default::default(),
        Default::default(),
    );
    publish_forever(&mut actor);
    let lookup = actor.state.address_lookup.clone();
    let max_message_size = actor.proto.max_message_size();
    let metrics = actor.metrics.clone();
    let (ended, stopped) = oneshot::channel();
    let (release, released) = oneshot::channel();
    let gossip = Gossip::new(
        to_actor,
        actor.stop.clone(),
        AbortOnDropHandle::new(spawn(async move {
            let result = actor.run().await;
            ended.send(()).unwrap();
            released.await.unwrap();
            result
        })),
        lookup,
        max_message_size,
        metrics,
    );
    let retained = gossip.clone();
    let mut first = Box::pin(gossip.shutdown());
    assert!(poll_once(first.as_mut()).await.is_none());
    drop(first);
    timeout(Duration::from_secs(5), stopped)
        .await
        .std_context("actor stop")?
        .std_context("actor dropped")?;
    let mut second = Box::pin(gossip.shutdown());
    let mut third = Box::pin(retained.shutdown());
    assert!(poll_once(second.as_mut()).await.is_none());
    assert!(poll_once(third.as_mut()).await.is_none());
    release.send(()).unwrap();
    timeout(Duration::from_secs(5), async {
        tokio::try_join!(second, third)?;
        gossip.shutdown().await
    })
    .await
    .std_context("join after cancellation")??;
    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn shutdown_closes_active_and_queued_connections_and_retires_lookup() -> Result {
    let pair = TestPair::new(1).await?;
    let (conn, peer) = pair.connect_back().await?;
    let (queued, queued_peer) = pair.connect_back().await?;
    let lookup = GossipAddressLookup::default();
    let id = pair.ep2.id();
    lookup.add(pair.ep2.addr());
    let mut snapshot = lookup.resolve(id).expect("known address");
    let (mut actor, to_actor) = Actor::new(
        pair.ep1.clone(),
        Default::default(),
        Default::default(),
        lookup.clone(),
    );
    actor.add_connection(conn.clone());
    let (started, running) = oneshot::channel();
    let (retired, mut destroyed) = oneshot::channel();
    actor.topic_event_forwarders.spawn(async move {
        struct ReportDrop(Option<oneshot::Sender<()>>);
        impl Drop for ReportDrop {
            fn drop(&mut self) {
                self.0.take().unwrap().send(()).ok();
            }
        }
        let _guard = ReportDrop(Some(retired));
        started.send(()).unwrap();
        std::future::pending().await
    });
    to_actor
        .send(ToActor::Connection(OwnedConnection(Some(queued.clone()))))
        .await
        .std_context("queue connection")?;
    running.await.std_context("start topic forwarder")?;
    // Stop before dispatch so the second connection is retired through the mailbox.
    actor.stop.cancel();
    let gossip = spawn_gossip(actor, to_actor);
    gossip.shutdown().await?;
    destroyed.try_recv().expect("forwarder destroyed");
    assert!(conn.close_reason().is_some());
    assert!(queued.close_reason().is_some());
    timeout(Duration::from_secs(5), async {
        tokio::join!(peer.closed(), queued_peer.closed())
    })
    .await
    .std_context("remote closes")?;
    assert!(matches!(snapshot.next().await, Some(Ok(_))));
    assert!(lookup.resolve(id).is_none());
    lookup.add(pair.ep2.addr());
    assert!(lookup.resolve(id).is_none());
    Ok(())
}

#[tokio::test]
async fn dropping_a_pending_handoff_closes_its_connection() -> Result {
    let pair = TestPair::new(1).await?;
    let (conn, peer) = pair.connect_back().await?;
    let (to_actor, _inbox) = mpsc::channel(1);
    let _slot = to_actor.reserve().await.std_context("fill inbox")?;
    let gossip = Gossip::new(
        to_actor.clone(),
        CancellationToken::new(),
        AbortOnDropHandle::new(spawn(std::future::pending())),
        Default::default(),
        4096,
        Default::default(),
    );
    let mut handoff = Box::pin(gossip.handle_connection(conn.clone()));
    assert!(poll_once(handoff.as_mut()).await.is_none());
    assert!(conn.close_reason().is_none());
    drop(handoff);
    assert!(conn.close_reason().is_some());
    timeout(Duration::from_secs(5), peer.closed())
        .await
        .std_context("remote close")?;
    Ok(())
}

#[cfg(not(target_family = "wasm"))]
#[tokio::test]
async fn a_child_failure_is_sticky_for_every_shutdown_waiter() -> Result {
    let pair = TestPair::new(1).await?;
    let lookup = GossipAddressLookup::default();
    lookup.add(pair.ep2.addr());
    let (mut actor, to_actor) = Actor::new(
        pair.ep1.clone(),
        Default::default(),
        Default::default(),
        lookup.clone(),
    );
    actor.topic_event_forwarders.spawn(async {
        panic!("failed topic forwarder");
    });
    let stop = actor.stop.clone();
    let gossip = spawn_gossip(actor, to_actor);
    timeout(Duration::from_secs(5), stop.cancelled())
        .await
        .std_context("actor observed child failure")?;
    let retained = gossip.clone();
    let (first, second) = tokio::join!(gossip.shutdown(), retained.shutdown());
    for outcome in [first, second, gossip.shutdown().await] {
        assert!(matches!(outcome, Err(crate::Error::ShutdownFailed { .. })));
    }
    assert!(lookup.resolve(pair.ep2.id()).is_none());
    Ok(())
}

#[cfg(not(target_family = "wasm"))]
#[tokio::test]
async fn an_actor_panic_is_sticky_and_retires_the_retained_lookup() -> Result {
    let pair = TestPair::new(1).await?;
    let lookup = GossipAddressLookup::default();
    lookup.add(pair.ep2.addr());
    let (mut actor, to_actor) = Actor::new(
        pair.ep1.clone(),
        Default::default(),
        Default::default(),
        lookup.clone(),
    );
    let (sender, receiver) = mpsc::channel(1);
    drop(sender);
    // Exercise the actor's actual invariant error before requesting shutdown.
    actor.in_event_rx = receiver;
    let gossip = spawn_gossip(actor, to_actor);
    timeout(Duration::from_secs(5), async {
        loop {
            {
                let state = gossip.inner.completion.lock().await;
                if matches!(&*state, TaskCompletion::Running(task) if task.is_finished()) {
                    break;
                }
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .std_context("actor panic")?;
    for _ in 0..2 {
        assert!(matches!(
            gossip.shutdown().await,
            Err(crate::Error::ShutdownFailed { .. })
        ));
        assert!(lookup.resolve(pair.ep2.id()).is_none());
    }
    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn shutdown_allows_same_endpoint_recreation_with_another_alpn_still_live() -> Result {
    use std::sync::RwLock;

    use iroh::protocol::{AcceptError, ProtocolHandler};

    #[derive(Debug, Clone)]
    struct ReplaceableGossip(Arc<RwLock<Gossip>>);

    impl ProtocolHandler for ReplaceableGossip {
        async fn accept(&self, conn: Connection) -> std::result::Result<(), AcceptError> {
            let gossip = self.0.read().unwrap().clone();
            gossip.accept(conn).await
        }

        async fn shutdown(&self) {
            let gossip = self.0.read().unwrap().clone();
            ProtocolHandler::shutdown(&gossip).await;
        }
    }

    async fn probe(send: &Connection, recv: &Connection, data: &[u8]) -> Result {
        let mut send = send.open_uni().await.std_context("open probe stream")?;
        send.write_all(data).await.std_context("write probe")?;
        send.finish().std_context("finish probe")?;
        let mut recv = recv.accept_uni().await.std_context("accept probe stream")?;
        assert_eq!(recv.read_to_end(64).await.std_context("read probe")?, data);
        Ok(())
    }

    timeout(Duration::from_secs(15), async {
        const PROBE_ALPN: &[u8] = b"/gossip-shutdown-independent-probe";
        let a = Endpoint::builder(presets::Minimal)
            .relay_mode(RelayMode::Disabled)
            .alpns(vec![PROBE_ALPN.to_vec()])
            .bind()
            .await?;
        let b = Endpoint::builder(presets::Minimal)
            .relay_mode(RelayMode::Disabled)
            .bind()
            .await?;
        let lookup = MemoryLookup::new();
        lookup.add_endpoint_info(a.addr());
        lookup.add_endpoint_info(b.addr());
        a.address_lookup()?.add(lookup.clone());
        b.address_lookup()?.add(lookup);

        let (probe_b, probe_a) = tokio::try_join!(
            async { Ok::<_, AnyError>(b.connect(a.addr(), PROBE_ALPN).await?) },
            async { Ok(a.accept().await.std_context("accept probe")?.await?) },
        )?;
        probe(&probe_b, &probe_a, b"before stop").await?;

        let old = Gossip::builder().spawn(a.clone());
        let gossip_b = Gossip::builder().spawn(b.clone());
        let handler = ReplaceableGossip(Arc::new(RwLock::new(old.clone())));
        let router_a = Router::builder(a.clone())
            .accept(ALPN, handler.clone())
            .spawn();
        let router_b = Router::builder(b.clone())
            .accept(ALPN, gossip_b.clone())
            .spawn();
        let topic = TopicId::from([73; 32]);
        let (_peer_send, mut peer_events) = gossip_b.subscribe(topic, vec![a.id()]).await?.split();
        let (old_send, _old_events) = old.subscribe_and_join(topic, vec![b.id()]).await?.split();

        old.shutdown().await?;
        assert!(old_send.join_peers(vec![b.id()]).await.is_err());
        probe(&probe_b, &probe_a, b"after stop").await?;

        let replacement = Gossip::builder().spawn(a);
        *handler.0.write().unwrap() = replacement.clone();
        let (restored_send, _restored_events) = replacement
            .subscribe_and_join(topic, vec![b.id()])
            .await?
            .split();
        let payload = Bytes::from_static(b"new gossip on the original endpoint");
        restored_send.broadcast(payload.clone()).await?;
        loop {
            let event = peer_events
                .next()
                .await
                .transpose()?
                .std_context("peer closed")?;
            if matches!(event, Event::Received(message) if message.content == payload) {
                break;
            }
        }
        probe(&probe_b, &probe_a, b"after recreation").await?;
        old.shutdown().await?;
        tokio::try_join!(router_a.shutdown(), router_b.shutdown()).std_context("routers")?;
        Ok::<_, AnyError>(())
    })
    .await
    .std_context("same endpoint lifecycle")??;
    Ok(())
}
