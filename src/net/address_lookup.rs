//! An address lookup service to gather addressing info collected from gossip Join and ForwardJoin messages.

use std::{
    collections::{btree_map::Entry, BTreeMap},
    sync::{Arc, RwLock, Weak},
    time::Duration,
};

use iroh::address_lookup::{self, AddressLookup, EndpointData, EndpointInfo};
use iroh_base::EndpointId;
use n0_future::{
    boxed::BoxStream,
    stream::{self, StreamExt},
    task::AbortOnDropHandle,
    time::SystemTime,
};
use tokio::sync::Mutex;
use tokio_util::sync::CancellationToken;

pub(crate) struct RetentionOpts {
    /// How long to keep received endpoint info records alive before pruning them
    retention: Duration,
    /// How often to check for expired entries
    evict_interval: Duration,
}

impl Default for RetentionOpts {
    fn default() -> Self {
        Self {
            retention: Duration::from_secs(60 * 5),
            evict_interval: Duration::from_secs(30),
        }
    }
}

/// An address lookup service that expires endpoints after some time.
///
/// It is added to the endpoint when constructing a gossip instance, and the gossip actor
/// then adds endpoint addresses as received with Join or ForwardJoin messages.
#[derive(Debug, Clone)]
pub(crate) struct GossipAddressLookup {
    endpoints: NodeMap,
    stop: CancellationToken,
    task: Arc<Mutex<WorkerState>>,
}

#[derive(Debug)]
enum WorkerState {
    Running(AbortOnDropHandle<()>),
    Complete(Result<(), ()>),
}

type NodeMap = Arc<RwLock<BTreeMap<EndpointId, StoredEndpointInfo>>>;

#[derive(Debug)]
struct StoredEndpointInfo {
    data: EndpointData,
    last_updated: SystemTime,
}

impl Default for GossipAddressLookup {
    fn default() -> Self {
        Self::new()
    }
}

impl GossipAddressLookup {
    const PROVENANCE: &'static str = "gossip";

    /// Creates a new gossip address lookup instance.
    pub(crate) fn new() -> Self {
        Self::with_opts(Default::default())
    }

    pub(crate) fn with_opts(opts: RetentionOpts) -> Self {
        let endpoints: NodeMap = Default::default();
        let stop = CancellationToken::new();
        let task = n0_future::task::spawn(evict_expired(
            Arc::downgrade(&endpoints),
            stop.clone(),
            opts,
        ));
        Self {
            endpoints,
            stop,
            task: Arc::new(Mutex::new(WorkerState::Running(AbortOnDropHandle::new(
                task,
            )))),
        }
    }

    /// Stop this lookup and await its worker. Retained clones become inert.
    ///
    /// On native runtimes success observes actual worker completion. Browser
    /// task primitives do not provide an equivalent completion/panic guarantee.
    /// A previously resolved stream owns its historical snapshot and is not
    /// revoked. Dropping a shutdown waiter retains shared join ownership.
    pub(crate) async fn shutdown(&self) -> Result<(), ()> {
        self.stop.cancel();
        let poisoned = match self.endpoints.write() {
            Ok(mut endpoints) => {
                endpoints.clear();
                false
            }
            Err(poisoned) => {
                // Still remove retained addresses, but never certify success
                // after a failure while the worker held this map.
                poisoned.into_inner().clear();
                true
            }
        };
        let mut state = self.task.lock().await;
        match &mut *state {
            WorkerState::Running(task) => {
                // Keep the handle in the slot across await. Canceling this
                // waiter must not abort or detach the worker.
                let failed = task.await.is_err() || poisoned;
                let result = if failed { Err(()) } else { Ok(()) };
                *state = WorkerState::Complete(result);
                result
            }
            WorkerState::Complete(result) => {
                if poisoned {
                    *result = Err(());
                }
                *result
            }
        }
    }

    /// Augments endpoint addressing information for the given endpoint ID.
    ///
    /// The provided addressing information is combined with the existing info in the in-memory
    /// lookup.  Any new direct addresses are added to those already present while the
    /// relay URL is overwritten.
    pub(crate) fn add(&self, endpoint_info: impl Into<EndpointInfo>) {
        let last_updated = SystemTime::now();
        let EndpointInfo { endpoint_id, data } = endpoint_info.into();
        let mut guard = self.endpoints.write().expect("poisoned");
        if self.stop.is_cancelled() {
            return;
        }
        match guard.entry(endpoint_id) {
            Entry::Occupied(mut entry) => {
                let existing = entry.get_mut();
                existing.data.add_addrs(data.addrs().cloned());
                existing.data.set_user_data(data.user_data().cloned());
                existing.last_updated = last_updated;
            }
            Entry::Vacant(entry) => {
                entry.insert(StoredEndpointInfo { data, last_updated });
            }
        }
    }
}

async fn evict_expired(
    endpoints: Weak<RwLock<BTreeMap<EndpointId, StoredEndpointInfo>>>,
    stop: CancellationToken,
    opts: RetentionOpts,
) {
    let mut interval = n0_future::time::interval(opts.evict_interval);
    loop {
        tokio::select! {
            biased;
            _ = stop.cancelled() => break,
            _ = interval.tick() => {}
        }
        let Some(endpoints) = endpoints.upgrade() else {
            break;
        };
        let now = SystemTime::now();
        let mut endpoints = endpoints.write().expect("poisoned");
        if stop.is_cancelled() {
            break;
        }
        endpoints.retain(|_k, v| {
            let age = now.duration_since(v.last_updated).unwrap_or(Duration::MAX);
            age <= opts.retention
        });
    }
}

impl AddressLookup for GossipAddressLookup {
    fn resolve(
        &self,
        endpoint_id: EndpointId,
    ) -> Option<BoxStream<Result<address_lookup::Item, address_lookup::Error>>> {
        let guard = self.endpoints.read().expect("poisoned");
        if self.stop.is_cancelled() {
            return None;
        }
        let info = guard.get(&endpoint_id)?;
        let last_updated = info
            .last_updated
            .duration_since(SystemTime::UNIX_EPOCH)
            .expect("time drift")
            .as_micros() as u64;
        let item = address_lookup::Item::new(
            EndpointInfo::from_parts(endpoint_id, info.data.clone()),
            Self::PROVENANCE,
            Some(last_updated),
        );
        Some(stream::iter(Some(Ok(item))).boxed())
    }
}

#[cfg(test)]
mod tests {
    use std::{
        future::{poll_fn, Future},
        pin::Pin,
        sync::Arc,
        task::Poll,
        time::Duration,
    };

    use iroh::{address_lookup::AddressLookup, EndpointAddr, SecretKey};
    use n0_future::StreamExt;
    use rand::{RngExt, SeedableRng};

    use tokio::sync::{oneshot, Mutex};
    use tokio_util::sync::CancellationToken;

    use super::{evict_expired, GossipAddressLookup, NodeMap, RetentionOpts, WorkerState};

    // Hold the real worker's owning task after its eviction loop observes stop.
    // This makes join custody deterministic without a production test hook.
    fn with_held_worker() -> (
        GossipAddressLookup,
        oneshot::Receiver<()>,
        oneshot::Sender<()>,
    ) {
        let endpoints: NodeMap = Default::default();
        let stop = CancellationToken::new();
        let (ended_tx, ended_rx) = oneshot::channel();
        let (release_tx, release_rx) = oneshot::channel();
        let worker = evict_expired(
            Arc::downgrade(&endpoints),
            stop.clone(),
            RetentionOpts::default(),
        );
        let task = n0_future::task::spawn(async move {
            worker.await;
            ended_tx.send(()).ok();
            release_rx.await.ok();
        });
        (
            GossipAddressLookup {
                endpoints,
                stop,
                task: Arc::new(Mutex::new(WorkerState::Running(
                    n0_future::task::AbortOnDropHandle::new(task),
                ))),
            },
            ended_rx,
            release_tx,
        )
    }

    async fn assert_pending<F: Future>(mut future: Pin<&mut F>) {
        let result = poll_fn(|cx| Poll::Ready(future.as_mut().poll(cx))).await;
        assert!(
            result.is_pending(),
            "shutdown did not await the held worker"
        );
    }

    #[tokio::test]
    async fn shutdown_retires_worker_and_retained_lookup_but_keeps_historical_snapshot() {
        tokio::time::timeout(Duration::from_secs(5), async {
            let (lookup, ended, release) = with_held_worker();
            let retained = lookup.clone();
            let id = SecretKey::from_bytes(&[9; 32]).public();
            lookup.add(EndpointAddr::new(id));
            let mut snapshot = retained.resolve(id).expect("pre-stop snapshot");
            let mut shutdown = Box::pin(lookup.shutdown());
            assert_pending(shutdown.as_mut()).await;
            ended
                .await
                .expect("eviction loop observed cooperative stop");
            assert!(lookup.endpoints.read().unwrap().is_empty());
            retained.add(EndpointAddr::new(id));
            assert!(retained.resolve(id).is_none());
            assert!(retained.endpoints.read().unwrap().is_empty());
            release.send(()).expect("worker still owned and waiting");
            assert_eq!(shutdown.await, Ok(()));
            assert!(matches!(
                &*retained.task.lock().await,
                WorkerState::Complete(Ok(()))
            ));
            // A previously returned stream is a caller-owned snapshot, not a
            // new post-stop resolution or a new worker operation.
            assert!(matches!(snapshot.next().await, Some(Ok(_))));
            assert!(snapshot.next().await.is_none());
            assert_eq!(retained.shutdown().await, Ok(()));
        })
        .await
        .expect("lookup shutdown timed out");
    }

    #[tokio::test]
    async fn dropped_shutdown_waiter_preserves_worker_for_concurrent_and_repeated_waiters() {
        tokio::time::timeout(Duration::from_secs(5), async {
            let (lookup, ended, release) = with_held_worker();
            let retained = lookup.clone();
            let mut first = Box::pin(lookup.shutdown());
            assert_pending(first.as_mut()).await;
            ended.await.expect("real eviction loop ended");
            drop(first);
            assert!(matches!(
                &*lookup.task.lock().await,
                WorkerState::Running(_)
            ));
            let mut second = Box::pin(lookup.shutdown());
            let mut third = Box::pin(retained.shutdown());
            assert_pending(second.as_mut()).await;
            assert_pending(third.as_mut()).await;
            release
                .send(())
                .expect("dropped waiter did not abort worker");
            assert_eq!(tokio::join!(second, third), (Ok(()), Ok(())));
            assert_eq!(lookup.shutdown().await, Ok(()));
        })
        .await
        .expect("concurrent lookup shutdown timed out");
    }

    #[tokio::test]
    async fn worker_panic_is_a_sticky_shutdown_failure() {
        tokio::time::timeout(Duration::from_secs(5), async {
            // The actual eviction worker constructs its timer before polling
            // stop; a zero interval panics there on the native runtime.
            let lookup = GossipAddressLookup::with_opts(RetentionOpts {
                retention: Duration::from_secs(1),
                evict_interval: Duration::ZERO,
            });
            let retained = lookup.clone();
            assert_eq!(lookup.shutdown().await, Err(()));
            assert_eq!(retained.shutdown().await, Err(()));
        })
        .await
        .expect("failed lookup shutdown timed out");
    }

    #[tokio::test]
    async fn test_retention() {
        let opts = RetentionOpts {
            evict_interval: Duration::from_millis(100),
            retention: Duration::from_millis(500),
        };
        let disco = GossipAddressLookup::with_opts(opts);

        let rng = &mut rand::rngs::ChaCha12Rng::seed_from_u64(1);
        let k1 = SecretKey::from_bytes(&rng.random());
        let a1 = EndpointAddr::new(k1.public());

        disco.add(a1);

        assert!(matches!(
            disco.resolve(k1.public()).unwrap().next().await,
            Some(Ok(_))
        ));

        tokio::time::sleep(Duration::from_millis(200)).await;

        assert!(matches!(
            disco.resolve(k1.public()).unwrap().next().await,
            Some(Ok(_))
        ));

        tokio::time::sleep(Duration::from_millis(700)).await;

        assert!(disco.resolve(k1.public()).is_none());
        disco.shutdown().await.unwrap();
    }
}
