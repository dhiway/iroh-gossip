use std::collections::HashSet;

use bytes::Bytes;
use iroh::{Endpoint, EndpointId};
use n0_future::task::{JoinError, JoinSet};
use tracing::Instrument;

use super::OwnedConnection;

type DialResult = (
    EndpointId,
    Result<OwnedConnection, iroh::endpoint::ConnectError>,
);

#[derive(Debug)]
pub(super) struct Dialer {
    endpoint: Endpoint,
    /// The ALPN of every dial.
    alpn: Bytes,
    pending: JoinSet<DialResult>,
    pending_dials: HashSet<EndpointId>,
}

impl Dialer {
    /// Creates a dialer that connects with `alpn` through `endpoint`.
    pub(super) fn new(endpoint: Endpoint, alpn: Bytes) -> Self {
        Self {
            endpoint,
            alpn,
            pending: Default::default(),
            pending_dials: Default::default(),
        }
    }

    /// Starts to dial an endpoint by [`EndpointId`].
    pub(super) fn queue_dial(&mut self, endpoint_id: EndpointId) {
        if self.is_dialing(endpoint_id) {
            return;
        }
        self.pending_dials.insert(endpoint_id);
        let endpoint = self.endpoint.clone();
        let alpn = self.alpn.clone();
        self.pending.spawn(
            async move {
                let res = endpoint
                    .connect(endpoint_id, &alpn)
                    .await
                    .map(|conn| OwnedConnection(Some(conn)));
                (endpoint_id, res)
            }
            .instrument(tracing::Span::current()),
        );
    }

    /// Checks if an endpoint is currently being dialed.
    pub(super) fn is_dialing(&self, endpoint: EndpointId) -> bool {
        self.pending_dials.contains(&endpoint)
    }

    /// Waits for the next dial operation to complete.
    pub(super) async fn next_conn(&mut self) -> Result<DialResult, JoinError> {
        match self.pending.join_next().await {
            Some(result) => {
                let (peer, connection) = result?;
                self.pending_dials.remove(&peer);
                Ok((peer, connection))
            }
            None => std::future::pending().await,
        }
    }

    /// Cancel pending dials and close successes the actor has yet to take.
    pub(super) async fn shutdown(&mut self) -> Result<(), ()> {
        self.pending.abort_all();
        let mut failed = false;
        while let Some(result) = self.pending.join_next().await {
            match result {
                Ok(_) => {}
                Err(err) => failed |= !err.is_cancelled(),
            }
        }
        self.pending_dials.clear();
        if failed {
            Err(())
        } else {
            Ok(())
        }
    }
}

#[cfg(all(test, not(target_family = "wasm")))]
mod tests {
    use iroh::endpoint::presets;
    use n0_error::{Result, StdResultExt};
    use tokio::{sync::oneshot, time::timeout};

    use super::*;

    #[tokio::test(flavor = "multi_thread")]
    async fn shutdown_reaps_a_pending_dial_and_closes_an_unclaimed_success() -> Result {
        use std::time::Duration;

        let a = Endpoint::bind(presets::Minimal).await?;
        let b = Endpoint::builder(presets::Minimal)
            .alpns(vec![crate::net::ALPN.to_vec()])
            .bind()
            .await?;
        let (conn, peer) = tokio::try_join!(
            async { Ok::<_, n0_error::AnyError>(a.connect(b.addr(), crate::net::ALPN).await?) },
            async {
                let incoming = b.accept().await.expect("endpoint accepting");
                Ok(incoming.await?)
            }
        )?;

        let mut dialer = Dialer::new(a.clone(), Bytes::from_static(crate::net::ALPN));
        let connected = conn.clone();
        let peer_id = b.id();
        let complete = dialer
            .pending
            .spawn(async move { (peer_id, Ok(OwnedConnection(Some(connected)))) });
        let (started, running) = oneshot::channel();
        let (retired, mut destroyed) = oneshot::channel::<()>();
        dialer.pending.spawn(async move {
            // Dropping the live dial task closes this sender.
            let _retired = retired;
            started.send(()).unwrap();
            std::future::pending().await
        });
        timeout(Duration::from_secs(5), async {
            running.await.unwrap();
            while !complete.is_finished() {
                tokio::task::yield_now().await;
            }
        })
        .await
        .std_context("dial tasks ready")?;
        assert!(conn.close_reason().is_none());
        assert!(dialer.shutdown().await.is_ok());
        assert!(matches!(
            destroyed.try_recv(),
            Err(oneshot::error::TryRecvError::Closed)
        ));
        assert!(conn.close_reason().is_some());
        timeout(Duration::from_secs(5), peer.closed())
            .await
            .std_context("abandoned dial closed remotely")?;
        tokio::join!(a.close(), b.close());
        Ok(())
    }
}
