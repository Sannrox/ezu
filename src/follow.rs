//! The follower side of log shipping: [`follow`] holds a stream of the
//! leader's committed records open (`Replication.Follow`), applies each
//! batch to a follower engine, and reconnects after a transport failure.
//! It resumes after the follower's applied LSN every time, so a dropped
//! stream or a restart neither skips nor repeats a record. What fails
//! closed (a gap, an epoch going back, a sealed record, a last record that
//! differs from the leader's, the leader no longer holding what comes next)
//! stops it for good.

use crate::api::proto::replication_client::ReplicationClient;
use crate::api::proto::{FollowBatch, FollowRequest, RecordFingerprint};
use crate::durable::{DurableEngine, DurableError, ReplicaError, Shipped};
use std::sync::Arc;
use std::time::Duration;

/// A stream silent this long is taken as lost; the leader sends a
/// heartbeat every second.
const SILENCE: Duration = Duration::from_secs(10);
/// Large enough for a batch holding the largest record the log takes.
const MAX_MESSAGE: usize = 80 << 20;
const MAX_BACKOFF: Duration = Duration::from_secs(5);

/// The leader at `url`. Over `https`, `ca` (PEM) verifies it (the system's
/// roots are not used), and `identity`, a PEM certificate and key, answers
/// a leader that asks for a client certificate.
pub fn endpoint(
    url: &str,
    ca: Option<String>,
    identity: Option<(String, String)>,
) -> Result<tonic::transport::Endpoint, tonic::transport::Error> {
    let endpoint = tonic::transport::Endpoint::from_shared(url.to_string())?
        .connect_timeout(Duration::from_secs(5));
    if !url.starts_with("https://") {
        return Ok(endpoint);
    }
    let mut tls = tonic::transport::ClientTlsConfig::new();
    if let Some(ca) = ca {
        tls = tls.ca_certificate(tonic::transport::Certificate::from_pem(ca));
    }
    if let Some((cert, key)) = identity {
        tls = tls.identity(tonic::transport::Identity::from_pem(cert, key));
    }
    endpoint.tls_config(tls)
}

/// Why one stream ended.
enum Ended {
    /// A transport failure or a lost stream; worth retrying.
    Retry(String),
    /// Fail closed: the follower is stopped.
    Stopped(DurableError),
}

/// Follow the leader at `leader` with the replication `token` until the
/// follower stops fail closed, and return why. `on_retry` hears every
/// transport failure before the reconnect.
pub async fn follow(
    engine: Arc<DurableEngine>,
    leader: tonic::transport::Endpoint,
    token: String,
    on_retry: impl Fn(&str),
) -> DurableError {
    // A token that is no header value is a configuration error: refuse it
    // before connecting, so it stops the follower instead of retrying.
    let authorization: tonic::metadata::MetadataValue<tonic::metadata::Ascii> =
        match format!("Bearer {token}").parse() {
            Ok(value) => value,
            Err(_) => {
                return engine.end_following(DurableError::Replica(ReplicaError::Leader(
                    "the replication token is not a valid header value".into(),
                )));
            }
        };
    let mut backoff = Duration::from_millis(100);
    loop {
        let ended = stream(&engine, &leader, &authorization, &mut backoff).await;
        engine.set_connected(false);
        match ended {
            Ended::Stopped(error) => return engine.end_following(error),
            Ended::Retry(why) => on_retry(&why),
        }
        tokio::time::sleep(backoff).await;
        backoff = (backoff * 2).min(MAX_BACKOFF);
    }
}

/// Open one stream after the applied LSN and apply what it carries until
/// it ends. `backoff` is reset once the leader answers.
async fn stream(
    engine: &Arc<DurableEngine>,
    leader: &tonic::transport::Endpoint,
    authorization: &tonic::metadata::MetadataValue<tonic::metadata::Ascii>,
    backoff: &mut Duration,
) -> Ended {
    let applied = match engine.replication() {
        Some(replication) => replication.applied_lsn,
        None => return Ended::Stopped(DurableError::ReadOnly),
    };
    let channel = match leader.connect().await {
        Ok(channel) => channel,
        Err(error) => return Ended::Retry(format!("connect: {error}")),
    };
    let mut client = ReplicationClient::new(channel).max_decoding_message_size(MAX_MESSAGE);
    // Proves to the leader that this follower's history is its own.
    let reading = engine.clone();
    let last = match tokio::task::spawn_blocking(move || reading.fingerprint(applied)).await {
        Ok(Ok(last)) => last.map(|last| RecordFingerprint {
            crc32: last.crc32,
            epoch: last.epoch,
        }),
        Ok(Err(error)) => return Ended::Stopped(error),
        Err(error) => return Ended::Retry(format!("fingerprint task: {error}")),
    };
    let mut request = tonic::Request::new(FollowRequest {
        after_lsn: applied,
        last,
    });
    request
        .metadata_mut()
        .insert("authorization", authorization.clone());
    // An open connection that never answers is retried like a silent stream.
    let mut batches = match tokio::time::timeout(SILENCE, client.follow(request)).await {
        Err(_) => return Ended::Retry("the leader did not answer".into()),
        Ok(Ok(response)) => response.into_inner(),
        Ok(Err(status)) => return refused(engine, status),
    };
    engine.set_connected(true);
    *backoff = Duration::from_millis(100);
    loop {
        let batch = match tokio::time::timeout(SILENCE, batches.message()).await {
            Err(_) => return Ended::Retry("the stream went silent".into()),
            Ok(Ok(Some(batch))) => batch,
            Ok(Ok(None)) => return Ended::Retry("the leader ended the stream".into()),
            Ok(Err(status)) => return refused(engine, status),
        };
        let FollowBatch {
            epoch,
            records,
            leader_lsn,
        } = batch;
        let follower = engine.clone();
        let applying = tokio::task::spawn_blocking(move || {
            follower.apply_shipped(
                Shipped {
                    epoch,
                    records,
                    leader_lsn,
                },
                crate::api::now_ms(),
            )
        });
        match applying.await {
            Ok(Ok(_)) => {}
            // The engine has stopped itself; see `apply_shipped`.
            Ok(Err(error)) => return Ended::Stopped(error),
            Err(error) => return Ended::Retry(format!("apply task: {error}")),
        }
    }
}

/// A status from the leader: what says the follower cannot go on stops
/// it, anything else is retried.
fn refused(engine: &DurableEngine, status: tonic::Status) -> Ended {
    match status.code() {
        // The token is fixed at start, so a refused credential stays refused.
        tonic::Code::OutOfRange
        | tonic::Code::FailedPrecondition
        | tonic::Code::DataLoss
        | tonic::Code::Unauthenticated
        | tonic::Code::PermissionDenied => Ended::Stopped(
            engine.stop_following(ReplicaError::Leader(status.message().to_string())),
        ),
        code => Ended::Retry(format!("{code:?}: {}", status.message())),
    }
}
