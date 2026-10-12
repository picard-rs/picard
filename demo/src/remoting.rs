use std::{net::SocketAddr, sync::Arc, thread, time::Duration};

use picard::actors::ActorSystem;
use picard::remoting::{
    ActorAddress, PeerAuthenticator, RemoteHandler, RemoteListener, RemotePeer, RemoteRequest,
    RemoteResponse,
};
use serde::Serialize;
use serde_json::Value;
use uuid::Uuid;

pub const GATEWAY_PATH: &str = "/user/gateway";
pub const SUPERVISOR_PATH: &str = "/user/supervisor";
const IDENTITY_PATH: &str = "/_living_city/identity";
#[cfg(feature = "console")]
const CONSOLE_PATH: &str = "/_living_city/console";

const DISCOVERY_TIMEOUT: Duration = Duration::from_secs(60);

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct PeerSpec {
    pub district: usize,
    pub endpoint: String,
}

impl std::str::FromStr for PeerSpec {
    type Err = String;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        let (district, endpoint) = value
            .split_once('=')
            .ok_or_else(|| "expected DISTRICT=ws://HOST:PORT/remoting".to_string())?;
        let district = district
            .parse::<usize>()
            .map_err(|err| format!("invalid district number: {err}"))?;
        if endpoint.is_empty() || !(endpoint.starts_with("ws://") || endpoint.starts_with("wss://"))
        {
            return Err("peer endpoint must use ws:// or wss://".into());
        }
        Ok(Self {
            district,
            endpoint: endpoint.to_string(),
        })
    }
}

#[derive(Clone)]
pub struct PeerLink {
    pub district: usize,
    pub system_id: Uuid,
    pub endpoint: String,
    pub connection: picard::remoting::RemoteConnection,
    outgoing: std::sync::mpsc::Sender<(String, Value)>,
}

impl std::fmt::Debug for PeerLink {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("PeerLink")
            .field("district", &self.district)
            .field("endpoint", &self.endpoint)
            .finish_non_exhaustive()
    }
}

impl PeerLink {
    pub fn send<T: Serialize>(&self, path: &str, message: &T) -> Result<(), String> {
        let payload = serde_json::to_value(message)
            .map_err(|err| format!("cannot encode remote message: {err}"))?;
        self.outgoing
            .send((path.to_string(), payload))
            .map_err(|err| format!("remote sender is unavailable: {err}"))
    }

    #[must_use]
    pub fn gateway_address(&self) -> ActorAddress {
        ActorAddress::remote(self.system_id, self.endpoint.clone(), GATEWAY_PATH)
    }
}

pub fn start(
    system: &ActorSystem,
    local_district: usize,
    bind: SocketAddr,
    token: String,
    peers: &[PeerSpec],
) -> Result<(Box<dyn RemoteListener>, Vec<PeerLink>), String> {
    let mut seen = std::collections::HashSet::new();
    for peer in peers {
        if peer.district >= crate::city::DISTRICT_COUNT || peer.district == local_district {
            return Err(format!("invalid remote district ID: {}", peer.district));
        }
        if !seen.insert(peer.district) {
            return Err(format!(
                "remote district {} was configured twice",
                peer.district
            ));
        }
    }

    let authenticator = PeerAuthenticator::new(
        peers
            .iter()
            .map(|peer| (format!("district-{}", peer.district), token.clone())),
    );
    let handler = request_handler(system);
    let transport = Arc::new(picard::remoting::ClusterTransport::new());
    let listener = system
        .listen_remote_with(bind, authenticator, handler, transport.clone())
        .map_err(|err| format!("cannot start remoting listener: {err}"))?;
    let mut links = Vec::with_capacity(peers.len());

    for spec in peers {
        let peer = RemotePeer::new(
            format!("district-{}", spec.district),
            spec.endpoint.clone(),
            token.clone(),
        )
        .map_err(|err| err.to_string())?;
        let connection = system.connect_remote_with(peer, transport.clone());
        let request = || {
            connection.send(
                ActorAddress::local(Uuid::nil(), IDENTITY_PATH),
                None,
                Value::Null,
            )
        };
        let identity = discover_identity(request, DISCOVERY_TIMEOUT)?;
        let (outgoing, receive) = std::sync::mpsc::channel::<(String, Value)>();
        let worker_connection = connection.clone();
        let peer_district = spec.district;
        let endpoint = spec.endpoint.clone();
        thread::Builder::new()
            .name(format!("living-city-remote-{peer_district}"))
            .spawn(move || {
                let mut identity = identity;
                while let Ok((path, payload)) = receive.recv() {
                    let send = |identity: Uuid| {
                        let address = ActorAddress::remote(identity, endpoint.clone(), &path);
                        let fut = worker_connection.send(address, None, payload.clone());
                        picard::remoting::block_on(fut)
                    };
                    if send(identity).is_ok() {
                        continue;
                    }
                    // A restarted peer has a new system ID; look it up again and retry once.
                    let lookup = || {
                        worker_connection.send(
                            ActorAddress::local(Uuid::nil(), IDENTITY_PATH),
                            None,
                            Value::Null,
                        )
                    };
                    match discover_identity(lookup, Duration::from_secs(5)) {
                        Ok(new_identity) => {
                            identity = new_identity;
                            if let Err(err) = send(identity) {
                                eprintln!("remote district {peer_district} send failed: {err}");
                            }
                        }
                        Err(err) => eprintln!("remote district {peer_district}: {err}"),
                    }
                }
            })
            .map_err(|err| format!("cannot start remote sender: {err}"))?;
        #[cfg(feature = "console")]
        system.register_console_remote_peer(
            format!("district-{}", spec.district),
            connection.clone(),
        );
        let link = PeerLink {
            district: spec.district,
            system_id: identity,
            endpoint: spec.endpoint.clone(),
            connection,
            outgoing,
        };
        links.push(link);
    }

    Ok((listener, links))
}

/// Answers identity and console queries itself and routes everything else to local actors.
fn request_handler(system: &ActorSystem) -> RemoteHandler {
    let local_handler = system.remote_handler();
    let local_system_id = system.id();
    #[cfg(feature = "console")]
    let console = system.console();
    Arc::new(move |peer_id, request: RemoteRequest| {
        if request.destination.path == IDENTITY_PATH {
            return RemoteResponse::replied(
                request.id,
                serde_json::json!({ "system_id": local_system_id }),
            );
        }
        #[cfg(feature = "console")]
        if request.destination.path == CONSOLE_PATH {
            return console_reply(&console, &request);
        }
        local_handler(peer_id, request)
    })
}

/// Runs a console request from a peer. Forwarding is refused so requests cannot bounce.
#[cfg(feature = "console")]
fn console_reply(console: &picard::console::Console, request: &RemoteRequest) -> RemoteResponse {
    let text = request.payload.as_str().unwrap_or_default();
    let forwarded = matches!(
        serde_json::from_str::<picard::console::Request>(text),
        Ok(picard::console::Request {
            command: picard::console::Command::Forward { .. },
            ..
        })
    );
    if forwarded {
        return RemoteResponse::rejected(
            request.id,
            picard::remoting::RemoteError::Rejected("console forwarding is not chained".into()),
        );
    }
    RemoteResponse::replied(request.id, Value::String(console.handle_text(text)))
}

fn discover_identity(
    mut send: impl FnMut() -> picard::remoting::RemoteFuture<RemoteResponse>,
    timeout: Duration,
) -> Result<Uuid, String> {
    let deadline = std::time::Instant::now() + timeout;
    let mut last_error = None;
    while std::time::Instant::now() < deadline {
        match futures::executor::block_on(send()) {
            Ok(response) if response.accepted => {
                let system_id = response
                    .payload
                    .and_then(|payload| payload.get("system_id").cloned())
                    .and_then(|value| serde_json::from_value::<Uuid>(value).ok());
                if let Some(system_id) = system_id {
                    return Ok(system_id);
                }
                return Err("remote identity endpoint returned an invalid system ID".into());
            }
            Ok(response) => {
                last_error = response.error.map(|err| err.to_string());
                break;
            }
            Err(err) => {
                last_error = Some(err.to_string());
                thread::sleep(Duration::from_millis(300));
            }
        }
    }
    Err(format!(
        "could not discover remote actor system identity: {}",
        last_error.unwrap_or_else(|| "connection timed out after 60 seconds".into())
    ))
}
