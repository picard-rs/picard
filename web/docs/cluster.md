# Remote messaging

Picard's opt-in `remoting` feature provides versioned JSON messaging over an
authenticated WebSocket connection. The built-in transport uses WebSocket over
TCP, so a trusted reverse proxy can terminate TLS. Picard still authenticates
each connecting peer with a bearer token; TLS termination does not replace peer
authentication.

This is remote message delivery, not transparent clustering. There is no
discovery, failover, exactly-once guarantee, or automatic remote-reference
conversion. UDP and distributed data (including CRDTs) are out of scope.

## Enable remote messaging

Enable the feature in the application:

```toml
picard = { version = "0.4", features = ["remoting"] }
```

The feature enables Picard's `serde` message bound. Actor messages delivered
remotely must implement `Serialize` and `DeserializeOwned`.

Create a receiver actor and start its authenticated WebSocket listener:

```rust,ignore
let listener = system.listen_remote_actors(
    "127.0.0.1:9000".parse()?,
    PeerAuthenticator::new([("orders-client".into(), token.into())]),
)?;
```

The listener binds plain HTTP. Bind it to a private interface or loopback when
a trusted reverse proxy terminates TLS. The proxy must prevent untrusted
clients from reaching the backend listener directly.

Connect and deliver a serialized message:

```rust,ignore
let peer = RemotePeer::new(
    "orders",
    "wss://actors.example.test/remoting",
    token,
)?;
let remote = system.connect_remote(peer);
let address = ActorAddress::remote(
    remote_system_id,
    "wss://actors.example.test/remoting",
    "/user/orders",
);
let ack = remote
    .send(address, None, serde_json::to_value(&message)?)
    .await?;
```

`listen_remote_actors` resolves the address's system ID and actor path against
the local actor registry, deserializes the payload using that actor's message
type, and acknowledges the request after it enters the mailbox. Acknowledgment
does not mean the actor has processed the message. Unknown actors, invalid
payloads, unsupported protocol versions, failed authentication, and transport
errors are reported to the caller.

Use `RemoteConnection::ask` when the actor must reply through the usual
`Sender`:

```rust,ignore
let reply: String = remote
    .ask(address, request, std::time::Duration::from_secs(2))
    .await?;
```

The destination actor receives a temporary `BasicActorRef` as its `Sender`;
the first serializable message it sends through that reference is returned to
the caller. This temporary reference supports typed `try_tell` replies; it
does not support watches, system messages, or type-erased `try_tell_any`. A
reply received after the timeout is rejected. If the actor drops its sender
without replying, the call returns `RemoteError::NoReply`; if no reply arrives
before the deadline, it returns `RemoteError::Timeout`. A connection or peer
failure is reported as `RemoteError::Transport`. The ask timeout bounds the
remote actor's reply wait after the request reaches its system; transport
setup failures are reported separately. Timeouts are limited to 60 seconds to
bound the time a listener worker waits on any one request.

`send` remains fire-and-forget with a mailbox-acceptance acknowledgment.
Application-defined exchanges may also use `listen_remote` with a custom
`RemoteHandler`, which can return a `RemoteResponse` payload.

## Custom transports

Applications can implement `RemoteTransport` to use their own communication
system. A transport must support both `send` and `listen`, authenticate peers
before dispatch, enforce the protocol frame-size limit, and preserve request
correlation IDs. Use `ActorSystem::connect_remote_with` and
`ActorSystem::listen_remote_with` to install it; inbound requests use the same
handler interface as the built-in WebSocket transport.

## Protocol and security boundaries

- Protocol version 2 uses JSON messages and limits frames to 1 MiB.
- Peer bearer tokens are sent in the HTTP upgrade `Authorization` header, not
  in URLs, and are redacted from `Debug` output.
- Use `wss://` or a private network for transport confidentiality and integrity.
  When a proxy terminates TLS, trust and secure the proxy-to-Picard link.
- Allow-listed peers may address any actor registered for remote delivery.
  Authorization by actor path or message type is not yet configurable.
- A disconnect can leave the caller uncertain whether the remote mailbox
  accepted a message. The protocol does not promise exactly-once delivery.
- The built-in transport opens a WebSocket connection per send. Connection
  pooling, retries, discovery, and remote peer lifecycle management are future
  work.

WebSockets are a pragmatic fit when deployments already use HTTP reverse
proxies for TLS termination. Compared with a raw TCP protocol, they provide
standard proxy traversal and HTTP-level peer authentication headers, at the
cost of an HTTP upgrade and WebSocket framing. The proxy is part of the trusted
computing base; Picard's bearer-token check remains mandatory regardless.
