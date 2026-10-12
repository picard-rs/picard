# Clustering roadmap

Picard does not yet provide remote actors or clustering. The first planned
deliverable is **native TCP remote actor messaging**: send messages to an actor
in another Picard process using an explicit remote address. UDP, cluster
membership and distributed data (including CRDTs) are out of scope for this
first step.

## Initial scope

The first implementation should be a small, opt-in vertical slice:

- Give actors an address that explicitly distinguishes local delivery from
  remote delivery. A remote address carries the peer endpoint and the actor's
  path on that peer.
- Deliver supported messages over TCP while keeping local `ActorRef` behavior
  unchanged.
- Authenticate peers with mutual TLS. After authentication, the receiving
  system resolves the requested path against its local actors and delivers the
  message to that actor.
- Use versioned, length-prefixed JSON framing for the first protocol.
- Keep actor addresses location-aware: a local address routes through the
  current system, while a remote address carries the peer endpoint and path.
- Carry sender addresses so actors can reply across peers.
- Make remote sends asynchronous and return a future that reports transport or
  mailbox-acceptance failures.
- Make connection failures and remote delivery failures observable to callers;
  do not present best-effort network delivery as guaranteed local delivery.
- Restrict the initial implementation to native targets. Browser/WASM transport
  needs separate design and is not part of this milestone.
- Do not claim transparent clustering, automatic discovery, failover, or
  exactly-once delivery.

## First-version API decisions

- Local and remote destinations will use the unified typed `ActorRef<Msg>` API,
  with the address identifying its location.
- The sender API will represent either local or remote senders. This changes
  public actor receive signatures and therefore requires a major-version
  compatibility plan.
- Mutual-TLS client and server configurations will be supplied by the
  application as rustls configuration objects; Picard will not load private
  keys from its own configuration files.
- Remote references support message delivery and replies. Hierarchy operations
  such as parent lookup, child listing, and stopping remain local-only in the
  first version and must fail explicitly when used on remote references.

## Decisions before implementation

Message-size limits, peer authorization policy, connection lifecycle, and the
exact mailbox-acceptance acknowledgment semantics still need to be specified.
The protocol versioning and framing rules should be documented and tested
before the format is treated as stable. A TCP send will not claim delivery
exactly once: disconnects may leave the sender uncertain whether a message was
accepted.

## Later work

UDP transport, cluster membership/discovery, vector-clock or other
conflict-resolution strategies, and CRDT-backed distributed data are separate
future milestones. Each needs its own reliability, consistency, and
security requirements before implementation.