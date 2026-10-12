# Clustering roadmap

Picard does not yet provide remote actors or clustering. The first planned
deliverable is **native TCP remote actor messaging**: send messages to an actor
in another Picard process using an explicit remote address. UDP, cluster
membership and distributed data (including CRDTs) are out of scope for this
first step.

## Initial scope

The first implementation should be a small, opt-in vertical slice:

- Define an address for a remote actor using a node endpoint and actor path.
- Deliver supported messages over TCP while keeping local `ActorRef` behavior
  unchanged.
- Make connection failures and remote delivery failures observable to callers;
  do not present best-effort network delivery as guaranteed local delivery.
- Restrict the initial implementation to native targets. Browser/WASM transport
  needs separate design and is not part of this milestone.
- Do not claim transparent clustering, automatic discovery, failover, or
  exactly-once delivery.

## Decisions before implementation

The wire format, compatibility/versioning policy, authentication and
authorization, message-size limits, connection lifecycle, and remote sender
semantics still need to be specified. These choices should be documented and
tested before the protocol is treated as stable. The local actor API should not
be changed until a working prototype demonstrates which abstractions can be
shared safely.

## Later work

UDP transport, cluster membership/discovery, vector-clock or other
conflict-resolution strategies, and CRDT-backed distributed data are separate
future milestones. Each needs its own reliability, consistency, and
security requirements before implementation.