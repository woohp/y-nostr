# y-nostr agent guide

y-nostr is a browser-first Yjs provider. Public Nostr relays carry only encrypted signaling; Yjs sync, awareness, and directed messages travel over native WebRTC data channels. It is a standalone protocol, intentionally not Trystero wire-compatible.

Favor simple, maintainable solutions for that scope: small rooms (`maxConns` defaults to 20), unreliable public relays, and peers that come and go. Do not complicate rare local races unless a concrete recurring failure justifies it. For example, a handshake that settles after `disconnect()` may resume or restart one retry episode in the next connection; that is acceptable. Protect wire compatibility, signaling security, and relay citizenship.

The project is 0.x and distributed from GitHub, so breaking API changes are acceptable. Bump the signaling protocol version whenever the wire format changes.

## Boundaries

- The public API is `NostrProvider`, its option/event types, `defaultRtcConfig`, and default relay selection. Keep crypto and Nostr event helpers internal; tests import them from `dist/*.js`.
- `crypto.ts` knows keys, room-secret derivation, and AES-GCM, but nothing about Nostr. `nostr-event.ts` owns the event wire format and freshness constants. `nostr-relay.ts` owns one socket: subscription, reconnect backoff, and verified event intake. `announcement-scheduler.ts` owns announcement retries and the open-relay set.
- WebRTC transport lives in `woohp/webrtc-data-peer`. Provider identity, signaling, retries, and RTC defaults stay here.
- Use `@noble/secp256k1` only for BIP-340 keys and signatures; use Web Crypto for SHA-256 and AES-GCM. A peer ID is its ephemeral Nostr public key.
- Non-trickle ICE, single-frame data-channel messages. STUN is the default; callers supply TURN. Fragmentation, outbound queueing, and large-document sync beyond one frame are non-goals.

## Signaling protocol

- Protocol v2 uses fixed ephemeral kind `20000`, exactly one indexed `x` tag, a short NIP-40 expiration, and encrypted signer/topic binding. Verify the event ID and signature before decrypting.
- Room secrets use PBKDF2-SHA-256 (210,000 iterations) with domain-separated HKDF outputs. Deterministic discovery still permits offline guessing, so private rooms need high-entropy passwords.
- Peer-key ordering picks the offer initiator to avoid glare.

## Relay citizenship

- Never announce periodically. Announce when a relay opens or reconnects, deduplicate across relays, and send responses, offers, and answers only through the relay known to overlap with the peer.
- Recovery is one bounded retry episode (three jittered attempts) after peer loss or a failed handshake. A handshake pauses retries. A failure before its signal is published resumes the existing episode, if one exists, so repeated failures cannot spam relays; a published handshake that later fails is a peer loss and starts a fresh episode. The first relay to open queues an episode, as a backstop for a dropped initial announcement, and losing the last relay cancels the current one.
- Default relays are deterministically distributed by room name.

## Connection lifecycle

- `connectionGeneration` changes on every connect and disconnect. Check liveness (`isLive(generation)`) at callback boundaries: relay and peer callbacks capture the generation they were created under, and methods that await capture the current generation on entry and recheck it afterward. Do not thread generation parameters through internal methods.
- `disconnect()` resets the scheduler, so the scheduler is deliberately unaware of generations.

## Validation

- Run `npm run format` and `npm run check`. Keep tests offline with fakes; use `examples/browser-smoke` for real relay and NAT validation.
- Assert outcomes such as announcements published, events emitted, and peers present. Do not add getters or state enums only for tests.
- Shared helpers live in `test-support/`, because Node's runner executes every file under `test/`. Wait with `waitFor`, never an unbounded loop. Mock only `setTimeout` for backoff timing.
- Keep this file to durable, non-obvious decisions, not file listings or facts obvious from the code.
