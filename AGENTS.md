# y-nostr agent notes

- Standalone provider; Nostr is signaling only. Yjs sync, awareness, and directed messages stay on WebRTC. It is intentionally not Trystero wire-compatible.
- WebRTC transport lives in `woohp/webrtc-data-peer`; keep provider identity, signaling, retries, and RTC defaults here.
- Use `@noble/secp256k1` only for BIP-340 keys/signatures; use Web Crypto for SHA-256 and AES-GCM. A peer ID is its ephemeral Nostr public key.
- Signaling protocol v2 uses fixed ephemeral kind `20000`, exactly one indexed `x` tag, short NIP-40 expiration, and encrypted signer/topic binding. Verify ID/signature before decrypting.
- Be a good public-relay citizen: no periodic announcements. Announce on connection/recovery, deduplicate across relays, and send responses/offers/answers through the relay known to overlap. Default relays are deterministically distributed by room.
- Room secrets use PBKDF2-SHA-256 with 210,000 iterations and domain-separated HKDF outputs; deterministic discovery still requires high-entropy passwords against offline guessing.
- Peer-key ordering selects the offer initiator to avoid glare. Preserve signaling protocol versioning.
- Browser-first, native WebRTC, non-trickle ICE. STUN is default; callers supply TURN when needed.
- Run `npm run format` and `npm run check`; keep relay tests offline. Use `examples/browser-smoke` for real relay/NAT validation.
- Keep this file updated only with important, durable, non-obvious decisions—not file listings or facts obvious from the code.
