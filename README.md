# y-nostr

A Yjs provider that uses Nostr for WebRTC signaling.

The project is currently distributed from GitHub; npm publication is not yet supported. For a source checkout:

```sh
git clone https://github.com/woohp/y-nostr.git
cd y-nostr
npm install
```

To consume the GitHub repository directly with npm 12, the consuming project must allow the transitive Git dependency:

```sh
npm config set allow-git all --location=project
npm install git+https://github.com/woohp/y-nostr.git#master
```

```js
import * as Y from "yjs";
import { NostrProvider } from "y-nostr";

const doc = new Y.Doc();
const provider = new NostrProvider("my-room", doc);
```

Use a password for private rooms:

```js
const provider = new NostrProvider("my-room", doc, {
    password: "a-high-entropy-random-shared-secret",
});
```

Or bring your own relays:

```js
const provider = new NostrProvider("my-room", doc, {
    relays: ["wss://nos.lol", "wss://relay.damus.io"],
});
```

The relays only see encrypted signaling. Document updates go over WebRTC. Passwords use PBKDF2-SHA-256 (210,000 iterations) and domain-separated HKDF keys, but deterministic discovery still permits offline guessing. Use a high-entropy shared secret when confidentiality matters; an empty password provides no secrecy.

`provider.synced` means Yjs Sync Step 2 completed with at least one currently connected peer. It does not mean every peer in the room has globally converged.

`provider.peerIds` is a readonly snapshot of the currently connected peer IDs. The `peers` event emits the same snapshot when membership changes.

`sendToPeer(peerId, data)` sends transient binary data to one connected peer. It returns `false` when the peer is missing or disconnected, the frame or buffering limit is exceeded, or the browser rejects the send. Transport failures are also reported through `peer-error`; directed messages are not persisted or retried.

When using the public crypto helpers, prefer `deriveRoomSecrets(roomName, password)` if both the room topic and encryption key are needed. Calling `createRoomTopic()` and `deriveEncryptionKey()` separately performs the password KDF twice.

Signaling protocol v2 encodes fields as UTF-8 strings prefixed by unsigned 32-bit big-endian lengths. It applies PBKDF2-SHA-256 to the password with 210,000 iterations and salt `["y-nostr", "v2", "room", roomName]`, then uses HKDF-SHA-256 with that salt and the labels `["y-nostr", "v2", "topic"]` and `["y-nostr", "v2", "aes-gcm"]`. Encrypted signaling includes and verifies the event signer and its single `x` topic.

Call `provider.destroy()` when you're done. `disconnect()` followed by `connect()` is supported. Pass `rtcConfig` if you need your own STUN/TURN servers, or `relayConnectTimeout` to change the 10-second relay opening timeout.

The remaining timeouts are `iceGatheringTimeout` (5 seconds to gather ICE candidates before sending a non-trickle offer or answer) and `handshakeTimeout` (15 seconds for a peer to finish connecting after its offer or answer is published, after which the attempt is abandoned and recovery is scheduled). `channelLabel` sets the data-channel label, `y-nostr` by default.

WebRTC messages are sent as single data-channel frames. The provider uses the negotiated SCTP `maxMessageSize`, falling back to 256 KiB when unavailable, and rejects sends that would raise `bufferedAmount` above 4 MiB. Override these safeguards with `fallbackMaxMessageSize` and `maxBufferedAmount`. Fragmentation, large-document synchronization beyond the single-frame limit, sustained burst traffic, and outbound queueing are intentionally unsupported.

```sh
npm run check
npm run dev
```
