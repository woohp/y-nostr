import { schnorr } from "@noble/secp256k1";

const textEncoder = new TextEncoder();
const textDecoder = new TextDecoder();

export interface NostrIdentity {
    secretKey: Uint8Array;
    publicKey: string;
}

export const bytesToHex = (bytes: Uint8Array): string => {
    let result = "";
    for (const byte of bytes) result += byte.toString(16).padStart(2, "0");
    return result;
};

/** Random hex string of `byteLength` bytes, for offer and subscription identifiers. */
export const randomHex = (byteLength: number): string =>
    bytesToHex(crypto.getRandomValues(new Uint8Array(byteLength)));

export const hexToBytes = (hex: string): Uint8Array => {
    if (!/^[0-9a-f]*$/i.test(hex) || hex.length % 2 !== 0) throw new Error("Invalid hex");
    const result = new Uint8Array(hex.length / 2);
    for (let i = 0; i < result.length; i++) {
        result[i] = Number.parseInt(hex.slice(i * 2, i * 2 + 2), 16);
    }
    return result;
};

const passwordKdfIterations = 210000;

export interface RoomSecrets {
    roomTopic: string;
    encryptionKey: CryptoKey;
}

export const deriveRoomSecrets = async (roomName: string, password = ""): Promise<RoomSecrets> => {
    const roomEncoding = encodeFields("y-nostr", "v2", "room", roomName);
    const passwordKey = await crypto.subtle.importKey(
        "raw",
        textEncoder.encode(password),
        "PBKDF2",
        false,
        ["deriveBits"],
    );
    const masterKeyBytes = await crypto.subtle.deriveBits(
        {
            name: "PBKDF2",
            hash: "SHA-256",
            salt: roomEncoding as BufferSource,
            iterations: passwordKdfIterations,
        },
        passwordKey,
        256,
    );
    const masterKey = await crypto.subtle.importKey("raw", masterKeyBytes, "HKDF", false, [
        "deriveBits",
        "deriveKey",
    ]);
    const hkdf = (label: string): HkdfParams => ({
        name: "HKDF",
        hash: "SHA-256",
        salt: roomEncoding as BufferSource,
        info: encodeFields("y-nostr", "v2", label) as BufferSource,
    });
    const topicBytes = new Uint8Array(
        await crypto.subtle.deriveBits(hkdf("topic"), masterKey, 256),
    );
    const encryptionKey = await crypto.subtle.deriveKey(
        hkdf("aes-gcm"),
        masterKey,
        { name: "AES-GCM", length: 256 },
        false,
        ["encrypt", "decrypt"],
    );
    return { roomTopic: bytesToHex(topicBytes), encryptionKey };
};

export const createPeerTopic = async (roomTopic: string, peerId: string): Promise<string> =>
    bytesToHex(await sha256(encodeFields("y-nostr", "v2", "peer", roomTopic, peerId)));

export const createIdentity = (secretKey?: Uint8Array): NostrIdentity => {
    if (secretKey) {
        const copiedSecretKey = secretKey.slice();
        return {
            secretKey: copiedSecretKey,
            publicKey: bytesToHex(schnorr.getPublicKey(copiedSecretKey)),
        };
    }
    const identity = schnorr.keygen();
    return { secretKey: identity.secretKey, publicKey: bytesToHex(identity.publicKey) };
};

export const encrypt = async (key: CryptoKey, plaintext: string): Promise<string> => {
    const iv = crypto.getRandomValues(new Uint8Array(12));
    const ciphertext = new Uint8Array(
        await crypto.subtle.encrypt({ name: "AES-GCM", iv }, key, textEncoder.encode(plaintext)),
    );
    const packed = new Uint8Array(iv.length + ciphertext.length);
    packed.set(iv);
    packed.set(ciphertext, iv.length);
    return bytesToBase64(packed);
};

export const decrypt = async (key: CryptoKey, packed: string): Promise<string> => {
    const bytes = base64ToBytes(packed);
    if (bytes.length < 29) throw new Error("Invalid encrypted payload");
    const plaintext = await crypto.subtle.decrypt(
        { name: "AES-GCM", iv: bytes.subarray(0, 12) as Uint8Array<ArrayBuffer> },
        key,
        bytes.subarray(12) as Uint8Array<ArrayBuffer>,
    );
    return textDecoder.decode(plaintext);
};

export const sha256 = async (value: string | Uint8Array): Promise<Uint8Array> =>
    new Uint8Array(
        await crypto.subtle.digest(
            "SHA-256",
            (typeof value === "string" ? textEncoder.encode(value) : value) as BufferSource,
        ),
    );

const encodeFields = (...fields: string[]): Uint8Array => {
    const encoded = fields.map((field) => textEncoder.encode(field));
    const length = encoded.reduce((total, field) => total + 4 + field.length, 0);
    const result = new Uint8Array(length);
    const view = new DataView(result.buffer);
    let offset = 0;
    for (const field of encoded) {
        view.setUint32(offset, field.length);
        offset += 4;
        result.set(field, offset);
        offset += field.length;
    }
    return result;
};

const bytesToBase64 = (bytes: Uint8Array): string => {
    let binary = "";
    for (const byte of bytes) binary += String.fromCharCode(byte);
    return btoa(binary);
};

const base64ToBytes = (value: string): Uint8Array => {
    const binary = atob(value);
    const bytes = new Uint8Array(binary.length);
    for (let i = 0; i < binary.length; i++) bytes[i] = binary.charCodeAt(i);
    return bytes;
};
