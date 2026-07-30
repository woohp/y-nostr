import { schnorr } from "@noble/secp256k1";
import { bytesToHex, hexToBytes, sha256, type NostrIdentity } from "./crypto.js";

/**
 * Signaling rides on ephemeral events so relays are not asked to store anything, and carries a
 * single indexed topic tag so one subscription filter can match a room.
 */
export const nostrEventKind = 20000;
export const nostrTopicTag = "x";
export const signalExpirationSeconds = 120;

export interface NostrEvent {
    id: string;
    pubkey: string;
    created_at: number;
    kind: number;
    tags: string[][];
    content: string;
    sig: string;
}

const serializeEvent = (event: Omit<NostrEvent, "id" | "sig">): string =>
    JSON.stringify([0, event.pubkey, event.created_at, event.kind, event.tags, event.content]);

export const createEvent = async (
    topic: string,
    content: string,
    identity: NostrIdentity,
    createdAt: number = Math.floor(Date.now() / 1000),
): Promise<NostrEvent> => {
    const unsigned = {
        pubkey: identity.publicKey,
        created_at: createdAt,
        kind: nostrEventKind,
        tags: [
            [nostrTopicTag, topic],
            ["expiration", String(createdAt + signalExpirationSeconds)],
        ],
        content,
    };
    const idBytes = await sha256(serializeEvent(unsigned));
    return {
        ...unsigned,
        id: bytesToHex(idBytes),
        sig: bytesToHex(await schnorr.signAsync(idBytes, identity.secretKey)),
    };
};

export const verifyEvent = async (event: NostrEvent): Promise<boolean> => {
    try {
        if (
            !/^[0-9a-f]{64}$/.test(event.id) ||
            !/^[0-9a-f]{64}$/.test(event.pubkey) ||
            !/^[0-9a-f]{128}$/.test(event.sig) ||
            !Number.isInteger(event.created_at) ||
            event.kind !== nostrEventKind ||
            !Array.isArray(event.tags) ||
            typeof event.content !== "string"
        ) {
            return false;
        }
        const idBytes = await sha256(
            serializeEvent({
                pubkey: event.pubkey,
                created_at: event.created_at,
                kind: event.kind,
                tags: event.tags,
                content: event.content,
            }),
        );
        if (bytesToHex(idBytes) !== event.id) return false;
        return await schnorr.verifyAsync(hexToBytes(event.sig), idBytes, hexToBytes(event.pubkey));
    } catch {
        return false;
    }
};

export const isNostrEvent = (value: unknown): value is NostrEvent => {
    if (!value || typeof value !== "object") return false;
    const event = value as Record<string, unknown>;
    return (
        typeof event["id"] === "string" &&
        typeof event["pubkey"] === "string" &&
        typeof event["created_at"] === "number" &&
        typeof event["kind"] === "number" &&
        Array.isArray(event["tags"]) &&
        event["tags"].every(
            (tag) => Array.isArray(tag) && tag.every((value) => typeof value === "string"),
        ) &&
        typeof event["content"] === "string" &&
        typeof event["sig"] === "string"
    );
};

/** Null unless the event carries exactly one topic tag, so routing is never ambiguous. */
export const getEventTopic = (event: NostrEvent): string | null => {
    const topicTags = event.tags.filter((tag) => tag[0] === nostrTopicTag);
    return topicTags.length === 1 && topicTags[0]?.length === 2 ? topicTags[0][1]! : null;
};

export const hasValidSignalExpiration = (event: NostrEvent): boolean => {
    const tags = event.tags.filter((tag) => tag[0] === "expiration");
    if (tags.length !== 1 || tags[0]!.length !== 2) return false;
    const value = tags[0]![1]!;
    if (!/^(0|[1-9]\d*)$/.test(value)) return false;
    const expiration = Number(value);
    return (
        Number.isSafeInteger(expiration) &&
        expiration === event.created_at + signalExpirationSeconds
    );
};
