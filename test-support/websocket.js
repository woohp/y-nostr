// Keep helpers outside test/: Node's default test discovery executes every JavaScript file there.
export const createWebSocketHarness = () => {
    const sockets = [];

    class HarnessWebSocket {
        static OPEN = 1;
        readyState = 0;
        listeners = new Map();
        sent = [];

        constructor(url) {
            this.url = url;
            sockets.push(this);
        }

        addEventListener(type, listener) {
            const listeners = this.listeners.get(type) ?? [];
            listeners.push(listener);
            this.listeners.set(type, listeners);
        }

        dispatch(type, event = {}) {
            for (const listener of this.listeners.get(type) ?? []) listener(event);
        }

        send(data) {
            this.sent.push(JSON.parse(data));
        }

        open() {
            this.readyState = HarnessWebSocket.OPEN;
            this.dispatch("open");
        }

        close() {
            this.readyState = 3;
            this.dispatch("close");
        }
    }

    return { WebSocket: HarnessWebSocket, sockets };
};
