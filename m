const http = require("http");
const { WebSocketServer, WebSocket } = require("ws");

const UPSTREAM_HOST = "mc.ricenetwork.xyz";
const UPSTREAM_PORT = 443;
const UPSTREAM_PROTOCOL = "wss";
const PORT = process.env.PORT || 3000;

const HOP_BY_HOP = new Set([
  "connection", "keep-alive", "proxy-authenticate", "proxy-authorization",
  "te", "trailers", "transfer-encoding", "upgrade",
  "sec-websocket-key", "sec-websocket-version", "sec-websocket-extensions",
  "sec-websocket-accept", "host",
]);

const server = http.createServer((req, res) => {
  res.writeHead(200, { "Content-Type": "text/plain" });
  res.end("RiceNetwork Proxy");
});

const wss = new WebSocketServer({ server, perMessageDeflate: false });

wss.on("connection", (clientSocket, req) => {
  const ip = req.socket.remoteAddress;
  const protocols = (req.headers["sec-websocket-protocol"] ?? "")
    .split(",").map((s) => s.trim()).filter(Boolean);

  const forwardHeaders = { Host: UPSTREAM_HOST };
  for (const [key, value] of Object.entries(req.headers)) {
    if (!HOP_BY_HOP.has(key.toLowerCase()) && typeof value === "string") {
      forwardHeaders[key] = value;
    }
  }

  const upstream = new WebSocket(
    `${UPSTREAM_PROTOCOL}://${UPSTREAM_HOST}:${UPSTREAM_PORT}/`,
    protocols.length ? protocols : undefined,
    { headers: forwardHeaders, perMessageDeflate: false, followRedirects: true },
  );

  const pending = [];

  upstream.on("open", () => {
    for (const msg of pending) upstream.send(msg.data, { binary: msg.isBinary });
    pending.length = 0;
  });

  clientSocket.on("message", (data, isBinary) => {
    if (upstream.readyState === WebSocket.OPEN) {
      upstream.send(data, { binary: isBinary });
    } else {
      pending.push({ data, isBinary });
    }
  });

  upstream.on("message", (data, isBinary) => {
    if (clientSocket.readyState === WebSocket.OPEN) {
      clientSocket.send(data, { binary: isBinary });
    }
  });

  const safeCode = (code) =>
    code >= 1000 && ![1004, 1005, 1006].includes(code) ? code : 1000;

  clientSocket.on("close", (code) => {
    if (upstream.readyState <= WebSocket.OPEN) upstream.close(safeCode(code));
  });

  upstream.on("close", (code) => {
    if (clientSocket.readyState <= WebSocket.OPEN) clientSocket.close(safeCode(code));
  });

  clientSocket.on("error", () => upstream.terminate());
  upstream.on("error", (err) => {
    if (clientSocket.readyState === WebSocket.OPEN) clientSocket.close(1011);
  });
});

server.listen(PORT, () => {
  console.log(`Proxy running on port ${PORT}`);
});
