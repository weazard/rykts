// Local dev server for the v0 preview / local runs.
//
// `vercel dev` needs interactive project linking and auth, which doesn't work
// in a headless sandbox. Instead this tiny server reproduces exactly what
// Vercel does in production for THIS project: serve the static `public/`
// directory and route `/api/<name>` to `api/<name>.ts`'s default export.
//
// The API handlers only touch a small slice of the Vercel req/res surface
// (`req.method`, `req.body`, `res.status().json()`, `res.setHeader`,
// `res.write`, `res.once('drain')`, `res.end`). We adapt Node's native
// IncomingMessage/ServerResponse to that shape so the same handler code runs
// here and on Vercel unchanged.
//
// Run with: npm run dev   (node 22+/24 strips the TS types natively)

import { createServer, type IncomingMessage, type ServerResponse } from "node:http";
import { readFile, stat } from "node:fs/promises";
import { extname, join, normalize } from "node:path";
import { fileURLToPath } from "node:url";

const __dirname = fileURLToPath(new URL(".", import.meta.url));
const PUBLIC_DIR = join(__dirname, "public");
const PORT = Number(process.env.PORT ?? 3000);

const MIME: Record<string, string> = {
  ".html": "text/html; charset=utf-8",
  ".js": "text/javascript; charset=utf-8",
  ".mjs": "text/javascript; charset=utf-8",
  ".css": "text/css; charset=utf-8",
  ".json": "application/json; charset=utf-8",
  ".svg": "image/svg+xml",
  ".ico": "image/x-icon",
  ".png": "image/png",
  ".map": "application/json; charset=utf-8",
};

// Route -> lazily imported handler module. Mirrors Vercel's file-based routing.
const API_ROUTES: Record<string, string> = {
  "/api/download": "./api/download.ts",
  "/api/announce": "./api/announce.ts",
  "/api/metadata": "./api/metadata.ts",
};

async function readBody(req: IncomingMessage): Promise<string> {
  const chunks: Buffer[] = [];
  for await (const c of req) chunks.push(c as Buffer);
  return Buffer.concat(chunks).toString("utf8");
}

// Adapt Node's ServerResponse to the subset of VercelResponse the handlers use.
function adaptRes(res: ServerResponse) {
  const vres = res as ServerResponse & {
    status: (code: number) => typeof vres;
    json: (body: unknown) => void;
    send: (body: string) => void;
  };
  vres.status = (code: number) => {
    res.statusCode = code;
    return vres;
  };
  vres.json = (body: unknown) => {
    if (!res.headersSent) res.setHeader("content-type", "application/json; charset=utf-8");
    res.end(JSON.stringify(body));
  };
  vres.send = (body: string) => res.end(body);
  return vres;
}

async function handleApi(route: string, req: IncomingMessage, res: ServerResponse): Promise<void> {
  const mod = await import(API_ROUTES[route]);
  const handler = mod.default as (rq: unknown, rs: unknown) => Promise<void>;

  const raw = req.method === "GET" || req.method === "HEAD" ? "" : await readBody(req);
  const vreq = req as IncomingMessage & { body: unknown; query: Record<string, string> };
  vreq.body = raw; // handlers JSON.parse strings themselves
  vreq.query = {};

  await handler(vreq, adaptRes(res));
}

async function serveStatic(pathname: string, res: ServerResponse): Promise<void> {
  // Normalize and confine to PUBLIC_DIR (no path traversal).
  const rel = normalize(pathname).replace(/^(\.\.[/\\])+/, "");
  let filePath = join(PUBLIC_DIR, rel === "/" || rel === "" ? "index.html" : rel);

  try {
    const s = await stat(filePath);
    if (s.isDirectory()) filePath = join(filePath, "index.html");
  } catch {
    res.statusCode = 404;
    res.setHeader("content-type", "text/plain; charset=utf-8");
    res.end("404 Not Found");
    return;
  }

  try {
    const data = await readFile(filePath);
    res.statusCode = 200;
    res.setHeader("content-type", MIME[extname(filePath)] ?? "application/octet-stream");
    res.setHeader("cache-control", "no-store");
    res.end(data);
  } catch {
    res.statusCode = 500;
    res.end("500 Internal Server Error");
  }
}

const server = createServer((req, res) => {
  const url = new URL(req.url ?? "/", `http://${req.headers.host ?? "localhost"}`);
  const route = url.pathname;

  const job =
    route in API_ROUTES ? handleApi(route, req, res) : serveStatic(route, res);

  job.catch((e) => {
    console.error("[dev-server] error:", e);
    if (!res.headersSent) res.statusCode = 500;
    if (!res.writableEnded) res.end("500 Internal Server Error");
  });
});

server.listen(PORT, () => {
  console.log(`[dev-server] serving public/ + /api on http://localhost:${PORT}`);
});
