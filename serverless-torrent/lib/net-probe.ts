// UDP egress reachability probe.
//
// Some environments (sandboxes, corporate networks, certain hosts) silently
// drop all outbound UDP. DHT (BEP-5) and UDP trackers (BEP-15) are UDP-only,
// so when UDP is dead we want to fail FAST with a clear reason instead of
// burning multi-second timeouts per lookup and reporting a vague "no peers".
//
// The probe sends a tiny DNS query to two public resolvers; any reply proves
// UDP round-trips work. The result is cached for the process lifetime (a
// serverless invocation or dev-server run), so it costs at most ~1.2s once.

import dgram from "node:dgram";

const PROBE_TIMEOUT_MS = 1200;
// Public DNS resolvers: virtually always reachable when UDP egress works.
const PROBE_TARGETS: { host: string; port: number }[] = [
  { host: "8.8.8.8", port: 53 },
  { host: "1.1.1.1", port: 53 },
];

// DNS query for "example.com" A record (fixed id 0xabcd, RD set).
const DNS_QUERY = Uint8Array.from(
  Buffer.from("abcd01000001000000000000076578616d706c6503636f6d0000010001", "hex"),
);

let cached: Promise<boolean> | null = null;

// Resolves true when outbound UDP works, false when it appears blocked.
export function udpAvailable(): Promise<boolean> {
  if (!cached) cached = probe();
  return cached;
}

function probe(): Promise<boolean> {
  return new Promise((resolve) => {
    let sock: dgram.Socket;
    try {
      sock = dgram.createSocket("udp4");
    } catch {
      resolve(false);
      return;
    }
    let done = false;
    const finish = (ok: boolean) => {
      if (done) return;
      done = true;
      clearTimeout(timer);
      try {
        sock.close();
      } catch {
        // already closed
      }
      resolve(ok);
    };
    const timer = setTimeout(() => finish(false), PROBE_TIMEOUT_MS);
    sock.on("message", () => finish(true));
    sock.on("error", () => finish(false));
    for (const t of PROBE_TARGETS) {
      try {
        sock.send(DNS_QUERY, t.port, t.host);
      } catch {
        // ignore, the timer handles total failure
      }
    }
  });
}
