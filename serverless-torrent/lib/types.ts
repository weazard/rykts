// Shared types for the serverless-torrent system.

export interface TorrentFile {
  path: string; // forward-slash joined path
  length: number;
  offset: number; // absolute byte offset within the concatenated torrent payload
}

export interface TorrentMeta {
  infoHash: string; // hex, 40 chars
  name: string;
  pieceLength: number;
  pieces: string[]; // hex SHA-1 per piece (40 chars each)
  totalLength: number;
  files: TorrentFile[];
  announce: string[]; // tracker URLs (http/https/udp)
}

export interface PeerAddr {
  ip: string;
  port: number;
}

// ---- /api/download contract ----

export interface DownloadRequest {
  infoHash: string; // hex
  pieceLength: number;
  totalLength: number;
  // The pieces the client still needs and is asking this invocation to fetch.
  wanted: { index: number; hash: string }[]; // hash = hex SHA-1 the client expects
  peers: PeerAddr[]; // client-cached peer list (avoids re-discovery per call)
  peerId?: string; // hex, 20 bytes; generated client-side and reused
  deadlineMs?: number; // soft budget; clamped server-side below the platform limit
}

// The response is no longer JSON: pieces stream back as binary frames followed
// by a summary frame — see ./frames.ts for the wire format and summary type.

export interface PeerHealth {
  peer: PeerAddr;
  connected: boolean;
  unchoked: boolean;
  piecesServed: number;
  error?: string;
}

// ---- /api/announce contract ----

export interface AnnounceRequest {
  infoHash: string; // hex
  // Tracker URLs. May be empty: DHT alone can discover peers (magnet links
  // often carry no trackers at all).
  announce: string[];
  peerId?: string; // hex
  port?: number;
  left?: number;
  downloaded?: number;
  uploaded?: number;
  numWant?: number;
  // Skip the DHT lookup (default false — DHT runs in parallel with trackers).
  noDht?: boolean;
}

export interface AnnounceResponse {
  peers: PeerAddr[];
  trackers: { url: string; ok: boolean; peerCount: number; error?: string }[];
  dht: { ok: boolean; peerCount: number; nodesQueried: number; error?: string };
}

// ---- /api/metadata contract (BEP 9 magnet support) ----

export interface MetadataRequest {
  infoHash: string; // hex, 40 chars
  announce?: string[]; // trackers from the magnet's `tr` params, if any
  peers?: PeerAddr[]; // optional pre-known peers (skips discovery)
  peerId?: string; // hex
}

export interface MetadataResponse {
  // Bencoded info dict, base64. Small (KBs) and one-shot, so JSON is fine here.
  infoBase64: string;
  name: string;
  totalLength: number;
  pieceCount: number;
  peers: PeerAddr[]; // peers discovered along the way, seed for the download loop
}
