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

export interface DownloadedPiece {
  index: number;
  data: string; // base64 of the raw piece bytes
}

export interface PeerHealth {
  peer: PeerAddr;
  connected: boolean;
  unchoked: boolean;
  piecesServed: number;
  error?: string;
}

export interface DownloadResponse {
  pieces: DownloadedPiece[]; // complete pieces fetched this invocation
  peerHealth: PeerHealth[]; // so the client can prune/keep peers in its cache
  elapsedMs: number;
  hitDeadline: boolean;
}

// ---- /api/announce contract ----

export interface AnnounceRequest {
  infoHash: string; // hex
  announce: string[]; // tracker URLs
  peerId?: string; // hex
  port?: number;
  left?: number;
  downloaded?: number;
  uploaded?: number;
  numWant?: number;
}

export interface AnnounceResponse {
  peers: PeerAddr[];
  trackers: { url: string; ok: boolean; peerCount: number; error?: string }[];
}
