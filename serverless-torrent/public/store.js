// IndexedDB-backed durable store. THIS is the "semi-stateless" trick: the
// serverless functions keep nothing, so the browser holds everything that must
// survive between invocations — verified piece bytes, the bitfield of what we
// have, the cached peer list, and our stable peer id. Each function call is fed
// exactly the slice of this state it needs and returns verifiable results.

const DB_NAME = "serverless-torrent";
// v2: pieces are stored as Blobs (browsers back stored Blobs with disk), so
// assembling multi-GB files no longer pins everything in memory. Old v1 values
// were raw ArrayBuffers; getPiece/assembly handle both shapes.
const DB_VERSION = 2;
const PIECES = "pieces";
const SESSIONS = "sessions";

function openDb() {
  return new Promise((resolve, reject) => {
    const req = indexedDB.open(DB_NAME, DB_VERSION);
    req.onupgradeneeded = () => {
      const db = req.result;
      if (!db.objectStoreNames.contains(PIECES)) db.createObjectStore(PIECES);
      if (!db.objectStoreNames.contains(SESSIONS)) db.createObjectStore(SESSIONS);
    };
    req.onsuccess = () => resolve(req.result);
    req.onerror = () => reject(req.error);
  });
}

function tx(db, store, mode) {
  return db.transaction(store, mode).objectStore(store);
}

function reqToPromise(req) {
  return new Promise((resolve, reject) => {
    req.onsuccess = () => resolve(req.result);
    req.onerror = () => reject(req.error);
  });
}

export class Store {
  constructor(db) {
    this.db = db;
  }
  static async open() {
    return new Store(await openDb());
  }

  async getSession(infoHash) {
    return reqToPromise(tx(this.db, SESSIONS, "readonly").get(infoHash));
  }

  async putSession(session) {
    return reqToPromise(tx(this.db, SESSIONS, "readwrite").put(session, session.infoHash));
  }

  async putPiece(infoHash, index, bytes) {
    // Store as a Blob so the browser can spill it to disk; assembly then slices
    // Blobs without ever materializing the whole file in memory.
    const blob = new Blob([bytes]);
    return reqToPromise(tx(this.db, PIECES, "readwrite").put(blob, `${infoHash}:${index}`));
  }

  // Returns the raw piece Blob (or null). Handles legacy v1 ArrayBuffer values.
  async getPieceBlob(infoHash, index) {
    const val = await reqToPromise(tx(this.db, PIECES, "readonly").get(`${infoHash}:${index}`));
    if (!val) return null;
    if (val instanceof Blob) return val;
    return new Blob([val]); // legacy ArrayBuffer
  }

  // Returns piece bytes (rarely needed now that assembly uses Blob slices).
  async getPiece(infoHash, index) {
    const blob = await this.getPieceBlob(infoHash, index);
    return blob ? new Uint8Array(await blob.arrayBuffer()) : null;
  }
}

// --- bitfield as a plain Uint8Array (MSB-first, matches the wire format) ---

export function emptyBitfield(numPieces) {
  return new Uint8Array(Math.ceil(numPieces / 8));
}
export function bitGet(bits, i) {
  return (bits[i >> 3] & (0x80 >> (i & 7))) !== 0;
}
export function bitSet(bits, i) {
  bits[i >> 3] |= 0x80 >> (i & 7);
}
export function countSet(bits, numPieces) {
  let n = 0;
  for (let i = 0; i < numPieces; i++) if (bitGet(bits, i)) n++;
  return n;
}
