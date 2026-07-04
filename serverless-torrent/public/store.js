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

  async deletePiece(infoHash, index) {
    return reqToPromise(tx(this.db, PIECES, "readwrite").delete(`${infoHash}:${index}`));
  }

  // Purge every stored piece for one torrent. This is how a finished download
  // stops "overstaying its welcome": once the bytes are on the user's disk we
  // reclaim the IndexedDB space instead of holding a redundant copy forever.
  async deleteAllPieces(infoHash, numPieces) {
    const os = tx(this.db, PIECES, "readwrite");
    for (let i = 0; i < numPieces; i++) os.delete(`${infoHash}:${i}`);
    return txDone(os.transaction);
  }

  async deleteSession(infoHash) {
    return reqToPromise(tx(this.db, SESSIONS, "readwrite").delete(infoHash));
  }

  // Full removal: pieces + the session record (bitfield, peers, peer id).
  async clearTorrent(infoHash, numPieces) {
    await this.deleteAllPieces(infoHash, numPieces);
    await this.deleteSession(infoHash);
  }
}

// Wait for a whole readwrite transaction (not just one request) to commit.
function txDone(transaction) {
  return new Promise((resolve, reject) => {
    transaction.oncomplete = () => resolve();
    transaction.onerror = () => reject(transaction.error);
    transaction.onabort = () => reject(transaction.error);
  });
}

// --- storage quota / durability (StorageManager API) ---

// Ask the browser to make our storage persistent so a large in-progress
// download isn't silently evicted under storage pressure. Best-effort: returns
// the granted state, and false where the API is unavailable.
export async function requestPersistentStorage() {
  if (!navigator.storage?.persist) return false;
  if (await navigator.storage.persisted?.()) return true;
  try {
    return await navigator.storage.persist();
  } catch {
    return false;
  }
}

// { usage, quota } in bytes, or null when the API is unavailable.
export async function storageEstimate() {
  if (!navigator.storage?.estimate) return null;
  try {
    return await navigator.storage.estimate();
  } catch {
    return null;
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
