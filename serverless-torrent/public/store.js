// IndexedDB-backed durable store. THIS is the "semi-stateless" trick: the
// serverless functions keep nothing, so the browser holds everything that must
// survive between invocations — verified piece bytes, the bitfield of what we
// have, the cached peer list, and our stable peer id. Each function call is fed
// exactly the slice of this state it needs and returns verifiable results.

const DB_NAME = "serverless-torrent";
const DB_VERSION = 1;
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
    // store the underlying ArrayBuffer for compactness
    return reqToPromise(
      tx(this.db, PIECES, "readwrite").put(bytes.buffer, `${infoHash}:${index}`),
    );
  }

  async getPiece(infoHash, index) {
    const buf = await reqToPromise(tx(this.db, PIECES, "readonly").get(`${infoHash}:${index}`));
    return buf ? new Uint8Array(buf) : null;
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
