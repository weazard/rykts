// Container prober: ffprobe-lite in pure JS, running wherever the engine
// runs (Service Worker or page). Reads the minimum necessary bytes of a
// media file through a range-reader and reports the container format,
// duration, and per-track codecs — the exact shape stremio-video expects
// back from GET /hlsv2/probe:
//
//   { format: { name, duration }, streams: [{ track, codec, channels? }] }
//
// Codec/format names mirror ffprobe's so stremio-web's capability matching
// (probe.format.name.indexOf(format) !== -1, codec allowlists) works
// unchanged. Supports the two containers that matter for torrents: MP4
// (ISO-BMFF) and Matroska/WebM.
//
// The range-reader signature is: read(start, endExclusive) -> Uint8Array.

// --- MP4 (ISO base media file format) ---

const MP4_VIDEO_CODECS = {
  avc1: "h264",
  avc3: "h264",
  hvc1: "hevc",
  hev1: "hevc",
  vp09: "vp9",
  av01: "av1",
  mp4v: "mpeg4",
};

const MP4_AUDIO_CODECS = {
  mp4a: "aac",
  "ac-3": "ac3",
  "ec-3": "eac3",
  opus: "opus",
  fLaC: "flac",
  ".mp3": "mp3",
};

function u32(b, o) {
  return (b[o] << 24 | b[o + 1] << 16 | b[o + 2] << 8 | b[o + 3]) >>> 0;
}

function u64(b, o) {
  return u32(b, o) * 0x100000000 + u32(b, o + 4);
}

function fourcc(b, o) {
  return String.fromCharCode(b[o], b[o + 1], b[o + 2], b[o + 3]);
}

// Walk top-level boxes to find `moov`, wherever it lives (faststart puts it
// up front; default mp4 muxing puts it after mdat, i.e. at the tail).
async function findMoov(read, fileLength) {
  let offset = 0;
  for (let hops = 0; hops < 64 && offset + 16 <= fileLength; hops++) {
    const head = await read(offset, Math.min(offset + 16, fileLength));
    let size = u32(head, 0);
    const type = fourcc(head, 4);
    let headerLen = 8;
    if (size === 1) {
      size = u64(head, 8);
      headerLen = 16;
    } else if (size === 0) {
      size = fileLength - offset;
    }
    if (size < headerLen) break;
    if (type === "moov") {
      return await read(offset + headerLen, Math.min(offset + size, fileLength));
    }
    offset += size;
  }
  return null;
}

// Depth-first scan of a box payload for child boxes.
function* boxes(buf, start = 0, end = buf.length) {
  let o = start;
  while (o + 8 <= end) {
    let size = u32(buf, o);
    const type = fourcc(buf, o + 4);
    let headerLen = 8;
    if (size === 1 && o + 16 <= end) {
      size = u64(buf, o + 8);
      headerLen = 16;
    } else if (size === 0) {
      size = end - o;
    }
    if (size < headerLen || o + size > end) return;
    yield { type, start: o + headerLen, end: o + size };
    o += size;
  }
}

function findBox(buf, path, start = 0, end = buf.length) {
  const [head, ...rest] = path;
  for (const b of boxes(buf, start, end)) {
    if (b.type !== head) continue;
    if (rest.length === 0) return b;
    const found = findBox(buf, rest, b.start, b.end);
    if (found) return found;
  }
  return null;
}

function parseMp4Moov(moov) {
  const streams = [];
  let duration = null;

  const mvhd = findBox(moov, ["mvhd"]);
  if (mvhd) {
    const version = moov[mvhd.start];
    if (version === 1) {
      const timescale = u32(moov, mvhd.start + 20);
      const dur = u64(moov, mvhd.start + 24);
      if (timescale > 0) duration = dur / timescale;
    } else {
      const timescale = u32(moov, mvhd.start + 12);
      const dur = u32(moov, mvhd.start + 16);
      if (timescale > 0) duration = dur / timescale;
    }
  }

  for (const trak of boxes(moov)) {
    if (trak.type !== "trak") continue;
    const hdlr = findBox(moov, ["mdia", "hdlr"], trak.start, trak.end);
    if (!hdlr) continue;
    const handler = fourcc(moov, hdlr.start + 8);
    const stsd = findBox(moov, ["mdia", "minf", "stbl", "stsd"], trak.start, trak.end);
    if (!stsd) continue;
    // stsd payload: version/flags (4) + entry_count (4) + first sample entry.
    const entryStart = stsd.start + 8;
    if (entryStart + 8 > stsd.end) continue;
    const entryType = fourcc(moov, entryStart + 4);

    if (handler === "vide") {
      streams.push({ track: "video", codec: MP4_VIDEO_CODECS[entryType] ?? entryType });
    } else if (handler === "soun") {
      // AudioSampleEntry: 8 header + 8 reserved + 2 channelcount at +24.
      const channels = entryStart + 26 <= stsd.end ? (moov[entryStart + 24] << 8) | moov[entryStart + 25] : 2;
      streams.push({ track: "audio", codec: MP4_AUDIO_CODECS[entryType] ?? entryType, channels });
    } else if (handler === "text" || handler === "sbtl" || handler === "subt") {
      streams.push({ track: "subtitle", codec: entryType });
    }
  }

  return { format: { name: "mov,mp4,m4a,3gp,3g2,mj2", duration }, streams };
}

// --- Matroska / WebM (EBML) ---

const MKV_CODEC_IDS = [
  ["V_MPEG4/ISO/AVC", "h264"],
  ["V_MPEGH/ISO/HEVC", "hevc"],
  ["V_VP9", "vp9"],
  ["V_VP8", "vp8"],
  ["V_AV1", "av1"],
  ["V_MPEG4", "mpeg4"],
  ["A_AAC", "aac"],
  ["A_AC3", "ac3"],
  ["A_EAC3", "eac3"],
  ["A_DTS", "dts"],
  ["A_TRUEHD", "truehd"],
  ["A_MLP", "mlp"],
  ["A_OPUS", "opus"],
  ["A_VORBIS", "vorbis"],
  ["A_FLAC", "flac"],
  ["A_MPEG/L3", "mp3"],
  ["A_PCM", "pcm"],
  ["S_TEXT/UTF8", "subrip"],
  ["S_TEXT/ASS", "ass"],
  ["S_TEXT/SSA", "ssa"],
  ["S_HDMV/PGS", "hdmv_pgs_subtitle"],
  ["S_VOBSUB", "dvd_subtitle"],
];

function mkvCodec(codecId) {
  for (const [prefix, name] of MKV_CODEC_IDS) {
    if (codecId.startsWith(prefix)) return name;
  }
  return codecId.toLowerCase();
}

// Read one EBML vint at `o`. Returns { value, length } or null.
// `keepMask` = true for element IDs (marker bit kept), false for sizes.
function vint(b, o, keepMask) {
  if (o >= b.length) return null;
  const first = b[o];
  if (first === 0) return null;
  let length = 1;
  for (let mask = 0x80; !(first & mask); mask >>= 1) length++;
  if (o + length > b.length) return null;
  let value = keepMask ? first : first & (0xff >> length);
  for (let i = 1; i < length; i++) value = value * 256 + b[o + i];
  return { value, length };
}

function ebmlFloat(b, start, size) {
  const view = new DataView(b.buffer, b.byteOffset + start, size);
  if (size === 4) return view.getFloat32(0);
  if (size === 8) return view.getFloat64(0);
  return null;
}

function ebmlUint(b, start, size) {
  let v = 0;
  for (let i = 0; i < size; i++) v = v * 256 + b[start + i];
  return v;
}

// IDs we care about (with marker bit, as they appear in the stream).
const MKV = {
  Segment: 0x18538067,
  Info: 0x1549a966,
  Tracks: 0x1654ae6b,
  TrackEntry: 0xae,
  TrackType: 0x83,
  CodecID: 0x86,
  Audio: 0xe1,
  Channels: 0x9f,
  TimestampScale: 0x2ad7b1,
  Duration: 0x4489,
  SeekHead: 0x114d9b74,
  Cluster: 0x1f43b675,
};

// Parse children of a master element occupying buf[start..end).
function* ebmlChildren(buf, start, end) {
  let o = start;
  while (o < end) {
    const id = vint(buf, o, true);
    if (!id) return;
    const size = vint(buf, o + id.length, false);
    if (!size) return;
    const dataStart = o + id.length + size.length;
    // "unknown size" (all value bits set) — treat as extending to end.
    const dataEnd = Math.min(end, dataStart + size.value);
    yield { id: id.value, start: dataStart, end: dataEnd };
    if (dataEnd <= o) return;
    o = dataEnd;
  }
}

async function parseMkv(read, fileLength) {
  // Info and Tracks sit near the head in practice; 2 MiB covers virtually
  // every real file (they precede the first Cluster).
  const budget = Math.min(fileLength, 2 * 1024 * 1024);
  const head = await read(0, budget);

  // Find the Segment element.
  let segment = null;
  for (const el of ebmlChildren(head, 0, head.length)) {
    if (el.id === MKV.Segment) {
      segment = el;
      break;
    }
  }
  if (!segment) return null;

  let timestampScale = 1_000_000; // ns per tick, default
  let rawDuration = null;
  const streams = [];

  for (const el of ebmlChildren(head, segment.start, Math.min(segment.end, head.length))) {
    if (el.id === MKV.Cluster) break; // media data begins; headers are done
    if (el.id === MKV.Info) {
      for (const c of ebmlChildren(head, el.start, el.end)) {
        if (c.id === MKV.TimestampScale) timestampScale = ebmlUint(head, c.start, c.end - c.start);
        if (c.id === MKV.Duration) rawDuration = ebmlFloat(head, c.start, c.end - c.start);
      }
    } else if (el.id === MKV.Tracks) {
      for (const t of ebmlChildren(head, el.start, el.end)) {
        if (t.id !== MKV.TrackEntry) continue;
        let type = 0;
        let codecId = "";
        let channels = 2;
        for (const f of ebmlChildren(head, t.start, t.end)) {
          if (f.id === MKV.TrackType) type = ebmlUint(head, f.start, f.end - f.start);
          else if (f.id === MKV.CodecID) codecId = new TextDecoder().decode(head.subarray(f.start, f.end));
          else if (f.id === MKV.Audio) {
            for (const a of ebmlChildren(head, f.start, f.end)) {
              if (a.id === MKV.Channels) channels = ebmlUint(head, a.start, a.end - a.start);
            }
          }
        }
        if (type === 1) streams.push({ track: "video", codec: mkvCodec(codecId) });
        else if (type === 2) streams.push({ track: "audio", codec: mkvCodec(codecId), channels });
        else if (type === 17) streams.push({ track: "subtitle", codec: mkvCodec(codecId) });
      }
    }
  }

  const duration = rawDuration !== null ? (rawDuration * timestampScale) / 1e9 : null;
  return { format: { name: "matroska,webm", duration }, streams };
}

// --- entry point ---

// probeMedia(read, fileLength) -> { format: { name, duration }, streams: [...] }
// Throws when the container is unrecognized.
export async function probeMedia(read, fileLength) {
  const head = await read(0, Math.min(16, fileLength));

  // MP4: [size]ftyp at offset 4. MKV: EBML magic 0x1A45DFA3 at offset 0.
  if (head.length >= 8 && fourcc(head, 4) === "ftyp") {
    const moov = await findMoov(read, fileLength);
    if (!moov) throw new Error("mp4: moov box not found");
    return parseMp4Moov(moov);
  }
  if (head.length >= 4 && head[0] === 0x1a && head[1] === 0x45 && head[2] === 0xdf && head[3] === 0xa3) {
    const result = await parseMkv(read, fileLength);
    if (!result) throw new Error("mkv: segment not found");
    return result;
  }
  throw new Error("unrecognized container");
}
