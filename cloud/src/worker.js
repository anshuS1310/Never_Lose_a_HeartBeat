import {computeWindow} from "./processing.js";

const json = (value, status = 200) => Response.json(value, {status, headers: {"Cache-Control": "no-store"}});
const validSession = value => typeof value === "string" && /^[A-Za-z0-9_-]{1,100}$/.test(value);

function validRows(rows, channels) {
  return Array.isArray(rows) && rows.length === 400 && rows.every(row => row === null ||
    (Array.isArray(row) && row.length === channels && row.every(v => Number.isInteger(v) &&
       v >= -8388608 && v <= 8388607)));
}

function validFrame(frame) {
  if (!frame || typeof frame !== "object" || !validSession(frame.session) ||
      !Number.isSafeInteger(frame.window) || frame.window < 0 ||
      !["online", "offline"].includes(frame.mode) || !validRows(frame.sensor1, 3) ||
      !validRows(frame.sensor2, 2) || !Array.isArray(frame.losses)) return false;
  const first = frame.window * 400, last = first + 399;
  if (!frame.losses.every(loss => [1, 2].includes(loss.sensor) &&
       Number.isSafeInteger(loss.from_index) && Number.isSafeInteger(loss.to_index) &&
       loss.from_index >= first && loss.to_index <= last && loss.to_index >= loss.from_index &&
       typeof loss.reason === "string" && loss.reason.length > 0 && loss.reason.length <= 100)) return false;
  if (frame.window >= 4 && frame.mode === "online") {
    if (frame.context_start !== (frame.window - 6) * 400 ||
        !Array.isArray(frame.context1) || frame.context1.length !== 2801 ||
        !frame.context1.every(row => row === null ||
           (Number.isInteger(row) && row >= -8388608 && row <= 8388607) ||
           (Array.isArray(row) && row.length === 3 && row.every(v => Number.isInteger(v) &&
            v >= -8388608 && v <= 8388607))) ||
        !(frame.previous_peak === null || (Number.isSafeInteger(frame.previous_peak) &&
          frame.previous_peak < (frame.window - 4) * 400))) return false;
  }
  return true;
}

async function ingestFrame(db, frame) {
  if (!validFrame(frame)) return json({error: "invalid frame"}, 400);
  const s1 = JSON.stringify(frame.sensor1), s2 = JSON.stringify(frame.sensor2);
  const losses = JSON.stringify(frame.losses);
  const old = await db.prepare("SELECT sensor1,sensor2,losses,mode FROM frames WHERE session=? AND window=?")
    .bind(frame.session, frame.window).first();
  if (old && (old.sensor1 !== s1 || old.sensor2 !== s2 || old.losses !== losses || old.mode !== frame.mode))
    return json({error: "conflicting frame identity"}, 409);
  if (!old) {
    const count1 = frame.sensor1.reduce((n, x) => n + (x === null ? 0 : 1), 0);
    const count2 = frame.sensor2.reduce((n, x) => n + (x === null ? 0 : 1), 0);
    await db.prepare("INSERT INTO frames(session,window,mode,sensor1,sensor2,losses,count1,count2) " +
      "VALUES(?,?,?,?,?,?,?,?)").bind(frame.session, frame.window, frame.mode,
        s1, s2, losses, count1, count2).run();
  }
  if (frame.mode === "online") {
    const result = computeWindow(frame.window, frame.sensor1, frame.sensor2,
      frame.context1, frame.context_start, frame.previous_peak, "online");
    await db.prepare("INSERT OR IGNORE INTO results(session,window,mode,body) VALUES(?,?,?,?)")
      .bind(frame.session, frame.window, "online", JSON.stringify(result)).run();
  }
  if (frame.offline_result) {
    const result = frame.offline_result;
    if (result.window !== frame.window || result.mode !== "offline")
      return json({error: "invalid offline result"}, 400);
    await db.prepare("INSERT OR IGNORE INTO results(session,window,mode,body) VALUES(?,?,?,?)")
      .bind(frame.session, frame.window, "offline", JSON.stringify(result)).run();
  }
  return json({ok: true, session: frame.session, window: frame.window});
}

async function ingestOfflineBatch(db, frames) {
  if (!Array.isArray(frames) || frames.length < 1 || frames.length > 8 ||
      !frames.every(frame => frame && frame.mode === "offline" && validFrame(frame) &&
        (!frame.offline_result || (frame.offline_result.window === frame.window &&
          frame.offline_result.mode === "offline"))) ||
      !frames.every(frame => frame.session === frames[0].session))
    return json({error: "invalid offline batch"}, 400);
  const windows = frames.map(frame => frame.window);
  if (new Set(windows).size !== windows.length) return json({error: "duplicate batch window"}, 400);
  const placeholders = windows.map(() => "?").join(",");
  const old = await db.prepare("SELECT window,mode,sensor1,sensor2,losses FROM frames " +
    `WHERE session=? AND window IN (${placeholders})`).bind(frames[0].session, ...windows).all();
  const existing = new Map(old.results.map(row => [row.window, row]));
  const statements = [];
  for (const frame of frames) {
    const s1 = JSON.stringify(frame.sensor1), s2 = JSON.stringify(frame.sensor2);
    const losses = JSON.stringify(frame.losses), found = existing.get(frame.window);
    if (found && (found.mode !== "offline" || found.sensor1 !== s1 ||
        found.sensor2 !== s2 || found.losses !== losses))
      return json({error: "conflicting frame identity"}, 409);
    if (!found) {
      const count1 = frame.sensor1.reduce((n, x) => n + (x === null ? 0 : 1), 0);
      const count2 = frame.sensor2.reduce((n, x) => n + (x === null ? 0 : 1), 0);
      statements.push(db.prepare("INSERT OR IGNORE INTO frames" +
        "(session,window,mode,sensor1,sensor2,losses,count1,count2) VALUES(?,?,?,?,?,?,?,?)")
        .bind(frame.session, frame.window, "offline", s1, s2, losses, count1, count2));
    }
    if (frame.offline_result) {
      statements.push(db.prepare("INSERT OR IGNORE INTO results(session,window,mode,body) VALUES(?,?,?,?)")
        .bind(frame.session, frame.window, "offline", JSON.stringify(frame.offline_result)));
    }
  }
  if (statements.length) await db.batch(statements);
  return json({ok: true, windows});
}

export default {
  async fetch(request, env) {
    if (!env.API_TOKEN || request.headers.get("Authorization") !== `Bearer ${env.API_TOKEN}`)
      return json({error: "unauthorized"}, 401);
    try {
      const url = new URL(request.url), path = url.pathname;
      if (path === "/health" && request.method === "GET") return json({ok: true});
      if (path === "/frame" && request.method === "POST")
        return ingestFrame(env.DB, await request.json());
      if (path === "/frame-batch" && request.method === "POST")
        return ingestOfflineBatch(env.DB, await request.json());
      if (path === "/offline-result" && request.method === "POST") {
        const incoming = await request.json();
        const result = incoming.result;
        if (!validSession(incoming.session) || !result || result.mode !== "offline" ||
            !Number.isSafeInteger(result.window) || result.window < 0)
          return json({error: "invalid offline result"}, 400);
        await env.DB.prepare("INSERT OR IGNORE INTO results(session,window,mode,body) VALUES(?,?,?,?)")
          .bind(incoming.session, result.window, "offline", JSON.stringify(result)).run();
        return json({ok: true});
      }
      if (path === "/frames" && request.method === "GET") {
        const session = url.searchParams.get("session");
        if (!validSession(session)) return json({error: "invalid session"}, 400);
        const after = Number(url.searchParams.get("after") ?? -1);
        const rows = await env.DB.prepare("SELECT window,mode,sensor1,sensor2,losses FROM frames " +
          "WHERE session=? AND window>? ORDER BY window LIMIT 25").bind(session, after).all();
        return json({frames: rows.results.map(row => ({window: row.window, mode: row.mode,
          sensor1: JSON.parse(row.sensor1), sensor2: JSON.parse(row.sensor2),
          losses: JSON.parse(row.losses)}))});
      }
      if (path === "/results" && request.method === "GET") {
        const session = url.searchParams.get("session");
        if (!validSession(session)) return json({error: "invalid session"}, 400);
        const offset = Math.max(0, Number(url.searchParams.get("offset") || 0));
        const rows = await env.DB.prepare("SELECT body,processed_at FROM results WHERE session=? " +
          "ORDER BY window,mode LIMIT 100 OFFSET ?").bind(session, offset).all();
        return json({results: rows.results.map(row => ({...JSON.parse(row.body), processed_at: row.processed_at}))});
      }
      if (path === "/terminal-loss" && request.method === "POST") {
        const loss = await request.json();
        if (!validSession(loss.session) || ![1, 2].includes(loss.sensor) ||
            !Number.isSafeInteger(loss.from_index) || loss.from_index < 0 ||
            typeof loss.reason !== "string" || !loss.reason.length)
          return json({error: "invalid loss"}, 400);
        await env.DB.prepare("INSERT OR IGNORE INTO terminal_losses VALUES(?,?,?,?)")
          .bind(loss.session, loss.sensor, loss.from_index, loss.reason).run();
        return json({ok: true});
      }
      if (path === "/terminal-loss" && request.method === "GET") {
        const session = url.searchParams.get("session");
        if (!validSession(session)) return json({error: "invalid session"}, 400);
        const rows = await env.DB.prepare("SELECT sensor,first_idx,reason FROM terminal_losses WHERE session=? " +
          "ORDER BY sensor,first_idx").bind(session).all();
        return json({losses: rows.results.map(row => ({sensor: row.sensor,
          from_index: row.first_idx, to_index: null, reason: row.reason}))});
      }
      if (path === "/status" && request.method === "GET") {
        const row = await env.DB.prepare("SELECT COALESCE(SUM(count1+count2),0) AS samples FROM frames").first();
        return json({samples: row.samples});
      }
      return json({error: "not found"}, 404);
    } catch (error) {
      return json({error: String(error)}, 500);
    }
  }
};
