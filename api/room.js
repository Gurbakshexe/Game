'use strict';

// Room presence for Moth Light, as one Vercel serverless function.
//
// Every player's browser POSTs its own presence here a few times a second and
// gets everyone else's back. State lives in one Redis hash per room
// (field = player id, value = "<server ms>|<presence json>"). One EVAL does the
// whole round trip, so each sync costs a single Redis command.
//
// Redis comes from the Upstash integration in the Vercel Marketplace, which
// sets KV_REST_API_URL and KV_REST_API_TOKEN. Plain Upstash names also work.

const REDIS_URL = process.env.UPSTASH_REDIS_REST_URL || process.env.KV_REST_API_URL;
const REDIS_TOKEN = process.env.UPSTASH_REDIS_REST_TOKEN || process.env.KV_REST_API_TOKEN;

const MAX_PLAYERS = 4;          // moths per room
const STALE_MS = 6000;          // a player not heard from for this long is dropped
const ROOM_TTL_MS = 120000;     // an empty room's key disappears after this
const MAX_PRESENCE_BYTES = 1500;

const LUA = `
local key = KEYS[1]
local now = tonumber(ARGV[1])
local id = ARGV[2]
local mode = ARGV[3]
local val = ARGV[4]
local all = redis.call('HGETALL', key)
local out = {}
local others = 0
local have = false
for i = 1, #all, 2 do
  local v = all[i + 1]
  local bar = string.find(v, '|', 1, true)
  local ts = tonumber(string.sub(v, 1, bar - 1))
  if now - ts > ${STALE_MS} then
    redis.call('HDEL', key, all[i])
  elseif all[i] == id then
    have = true
  else
    others = others + 1
    out[#out + 1] = all[i]
    out[#out + 1] = string.sub(v, bar + 1)
    out[#out + 1] = tostring(now - ts)
  end
end
if mode == 'leave' then
  redis.call('HDEL', key, id)
  return out
end
-- "others" counts everyone except this player; a newcomer is turned away when 4 are already in.
if (not have) and others >= ${MAX_PLAYERS} then
  return { 'FULL' }
end
redis.call('HSET', key, id, now .. '|' .. val)
redis.call('PEXPIRE', key, ${ROOM_TTL_MS})
return out
`;

async function redis(command) {
  const r = await fetch(REDIS_URL, {
    method: 'POST',
    headers: { Authorization: 'Bearer ' + REDIS_TOKEN, 'Content-Type': 'application/json' },
    body: JSON.stringify(command),
  });
  const j = await r.json().catch(() => ({}));
  if (!r.ok || j.error) throw new Error(j.error || 'redis ' + r.status);
  return j.result;
}

function readBody(req) {
  let b = req.body;
  if (typeof b === 'string') {
    try { b = JSON.parse(b); } catch (e) { b = null; }
  }
  return b && typeof b === 'object' && !Array.isArray(b) ? b : {};
}

module.exports = async function handler(req, res) {
  res.setHeader('Cache-Control', 'no-store');
  if (req.method !== 'POST') {
    res.setHeader('Allow', 'POST');
    return res.status(405).json({ error: 'method_not_allowed' });
  }
  if (!REDIS_URL || !REDIS_TOKEN) return res.status(503).json({ error: 'not_configured' });

  const body = readBody(req);
  try {
    if (body.op === 'ping') {
      await redis(['PING']);
      return res.status(200).json({ ok: true });
    }

    const room = String(body.room || '');
    const id = String(body.id || '');
    const leave = body.op === 'leave';
    if (!leave && body.op !== 'sync') return res.status(400).json({ error: 'bad_request' });
    if (!/^[a-z0-9]{1,8}$/.test(room) || !/^[a-z0-9]{8,24}$/.test(id)) {
      return res.status(400).json({ error: 'bad_request' });
    }

    let val = '';
    if (!leave) {
      const p = body.p && typeof body.p === 'object' && !Array.isArray(body.p) ? body.p : {};
      val = JSON.stringify(p);
      if (Buffer.byteLength(val) > MAX_PRESENCE_BYTES) return res.status(413).json({ error: 'too_large' });
    }

    const out = await redis(['EVAL', LUA, '1', 'moth:room:' + room, String(Date.now()), id, leave ? 'leave' : 'sync', val]);
    if (out.length === 1 && out[0] === 'FULL') return res.status(200).json({ full: true, peers: [] });

    const peers = [];
    for (let i = 0; i + 2 < out.length; i += 3) {
      let p = {};
      try { p = JSON.parse(out[i + 1]); } catch (e) { p = {}; }
      peers.push({ id: out[i], p, age: Number(out[i + 2]) || 0 });
    }
    return res.status(200).json({ peers });
  } catch (e) {
    console.error('room api:', e && e.message);
    return res.status(502).json({ error: 'store_unavailable' });
  }
};
