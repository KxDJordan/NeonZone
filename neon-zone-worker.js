/**
 * NEON ZONE TCG — Cloudflare Worker
 * ─────────────────────────────────
 * Deployment:
 *   1. Create a Cloudflare account and install Wrangler:
 *        npm install -g wrangler
 *   2. Create wrangler.toml (see bottom of this file for template)
 *   3. Deploy:
 *        wrangler deploy
 *
 * Architecture:
 *   - Each game room is a Durable Object instance (GameRoom)
 *   - Two players connect via WebSocket; the Worker relays actions
 *     and holds authoritative turn/score state
 *   - Room codes are 6-character uppercase alphanumeric strings
 *   - Rooms expire after 30 minutes of inactivity
 */

// ─────────────────────────────────────────────────────────
// MAIN WORKER — routes HTTP + WebSocket upgrade requests
// ─────────────────────────────────────────────────────────
// CORS that supports credentials: 'include' (specific origin required, not *)
function makeCors(request) {
  const origin = request.headers.get('Origin') || '';
  return {
    'Access-Control-Allow-Origin': origin || '*',
    'Access-Control-Allow-Methods': 'GET, POST, DELETE, OPTIONS',
    'Access-Control-Allow-Headers': 'Content-Type, Authorization',
    'Access-Control-Allow-Credentials': 'true',
    'Vary': 'Origin',
  };
}

export default {
  async fetch(request, env) {
    const url  = new URL(request.url);
    const cors = makeCors(request);

    if (request.method === 'OPTIONS') {
      return new Response(null, { status: 204, headers: cors });
    }

    // Auth routes
    const authResp = await handleAuth(request, env, cors);
    if (authResp) return authResp;

    // Deck sync routes
    const deckResp = await handleDecks(request, env, cors);
    if (deckResp) return deckResp;

    // Inventory & gacha routes
    const invResp = await handleInventory(request, env, cors);
    if (invResp) return invResp;

    // Route: POST /room/create
    if (url.pathname === '/room/create' && request.method === 'POST') {
      const code = generateRoomCode();
      const id   = env.GAME_ROOM.idFromName(code);
      const stub = env.GAME_ROOM.get(id);
      const resp = await stub.fetch(new Request('https://internal/init', {
        method: 'POST',
        body: JSON.stringify({ code }),
        headers: { 'Content-Type': 'application/json' },
      }));
      if (!resp.ok) return jsonError('Failed to create room', 500, cors);
      return json({ code }, cors);
    }

    // Route: GET /room/:code/status
    if (url.pathname.match(/^\/room\/[A-Z0-9]{6}\/status$/) && request.method === 'GET') {
      const code = url.pathname.split('/')[2];
      const stub = env.GAME_ROOM.get(env.GAME_ROOM.idFromName(code));
      const data = await (await stub.fetch(new Request('https://internal/status'))).json();
      return json(data, cors);
    }

    // Route: GET /room/:code/ws
    if (url.pathname.match(/^\/room\/[A-Z0-9]{6}\/ws$/) && request.method === 'GET') {
      if (request.headers.get('Upgrade') !== 'websocket') {
        return new Response('Expected WebSocket upgrade', { status: 426 });
      }
      const code = url.pathname.split('/')[2];
      const stub = env.GAME_ROOM.get(env.GAME_ROOM.idFromName(code));
      return stub.fetch(request);
    }

    return new Response('NEON ZONE TCG API — v1.0', {
      headers: { ...cors, 'Content-Type': 'text/plain' }
    });
  }
};

// ─────────────────────────────────────────────────────────
// DURABLE OBJECT — GameRoom
// ─────────────────────────────────────────────────────────
export class GameRoom {
  constructor(state, env) {
    this.state = state;
    this.env   = env;
    this.sessions = [];      // [{ ws, playerId, ready, deckSummary }]
    this.gameState = null;   // authoritative game state snapshot
    this.roomCode = null;
    this.lastActivity = Date.now();
    this.started = false;
    this.playerDecks = {};   // playerId -> deck definition
  }

  // Rebuild in-memory sessions from WebSockets that survived hibernation.
  // Each WS carries its session data as a serialized attachment.
  _syncSessions() {
    this.sessions = this.state.getWebSockets().map(ws => {
      const d = ws.deserializeAttachment() || {};
      return { ws, playerId: d.playerId, ready: d.ready || false, deckSummary: d.deckSummary || null };
    });
  }

  // Reload persisted fields that are lost when the DO hibernates.
  async _loadState() {
    if (!this.roomCode)             this.roomCode    = (await this.state.storage.get('code'))        || null;
    if (!this.started)              this.started     = (await this.state.storage.get('started'))     || false;
    if (!this.gameState)            this.gameState   = (await this.state.storage.get('gameState'))   || null;
    if (!Object.keys(this.playerDecks).length)
                                    this.playerDecks = (await this.state.storage.get('playerDecks')) || {};
  }

  async fetch(request) {
    const url = new URL(request.url);

    // Internal init
    if (url.pathname === '/init' && request.method === 'POST') {
      const { code } = await request.json();
      this.roomCode = code;
      await this.state.storage.put('code', code);
      return new Response('OK');
    }

    // Status check
    if (url.pathname === '/status') {
      await this._loadState();
      this._syncSessions();
      return new Response(JSON.stringify({
        code:    this.roomCode,
        players: this.sessions.length,
        started: this.started,
        full:    this.sessions.length >= 2,
      }), { headers: { 'Content-Type': 'application/json' } });
    }

    // WebSocket upgrade
    if (request.headers.get('Upgrade') === 'websocket') {
      return this.handleWebSocket(request);
    }

    return new Response('Not found', { status: 404 });
  }

  async handleWebSocket(request) {
    // Recover any sessions from before hibernation before checking capacity.
    await this._loadState();
    this._syncSessions();

    if (this.sessions.length >= 2) {
      return new Response('Room full', { status: 409 });
    }

    const pair = new WebSocketPair();
    const [client, server] = Object.values(pair);

    // Serialize session identity into the WS attachment so it survives hibernation.
    const playerId = this.sessions.length === 0 ? 'A' : 'B';
    server.serializeAttachment({ playerId, ready: false, deckSummary: null });
    this.state.acceptWebSocket(server);

    const session = { ws: server, playerId, ready: false };
    this.sessions.push(session);
    this.lastActivity = Date.now();

    // If joiner, tell them about the opponent's readiness.
    let opponentReady = false;
    if (playerId === 'B' && this.sessions.length > 1) {
      const opponent = this.sessions[0];
      opponentReady = opponent.ready;
    }

    // Send welcome
    this.sendTo(server, {
      type: 'connected',
      playerId,
      roomCode: this.roomCode,
      playerCount: this.sessions.length,
      opponentReady,
    });

    // Notify other player someone joined
    this.broadcast({ type: 'player_joined', playerCount: this.sessions.length }, server);

    return new Response(null, { status: 101, webSocket: client });
  }

  async webSocketMessage(ws, rawMsg) {
    await this._loadState();
    this._syncSessions();
    this.lastActivity = Date.now();

    let msg;
    try { msg = JSON.parse(rawMsg); } catch { return; }

    const session = this.sessions.find(s => s.ws === ws);
    if (!session) return;

    switch (msg.type) {

      // Player declares their deck and signals ready
      case 'ready': {
        session.ready = true;
        session.deckSummary = msg.deck; // { name, active, bench, free }
        // Persist updated session data in the WS attachment
        ws.serializeAttachment({ playerId: session.playerId, ready: true, deckSummary: msg.deck });
        this.playerDecks[session.playerId] = msg.deck;
        await this.state.storage.put('playerDecks', this.playerDecks);
        this.broadcast({ type: 'opponent_ready', playerId: session.playerId });

        // Both ready → start game
        if (this.sessions.length === 2 && this.sessions.every(s => s.ready)) {
          await this.startGame();
        }
        break;
      }

      // A player performed a game action
      case 'action': {
        if (!this.started) return;
        if (msg.playerId !== session.playerId) return; // can't spoof

        // Validate it's their turn
        if (this.gameState && this.gameState.turn !== session.playerId) {
          this.sendTo(ws, { type: 'error', message: 'Not your turn' });
          return;
        }

        // Relay action to the other player
        this.broadcast({ type: 'opponent_action', action: msg.action }, ws);

        // Update our authoritative snapshot
        if (msg.stateSnapshot) {
          this.gameState = msg.stateSnapshot;
          await this.state.storage.put('gameState', this.gameState);
        }
        break;
      }

      // Player ended their turn — update authoritative turn state
      case 'end_turn': {
        if (!this.started) return;
        if (this.gameState) {
          this.gameState.turn = session.playerId === 'A' ? 'B' : 'A';
          this.gameState.round = (this.gameState.round || 1) + (session.playerId === 'B' ? 1 : 0);
          await this.state.storage.put('gameState', this.gameState);
        }
        this.broadcast({
          type:  'turn_ended',
          byPlayer: session.playerId,
          newTurn: this.gameState?.turn,
        }, ws);
        break;
      }

      // Score/win update
      case 'score_update': {
        if (this.gameState) {
          this.gameState.score = msg.score;
          await this.state.storage.put('gameState', this.gameState);
        }
        this.broadcast({ type: 'score_update', score: msg.score }, ws);
        if (msg.winner) {
          this.broadcast({ type: 'game_over', winner: msg.winner, score: msg.score });
          this.started = false;
          await this.state.storage.put('started', false);
        }
        break;
      }

      // Interrupt response (block / pass)
      case 'interrupt_response': {
        this.broadcast({ type: 'interrupt_response', response: msg.response, byPlayer: session.playerId }, ws);
        break;
      }

      // Chat / ping
      case 'ping': {
        this.sendTo(ws, { type: 'pong', ts: Date.now() });
        break;
      }

      case 'chat': {
        if (!msg.text || msg.text.length > 200) return;
        this.broadcast({
          type: 'chat',
          playerId: session.playerId,
          text: msg.text.replace(/</g, '&lt;').replace(/>/g, '&gt;'),
        });
        break;
      }

      default:
        break;
    }
  }

  async webSocketClose(ws, code, reason) {
    // The closing WS may not appear in getWebSockets() any more, so read its
    // identity from the attachment before syncing the remaining sessions.
    const attachment = ws.deserializeAttachment() || {};
    const disconnectedPlayerId = attachment.playerId;

    await this._loadState();
    this._syncSessions();
    // Remove the closing socket if it still appears
    this.sessions = this.sessions.filter(s => s.ws !== ws);

    if (!disconnectedPlayerId) return;

    this.broadcast({
      type: 'opponent_disconnected',
      playerId: disconnectedPlayerId,
      started: this.started,
    });

    if (this.started) {
      this.started = false;
      await this.state.storage.put('started', false);
    }
  }

  async webSocketError(ws, error) {
    await this.webSocketClose(ws, 1011, error?.message || 'error');
  }

  async startGame() {
    this.started = true;
    this.gameState = {
      turn:  'A',
      round: 1,
      score: { A: 0, B: 0 },
      startedAt: Date.now(),
    };
    await this.state.storage.put('started', true);
    await this.state.storage.put('gameState', this.gameState);

    // Tell each player the game starts, give them each other's deck summary
    const [sesA, sesB] = this.sessions[0].playerId === 'A'
      ? [this.sessions[0], this.sessions[1]]
      : [this.sessions[1], this.sessions[0]];

    this.sendTo(sesA.ws, {
      type:        'game_start',
      yourPlayer:  'A',
      opponentDeck: this.playerDecks['B'],
      gameState:   this.gameState,
    });

    this.sendTo(sesB.ws, {
      type:        'game_start',
      yourPlayer:  'B',
      opponentDeck: this.playerDecks['A'],
      gameState:   this.gameState,
    });
  }

  sendTo(ws, obj) {
    try { ws.send(JSON.stringify(obj)); } catch {}
  }

  broadcast(obj, exclude = null) {
    for (const s of this.sessions) {
      if (s.ws !== exclude) this.sendTo(s.ws, obj);
    }
  }
}

// ── Password hashing with PBKDF2 (built into Workers runtime) ──
async function hashPassword(password, salt) {
  const enc = new TextEncoder();
  const keyMaterial = await crypto.subtle.importKey(
    'raw', enc.encode(password), 'PBKDF2', false, ['deriveBits']
  );
  const bits = await crypto.subtle.deriveBits(
    { name:'PBKDF2', salt: enc.encode(salt), iterations: 100_000, hash:'SHA-256' },
    keyMaterial, 256
  );
  return Array.from(new Uint8Array(bits)).map(b=>b.toString(16).padStart(2,'0')).join('');
}

function randomHex(bytes = 32) {
  return Array.from(crypto.getRandomValues(new Uint8Array(bytes)))
    .map(b=>b.toString(16).padStart(2,'0')).join('');
}

function nanoid(prefix = 'usr') {
  return `${prefix}_${randomHex(8)}`;
}

// ── Session token → user_id lookup ──
async function getSession(request, env) {
  const token = request.headers.get('Authorization')?.replace('Bearer ', '')
    || getCookie(request, 'nz_session');
  if (!token) return null;
  const row = await env.DB.prepare(
    'SELECT user_id, expires_at FROM sessions WHERE token = ?'
  ).bind(token).first();
  if (!row || row.expires_at < Date.now() / 1000) return null;
  return row.user_id;
}

function getCookie(request, name) {
  const header = request.headers.get('Cookie') || '';
  const match = header.match(new RegExp(`(?:^|; )${name}=([^;]+)`));
  return match ? match[1] : null;
}

function sessionCookie(token, maxAgeDays = 30) {
  const maxAge = maxAgeDays * 86400;
  return `nz_session=${token}; HttpOnly; Secure; SameSite=None; Max-Age=${maxAge}; Path=/`;
}

// ── Auth route handler — plug into your existing fetch() ──
export async function handleAuth(request, env, corsHeaders) {
  const url  = new URL(request.url);
  const path = url.pathname;

  // POST /auth/register
  if (path === '/auth/register' && request.method === 'POST') {
    const { username, email, password } = await request.json();

    if (!username?.match(/^[a-zA-Z0-9_]{3,20}$/))
      return jsonError('Username must be 3–20 alphanumeric characters', 400, corsHeaders);
    if (!password || password.length < 8)
      return jsonError('Password must be at least 8 characters', 400, corsHeaders);

    // Check uniqueness
    const byUsername = await env.DB.prepare(
      'SELECT id FROM users WHERE username = ?'
    ).bind(username).first();
    if (byUsername) return jsonError('Username already taken', 409, corsHeaders);

    if (email?.includes('@')) {
      const byEmail = await env.DB.prepare(
        'SELECT id FROM users WHERE email = ?'
      ).bind(email.toLowerCase()).first();
      if (byEmail) return jsonError('Email already in use', 409, corsHeaders);
    }

    const salt    = randomHex(16);
    const pw_hash = await hashPassword(password, salt);
    const id      = nanoid('usr');
    const storedEmail = email?.includes('@') ? email.toLowerCase() : null;

    const starterUnits = JSON.stringify(['vex','skar','bastion','rampart','lyra','grid']);
    const starterCards = JSON.stringify({'surge-strike':3,'barrier-field':3,'quick-heal':2,'push-wave':2,'charge-pack':2,'draw-protocol':1,'combo-hit':1,'brace':1});
    await env.DB.prepare(
      'INSERT INTO users (id, username, email, pw_hash, pw_salt, credits, owned_units, owned_cards) VALUES (?, ?, ?, ?, ?, 500, ?, ?)'
    ).bind(id, username, storedEmail, pw_hash, salt, starterUnits, starterCards).run();

    // Auto-login on register
    const token     = crypto.randomUUID();
    const expiresAt = Math.floor(Date.now() / 1000) + 30 * 86400;
    await env.DB.prepare(
      'INSERT INTO sessions (token, user_id, expires_at) VALUES (?, ?, ?)'
    ).bind(token, id, expiresAt).run();

    return new Response(JSON.stringify({ id, username, token }), {
      headers: {
        'Content-Type': 'application/json',
        'Set-Cookie': sessionCookie(token),
        ...corsHeaders,
      },
    });
  }

  // POST /auth/login
  if (path === '/auth/login' && request.method === 'POST') {
    const { username, password } = await request.json();
    const user = await env.DB.prepare(
      'SELECT id, pw_hash, pw_salt, username FROM users WHERE username = ? OR email = ?'
    ).bind(username, username).first();

    if (!user) return jsonError('Invalid username or password', 401, corsHeaders);

    const hash = await hashPassword(password, user.pw_salt);
    if (hash !== user.pw_hash) return jsonError('Invalid username or password', 401, corsHeaders);

    const token     = crypto.randomUUID();
    const expiresAt = Math.floor(Date.now() / 1000) + 30 * 86400;
    await env.DB.prepare(
      'INSERT INTO sessions (token, user_id, expires_at, ip) VALUES (?, ?, ?, ?)'
    ).bind(token, user.id, expiresAt, request.headers.get('CF-Connecting-IP')).run();

    await env.DB.prepare('UPDATE users SET last_seen = unixepoch() WHERE id = ?')
      .bind(user.id).run();

    return new Response(JSON.stringify({ id: user.id, username: user.username, token }), {
      headers: {
        'Content-Type': 'application/json',
        'Set-Cookie': sessionCookie(token),
        ...corsHeaders,
      },
    });
  }

  // POST /auth/logout
  if (path === '/auth/logout' && request.method === 'POST') {
    const token = request.headers.get('Authorization')?.replace('Bearer ', '')
      || getCookie(request, 'nz_session');
    if (token) {
      await env.DB.prepare('DELETE FROM sessions WHERE token = ?').bind(token).run();
    }
    return new Response('{}', {
      headers: {
        'Content-Type': 'application/json',
        'Set-Cookie': 'nz_session=; Max-Age=0; Path=/',
        ...corsHeaders,
      },
    });
  }

  // GET /auth/me  — validate current session, return profile
  if (path === '/auth/me' && request.method === 'GET') {
    const userId = await getSession(request, env);
    if (!userId) return jsonError('Not authenticated', 401, corsHeaders);
    const user = await env.DB.prepare(
      'SELECT id, username, email, created_at, last_seen FROM users WHERE id = ?'
    ).bind(userId).first();
    if (!user) return jsonError('User not found', 404, corsHeaders);
    return json(user, corsHeaders);
  }

  return null; // not an auth route
}

// ─── Deck sync routes ─────────────────────────────────────────
export async function handleDecks(request, env, corsHeaders) {
  const url = new URL(request.url);
  if (!url.pathname.startsWith('/decks')) return null;
  const userId = await getSession(request, env);
  if (!userId) return jsonError('Not authenticated', 401, corsHeaders);

  // GET /decks  — list all decks for current user
  if (url.pathname === '/decks' && request.method === 'GET') {
    const { results } = await env.DB.prepare(
      'SELECT id, name, active, bench, free_cards, updated_at FROM decks WHERE user_id = ? ORDER BY updated_at DESC'
    ).bind(userId).all();
    const decks = results.map(d => ({
      ...d,
      active:     JSON.parse(d.active),
      bench:      JSON.parse(d.bench),
      free:       JSON.parse(d.free_cards),
    }));
    return json(decks, corsHeaders);
  }

  // POST /decks  — replace all decks for this user (client sends full array)
  if (url.pathname === '/decks' && request.method === 'POST') {
    const body = await request.json();
    const decksArray = Array.isArray(body) ? body : [body];
    await env.DB.prepare('DELETE FROM decks WHERE user_id = ?').bind(userId).run();
    for (const deck of decksArray) {
      const { id, name, active, bench, free } = deck;
      if (!name || !active || !bench) continue;
      const deckId = id || nanoid('dck');
      await env.DB.prepare(`
        INSERT INTO decks (id, user_id, name, active, bench, free_cards, updated_at)
        VALUES (?, ?, ?, ?, ?, ?, unixepoch())
      `).bind(deckId, userId, name, JSON.stringify(active), JSON.stringify(bench), JSON.stringify(free||[])).run();
    }
    return json({ ok: true }, corsHeaders);
  }

  // DELETE /decks/:id
  if (url.pathname.match(/^\/decks\/[a-z0-9_]+$/) && request.method === 'DELETE') {
    const deckId = url.pathname.split('/')[2];
    await env.DB.prepare('DELETE FROM decks WHERE id = ? AND user_id = ?')
      .bind(deckId, userId).run();
    return json({ deleted: deckId }, corsHeaders);
  }

  return null;
}

// ─── Inventory & Gacha ───────────────────────────────────────
async function handleInventory(request, env, corsHeaders) {
  const url = new URL(request.url);
  if (!url.pathname.startsWith('/inventory') && url.pathname !== '/match/reward') return null;

  const userId = await getSession(request, env);
  if (!userId) return jsonError('Not authenticated', 401, corsHeaders);

  // GET /inventory
  if (url.pathname === '/inventory' && request.method === 'GET') {
    let user = await env.DB.prepare(
      'SELECT credits, owned_units, owned_cards FROM users WHERE id = ?'
    ).bind(userId).first();
    if (!user) return jsonError('Not found', 404, corsHeaders);

    // First-time init for accounts created before this feature
    if (user.owned_units === null) {
      const su = JSON.stringify(['vex','skar','bastion','rampart','lyra','grid']);
      const sc = JSON.stringify({'surge-strike':3,'barrier-field':3,'quick-heal':2,'push-wave':2,'charge-pack':2,'draw-protocol':1,'combo-hit':1,'brace':1,'reposition':1,'counterstrike':1});
      await env.DB.prepare('UPDATE users SET credits=500, owned_units=?, owned_cards=? WHERE id=?')
        .bind(su, sc, userId).run();
      return json({ credits: 500, ownedUnits: JSON.parse(su), ownedCards: JSON.parse(sc) }, corsHeaders);
    }

    return json({
      credits:    user.credits || 0,
      ownedUnits: JSON.parse(user.owned_units || '[]'),
      ownedCards: JSON.parse(user.owned_cards || '{}'),
    }, corsHeaders);
  }

  // POST /inventory/pull-unit — 200 credits → 1 random non-starter unit
  if (url.pathname === '/inventory/pull-unit' && request.method === 'POST') {
    const user = await env.DB.prepare('SELECT credits, owned_units FROM users WHERE id = ?').bind(userId).first();
    if ((user?.credits || 0) < 200) return jsonError('Insufficient credits', 400, corsHeaders);

    const owned = JSON.parse(user.owned_units || '["vex","bastion","lyra","grid"]');
    const pool  = ['skar','kira','dusk','fang','nova',
                   'rampart','forge','aegis','wall','bulkhead',
                   'echo','pulse','flux','veil','axiom',
                   'cipher','vector','relay','parse','delta'];
    const unowned = pool.filter(u => !owned.includes(u));

    if (unowned.length === 0) {
      await env.DB.prepare('UPDATE users SET credits = credits - 100 WHERE id = ?').bind(userId).run();
      const up = await env.DB.prepare('SELECT credits FROM users WHERE id = ?').bind(userId).first();
      return json({ unit: null, creditRefund: 100, creditsAfter: up.credits }, corsHeaders);
    }

    const newUnit = unowned[Math.floor(Math.random() * unowned.length)];
    owned.push(newUnit);
    await env.DB.prepare('UPDATE users SET credits = credits - 200, owned_units = ? WHERE id = ?')
      .bind(JSON.stringify(owned), userId).run();
    const up = await env.DB.prepare('SELECT credits FROM users WHERE id = ?').bind(userId).first();
    return json({ unit: newUnit, creditsAfter: up.credits }, corsHeaders);
  }

  // POST /inventory/pull-cards — 100 credits → 3 random free ability cards
  if (url.pathname === '/inventory/pull-cards' && request.method === 'POST') {
    const user = await env.DB.prepare('SELECT credits, owned_cards FROM users WHERE id = ?').bind(userId).first();
    if ((user?.credits || 0) < 100) return jsonError('Insufficient credits', 400, corsHeaders);

    const ownedCards = JSON.parse(user.owned_cards || '{}');
    const freePool = [
      {id:'surge-strike',max:3},{id:'barrier-field',max:3},{id:'quick-heal',max:3},
      {id:'push-wave',max:3},{id:'charge-pack',max:3},{id:'draw-protocol',max:3},
      {id:'combo-hit',max:3},{id:'brace',max:3},{id:'reposition',max:3},
      {id:'chain-reaction',max:2},{id:'overclock-field',max:2},{id:'counterstrike',max:3},
      {id:'nullify-field',max:2},{id:'deep-heal',max:2},{id:'scavenge',max:2},{id:'blitz-wave',max:2},
    ];
    const pulled = [];
    for (let i = 0; i < 3; i++) {
      const available = freePool.filter(c => (ownedCards[c.id] || 0) < c.max);
      if (available.length === 0) break;
      const pick = available[Math.floor(Math.random() * available.length)];
      ownedCards[pick.id] = (ownedCards[pick.id] || 0) + 1;
      pulled.push(pick.id);
    }

    await env.DB.prepare('UPDATE users SET credits = credits - 100, owned_cards = ? WHERE id = ?')
      .bind(JSON.stringify(ownedCards), userId).run();
    const up = await env.DB.prepare('SELECT credits FROM users WHERE id = ?').bind(userId).first();
    return json({ cards: pulled, ownedCards, creditsAfter: up.credits }, corsHeaders);
  }

  // POST /match/reward — award credits for completing a match
  if (url.pathname === '/match/reward' && request.method === 'POST') {
    const { won, myScore = 0, oppScore = 0, unitsAlive = 0 } = await request.json();
    let amount;
    if (won) {
      // Base 100 + 10 per point margin + 15 per surviving unit
      const margin = Math.max(0, (myScore || 5) - (oppScore || 0));
      amount = 100 + margin * 10 + Math.min(unitsAlive, 3) * 15;
    } else {
      // Base 30 + 10 per point scored + 10 per surviving unit
      amount = 30 + Math.min(myScore, 4) * 10 + Math.min(unitsAlive, 3) * 10;
    }
    await env.DB.prepare('UPDATE users SET credits = credits + ? WHERE id = ?').bind(amount, userId).run();
    const up = await env.DB.prepare('SELECT credits FROM users WHERE id = ?').bind(userId).first();
    return json({ earned: amount, creditsAfter: up.credits }, corsHeaders);
  }

  return null;
}

// ─── Match history ────────────────────────────────────────────
export async function saveMatch(env, matchData) {
  const { playerAId, playerBId, winner, scoreA, scoreB, deckAId, deckBId, durationS } = matchData;
  const id = nanoid('mch');
  await env.DB.prepare(`
    INSERT INTO matches (id, player_a_id, player_b_id, winner, score_a, score_b, deck_a_id, deck_b_id, duration_s)
    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
  `).bind(id, playerAId||null, playerBId||null, winner, scoreA, scoreB, deckAId||null, deckBId||null, durationS||0).run();
}

// ─────────────────────────────────────────────────────────
// HELPERS
// ─────────────────────────────────────────────────────────
function generateRoomCode() {
  const chars = 'ABCDEFGHJKLMNPQRSTUVWXYZ23456789'; // no ambiguous chars
  return Array.from({ length: 6 }, () => chars[Math.floor(Math.random() * chars.length)]).join('');
}

function json(data, headers = {}) {
  return new Response(JSON.stringify(data), {
    headers: { 'Content-Type': 'application/json', ...headers },
  });
}

function jsonError(message, status = 400, headers = {}) {
  return new Response(JSON.stringify({ error: message }), {
    status,
    headers: { 'Content-Type': 'application/json', ...headers },
  });
}


