const { WebSocketServer } = require("ws");

const PORT = 3000;
const TURN_MS = 30000;
const REJOIN_GRACE_MS = 30000;
const FINISH_TO_LOBBY_MS = 4500;
const READY_MS = 15000;

const wss = new WebSocketServer({ port: PORT });

/** ======================
 *  Helpers
 ======================= */

function now() { return Date.now(); }

function safeStr(v, max = 64) {
  return (v ?? "").toString().slice(0, max);
}

function sanitizeProfile(p) {
  const name = safeStr(p?.name || p?.username || "Player", 32);
  const username = safeStr(p?.username || "", 32);
  const photoUrl = safeStr(p?.photoUrl || "", 512);
  return { name, username, photoUrl };
}

function send(ws, data) {
  if (!ws || ws.readyState !== 1) return;
  ws.send(JSON.stringify(data));
}

function kickWs(ws, message = "Вы зашли с другого устройства.") {
  if (!ws) return;
  send(ws, { type: "kicked", message });
  ws.kicked = true;
  try { ws.close(); } catch {}
}

function randId(prefix = "R") {
  return prefix + Math.random().toString(16).slice(2, 10) + "-" + Date.now().toString(16).slice(-6);
}

function getRoomByUserKey(userKey) {
  const roomId = userToRoom.get(userKey);
  if (!roomId) return null;
  return rooms.get(roomId) || null;
}

function broadcastToRoom(room, data) {
  if (!room) return;
  for (const p of room.players || []) {
    if (p?.ws && p.ws.readyState === 1) {
      send(p.ws, data);
    }
  }
}


/** ======================
 *  Data
 ======================= */

const rooms = new Map(); // roomId -> room
// room:
// {
//   id,isPrivate,password,maxPlayers,winMode,allowTransfer,
//   roles,createdAt,startedAt,
//   players:[{ws,userKey,role,profile,ready}],
//   state:null|gameState,
//   timer:null,
//   disconnectTimers:Map,
//   finishResetTimer:null
// }

const userToRoom = new Map(); // userKey -> roomId

const quickQueue = {
  waiting: null, // { userKey, ws, profile, winMode, allowTransfer, ts }
};

/** ======================
 *  Lobby lists rules
 *  - if game started and not finished => hidden from list
 *  - if finished: show ONLY if missing players
 ======================= */

function roomToListItem(room) {
  const players = room.players
    .filter(p => !!p.userKey)
    .map(p => ({
      role: p.role,
      username: p.profile?.username || "",
      name: p.profile?.name || "",
      photoUrl: p.profile?.photoUrl || ""
    }));

  const playersCount = room.players.filter(p => !!p.userKey).length;

  return {
    id: room.id,
    isPrivate: !!room.isPrivate,
    maxPlayers: room.maxPlayers,
    winMode: room.winMode,
    allowTransfer: !!room.allowTransfer,
    throwInMode: room.throwInMode || "all",
    playersCount,
    started: !!room.state,
    createdAt: room.createdAt,
    players
  };
}

function buildLobbyLists() {
  const pub = [];
  const priv = [];
  const emptyRooms = [];
  for (const room of rooms.values()) {
    const playersCount = room.players.filter(p => !!p.userKey).length;

    // 0 игроков — в идеале таких уже не будет (удалим ниже), но на всякий
    if (playersCount === 0) {
      emptyRooms.push(room);
      continue;
    }

    // Идёт игра => скрываем
    if (room.state && room.state.phase !== "finished") continue;

    // Игра завершилась => показываем только если не хватает игроков
    if (room.state && room.state.phase === "finished") {
      if (playersCount >= room.maxPlayers) continue;
    }

    // Лобби (room.state == null) => показываем
    const item = roomToListItem(room);
    if (room.isPrivate) priv.push(item);
    else pub.push(item);
  }
  pub.sort((a,b) => b.createdAt - a.createdAt);
  priv.sort((a,b) => b.createdAt - a.createdAt);
  for (const room of emptyRooms) deleteRoomIfEmpty(room, { broadcast: false });
  return { public: pub, private: priv };
}

function broadcastLobbyLists() {
  const lists = buildLobbyLists();
  for (const ws of wss.clients) {
    if (ws.readyState !== 1) continue;
    send(ws, { type:"lobbies", ...lists });
  }
}

function playersCount(room) {
  return room.players.filter(p => !!p.userKey).length;
}

function clearReadyTimer(room) {
  if (room.readyTimer) clearTimeout(room.readyTimer);
  room.readyTimer = null;
  room.readyDeadlineTs = null;
}

function scheduleReadyCountdown(room) {
  if (room.state) {
    clearReadyTimer(room);
    return;
  }
  if (playersCount(room) !== room.maxPlayers) {
    clearReadyTimer(room);
    return;
  }
  if (!room.readyDeadlineTs || room.readyDeadlineTs <= now()) {
    room.readyDeadlineTs = now() + READY_MS;
  }
  if (room.readyTimer) return;
  room.readyTimer = setTimeout(() => onReadyTimeout(room), READY_MS + 50);
}

function onReadyTimeout(room) {
  room.readyTimer = null;
  if (room.state) return;
  if (playersCount(room) !== room.maxPlayers) {
    room.readyDeadlineTs = null;
    return;
  }
  const allReady = room.roles.every(r => !!getPlayer(room, r)?.ready);
  if (allReady) {
    tryStartGame(room);
    return;
  }
  for (const p of room.players) p.ready = false;
  room.readyDeadlineTs = now() + READY_MS;
  sendLobbyState(room);
  scheduleReadyCountdown(room);
}

/** ======================
 *  Game Logic
 ======================= */

const SUITS = ["♣", "♦", "♥", "♠"];
const RANKS = [6, 7, 8, 9, 10, "J", "Q", "K", "A"];
const RANK_VALUE = new Map(RANKS.map((r, i) => [r, i]));

function createDeck() {
  const deck = [];
  let id = 1;
  for (const suit of SUITS) for (const rank of RANKS) deck.push({ id: id++, suit, rank });
  return deck;
}

function shuffle(a) {
  for (let pass = 0; pass < 3; pass++) {
    for (let i = a.length - 1; i > 0; i--) {
      const j = (Math.random() * (i + 1)) | 0;
      [a[i], a[j]] = [a[j], a[i]];
    }
  }
  if (a.length > 1) {
    const cut = (Math.random() * a.length) | 0;
    if (cut > 0) a.push(...a.splice(0, cut));
  }
}

function randomChoice(list) {
  return list[(Math.random() * list.length) | 0];
}

function randomBool() {
  return Math.random() < 0.5;
}

function getPlayer(room, role) {
  return room.players.find(p => p.role === role) || null;
}

function nextRole(room, role) {
  const idx = room.roles.indexOf(role);
  if (idx === -1) return room.roles[0];
  return room.roles[(idx + 1) % room.roles.length];
}

function buildRoles(maxPlayers) {
  return Array.from({ length: maxPlayers }, (_, i) => `p${i + 1}`);
}

function throwInRoles(room, state) {
  const roles = room.roles.filter(r => r !== state.defender);
  if (room.throwInMode !== "neighbors" || room.roles.length < 4) return roles;

  const idx = room.roles.indexOf(state.defender);
  const prev = room.roles[(idx - 1 + room.roles.length) % room.roles.length];
  const next = room.roles[(idx + 1) % room.roles.length];
  return roles.filter(r => r === prev || r === next);
}

function dealUpTo6(state, role) {
  const hand = state.players[role].hand;
  while (hand.length < 6 && state.deck.length > 0) hand.push(state.deck.shift());
}

function dealRound(state, room) {
  let r = state.attacker;
  for (let i = 0; i < room.roles.length; i++) {
    dealUpTo6(state, r);
    r = nextRole(room, r);
  }
}

function cardBeats(trumpSuit, def, atk) {
  if (def.suit === atk.suit) return RANK_VALUE.get(def.rank) > RANK_VALUE.get(atk.rank);
  if (def.suit === trumpSuit && atk.suit !== trumpSuit) return true;
  return false;
}

function ranksOnTable(state) {
  const set = new Set();
  for (const p of state.table) {
    if (!p) continue;
    set.add(p.attack.rank);
    if (p.defend) set.add(p.defend.rank);
  }
  return set;
}

function canAttackAddMore(state) {
  const totalPairs = state.table.filter(Boolean).length;
  const defenderHand = state.players?.[state.defender]?.hand?.length ?? 0;
  const defendedCount = state.table.filter(p => p && p.defend).length;
  const roundPairsLimit = state.roundIndex === 1 ? 5 : 6;
  const maxPairs = Math.min(roundPairsLimit, defenderHand + defendedCount);
  if (maxPairs <= 0) return false;
  return totalPairs < maxPairs;
}

function isOutClassic(room, state, role) {
  if (room.winMode !== "classic") return false;
  return state.deck.length === 0 && (state.players?.[role]?.hand?.length || 0) === 0;
}

function isOutDraw(room, state, role) {
  if (room.winMode !== "draw") return false;
  return (state.players?.[role]?.hand?.length || 0) === 0;
}

function isOutRole(room, state, role) {
  return isOutClassic(room, state, role) || isOutDraw(room, state, role);
}

function nextActiveRole(room, state, fromRole) {
  let cur = fromRole;
  for (let i=0;i<room.roles.length;i++) {
    cur = nextRole(room, cur);
    if (!isOutRole(room, state, cur)) return cur;
  }
  return fromRole;
}

function computeActiveRole(state, room) {
  if (!state || state.phase === "finished") return null;

  const hasUndefended = state.table.some(p => p && !p.defend);
  if ((state.phase === "defend" || state.phase === "taking") && hasUndefended) return state.defender;

  // when attack phase and table empty -> attacker
  return state.attacker;
}

function normalizeTurn(room, state) {
  if (!state || state.phase === "finished") return;

  if (room.roles.length < 2) return;

  if (state.attacker && !room.roles.includes(state.attacker)) state.attacker = room.roles[0];
  if (state.defender && !room.roles.includes(state.defender)) state.defender = nextRole(room, state.attacker);

  if (isOutRole(room, state, state.attacker)) {
    state.attacker = nextActiveRole(room, state, state.attacker);
  }
  if (isOutRole(room, state, state.defender) || state.defender === state.attacker) {
    state.defender = nextActiveRole(room, state, state.attacker);
  }
}

function resetTakingPass(room, state) {
  state.takingPass = {};
  for (const r of throwInRoles(room, state)) state.takingPass[r] = false;
  state.takingLeaderPassed = false;
}

function allAttackersPassed(room, state) {
  const ats = throwInRoles(room, state);
  return ats.every(r => state.takingPass?.[r] === true);
}

function finalizeTakingRound(room, state, defender) {
  const takingReason = state.takingReason || "take";

  if (takingReason === "take") {
    const takeTo = state.players[defender].hand;
    for (const pair of state.table) {
      if (!pair) continue;
      takeTo.push(pair.attack);
      if (pair.defend) takeTo.push(pair.defend);
    }
    state.table = [null,null,null,null,null,null];

    if (checkImmediateClassicWin(room, state)) return state;

    dealRound(state, room);

    state.attacker = nextActiveRole(room, state, defender);
    state.defender = nextActiveRole(room, state, state.attacker);
  } else {
    for (const pair of state.table) {
      if (!pair) continue;
      state.discard.push(pair.attack);
      if (pair.defend) state.discard.push(pair.defend);
    }
    state.table = [null,null,null,null,null,null];

    if (checkImmediateClassicWin(room, state)) return state;

    dealRound(state, room);

    state.attacker = defender;
    if (isOutRole(room, state, state.attacker)) state.attacker = nextActiveRole(room, state, state.attacker);
    state.defender = nextActiveRole(room, state, state.attacker);
  }

  state.phase = "attack";
  state.roundIndex += 1;
  state.message = "";
  state.takingReason = null;
  resetTakingPass(room, state);

  checkFinish(room, state);
  return state;
}

function roleCanActInTaking(state, role, attacker, defender) {
  if (role === defender) return false;
  if (state.takingPass?.[role]) return false;
  if (state.takingReason === "bito" && role !== attacker && !state.takingLeaderPassed) return false;
  return true;
}

function lowestTrumpAttacker(room, state) {
  let bestRole = null;
  let bestValue = Infinity;
  for (const r of room.roles) {
    const hand = state.players?.[r]?.hand || [];
    for (const c of hand) {
      if (c.suit !== state.trumpSuit) continue;
      const val = RANK_VALUE.get(c.rank);
      if (typeof val !== "number") continue;
      if (val < bestValue) {
        bestValue = val;
        bestRole = r;
      }
    }
  }
  return bestRole;
}

function finishFirstOut(state, room, winners) {
  const winnerList = Array.isArray(winners) ? winners : [winners];
  state.phase = "finished";
  state.winners = winnerList;
  state.loser = room.roles.find(r => !winnerList.includes(r)) || null;
  state.message = winnerList.length > 1 ? "Победа: несколько игроков без карт." : "Победа: первый без карт.";
  state.activeRole = null;
  state.deadlineTs = null;
}

function finishClassic(state, room, loser) {
  state.phase = "finished";
  state.loser = loser;
  state.winners = room.roles.filter(r => r !== loser);
  state.message = loser ? "Игра окончена." : "Ничья.";
  state.activeRole = null;
  state.deadlineTs = null;
}

function finishClassicImmediate(state, room, winner) {
  state.phase = "finished";
  state.loser = null;
  state.winners = winner ? [winner] : [];
  state.message = "Победа.";
  state.activeRole = null;
  state.deadlineTs = null;
}

function finishDraw(state, room) {
  state.phase = "finished";
  state.loser = null;
  state.winners = room.roles.slice();
  state.message = "Ничья.";
  state.activeRole = null;
  state.deadlineTs = null;
}

function checkImmediateClassicWin(room, state) {
  if (state.phase === "finished") return true;
  if (room.winMode !== "classic") return false;
  const winner = room.roles.find(r => (state.players?.[r]?.hand?.length || 0) === 0);
  if (!winner) return false;
  finishClassicImmediate(state, room, winner);
  return true;
}

function checkFinish(room, state) {
  if (state.phase === "finished") return true;

  if (room.roles.length < 2) {
    // если осталось 1 игрок — он победил
    const only = room.roles[0] || null;
    state.phase = "finished";
    state.winners = only ? [only] : [];
    state.loser = null;
    state.message = "Игра окончена.";
    state.activeRole = null;
    state.deadlineTs = null;
    return true;
  }

  if (room.winMode === "classic") {
    if (checkImmediateClassicWin(room, state)) return true;
    return false;
  }

  if (room.winMode === "firstout") {
    const winners = room.roles.filter(r => state.players[r].hand.length === 0);
    if (winners.length > 0) {
      finishFirstOut(state, room, winners);
      return true;
    }
    return false;
  }

  if (room.winMode === "draw") {
    if (state.deck.length > 0) return false;
    const withCards = room.roles.filter(r => state.players[r].hand.length > 0);
    if (withCards.length === 0) { finishDraw(state, room); return true; }
    if (withCards.length === 1) { finishClassic(state, room, withCards[0]); return true; }
    return false;
  }

  return false;
}

function createNewGame(room) {
  const deck = createDeck();
  shuffle(deck);

  const trumpCard = deck[deck.length - 1];
  const trumpSuit = trumpCard.suit;

  const state = {
    trumpSuit,
    trumpCard,
    deck,
    discard: [],
    table: [null,null,null,null,null,null],
    attacker: room.roles[0],
    defender: nextRole(room, room.roles[0]),
    phase: "attack",
    roundIndex: 1,
    players: {},
    message: "",
    winners: [],
    loser: null,
    activeRole: null,
    deadlineTs: null,
    takingPass: {},
    takingLeaderPassed: false,
    takingReason: null,
  };

  for (const r of room.roles) state.players[r] = { hand: [] };
  for (const r of room.roles) dealUpTo6(state, r);

  const firstAttacker = lowestTrumpAttacker(room, state);
  if (firstAttacker) {
    state.attacker = firstAttacker;
    state.defender = nextRole(room, firstAttacker);
  }

  return state;
}

function removeRoleFromGame(room, state, role, toDiscard = true) {
  if (!room.roles.includes(role)) return;

  // move hand to discard
  if (toDiscard && state?.players?.[role]?.hand) {
    for (const c of state.players[role].hand) state.discard.push(c);
    state.players[role].hand.length = 0;
  }

  // remove from roles
  room.roles = room.roles.filter(r => r !== role);

  // remove player hand holder
  if (state.players[role]) delete state.players[role];

  // if role was defender/attacker fix
  if (state.attacker === role) state.attacker = room.roles[0] || null;
  if (state.defender === role) state.defender = room.roles[0] ? nextRole(room, state.attacker) : null;

  // remove any table cards that belong to role? table cards are anonymous, keep.
  // re-init takingPass
  resetTakingPass(room, state);

  // normalize
  normalizeTurn(room, state);
}

function canTransfer(room, state, role, card) {
  if (!room.allowTransfer) return false;
  if (state.defender !== role) return false;
  if (state.phase !== "defend") return false;

  // Transfer allowed only if there is at least one attack and NO defended cards yet
  const anyAttack = state.table.some(p => p && p.attack);
  if (!anyAttack) return false;
  const anyDefended = state.table.some(p => p && p.defend);
  if (anyDefended) return false;

  // Card rank must match rank on table (any attack)
  const ranks = ranksOnTable(state);
  return ranks.has(card.rank);
}

function applyAction(room, state, role, action) {
  if (!action?.kind) return null;
  if (state.phase === "finished") return null;
  if (!room.roles.includes(role)) return null;

  const attacker = state.attacker;
  const defender = state.defender;

  // resign
  if (action.kind === "resign") {
    // 2p: immediate finish
    if (room.roles.length === 2) {
      state.phase = "finished";
      state.winners = [room.roles.find(r => r !== role)];
      state.loser = role;
      state.message = "Сдача.";
      state.activeRole = null;
      state.deadlineTs = null;
      return state;
    }

    // 3+ classic: remove resigning role, move hand to discard, continue
    if (room.roles.length >= 3 && room.winMode === "classic") {
      state.message = "Игрок сдался.";
      removeRoleFromGame(room, state, role, true);

      if (room.roles.length < 2) {
        checkFinish(room, state);
        return state;
      }

      // if table has cards, keep them; turn must normalize
      normalizeTurn(room, state);
      state.phase = "attack"; // упрощенно: после сдачи — продолжаем с атакой
      state.table = state.table; // keep
      checkFinish(room, state);
      return state;
    }

    // for 3+ firstout — считаем сдачу как поражение для игрока и конец (упрощаем)
    state.phase = "finished";
    state.winners = room.roles.filter(r => r !== role);
    state.loser = role;
    state.message = "Сдача.";
    state.activeRole = null;
    state.deadlineTs = null;
    return state;
  }

  // transfer
  if (action.kind === "transfer") {
    const cardId = action.cardId;
    if (!cardId) return null;
    const hand = state.players[role]?.hand;
    if (!hand) return null;
    const idx = hand.findIndex(c => c.id === cardId);
    if (idx === -1) return null;

    const card = hand[idx];
    if (!canTransfer(room, state, role, card)) return null;

    // put as new attack card into free slot
    const free = state.table.findIndex(p => !p);
    if (free === -1) return null;

    hand.splice(idx, 1);
    state.table[free] = { attack: card, defend: null };

    // defender becomes attacker, next player becomes defender
    const oldDef = state.defender;
    state.attacker = oldDef;
    state.defender = nextRole(room, oldDef);

    state.phase = "defend";
    state.message = "Перевод!";
    resetTakingPass(room, state);
    if (checkImmediateClassicWin(room, state)) return state;
    return state;
  }

  // attack
  if (action.kind === "attack") {
    const cardId = action.cardId;
    if (!cardId) return null;

    if (role === defender) return null;
    if (!(state.phase === "attack" || state.phase === "defend" || state.phase === "taking")) return null;
	if (state.phase === "taking") {
      if (!roleCanActInTaking(state, role, attacker, defender)) return null;
    }

    const hand = state.players[role]?.hand;
    if (!hand) return null;
    const idx = hand.findIndex(c => c.id === cardId);
    if (idx === -1) return null;

    const curPairs = state.table.filter(Boolean).length;

    if (curPairs === 0) {
      if (role !== attacker) return null;
      if (!canAttackAddMore(state)) return null;
      const free = state.table.findIndex(p => !p);
      if (free === -1) return null;
      const card = hand.splice(idx, 1)[0];
      state.table[free] = { attack: card, defend: null };
      state.phase = "defend";
      state.message = "";
      if (checkImmediateClassicWin(room, state)) return state;
      return state;
    }

    if (role !== attacker && state.phase !== "taking") return null;

    const allowedThrowIn = throwInRoles(room, state);
    if (!allowedThrowIn.includes(role)) return null;

    if (!canAttackAddMore(state)) return null;
    const ranks = ranksOnTable(state);
    if (!ranks.has(hand[idx].rank)) return null;

    const free = state.table.findIndex(p => !p);
    if (free === -1) return null;

    const card = hand.splice(idx, 1)[0];
    state.table[free] = { attack: card, defend: null };
    state.phase = "defend";
    state.message = "";
    if (checkImmediateClassicWin(room, state)) return state;
    return state;
  }

  // defend
  if (action.kind === "defend") {
    if (role !== defender) return null;
    if (state.phase !== "defend") return null;

    const { cardId, targetAttackId } = action;
    if (!cardId || !targetAttackId) return null;

    const hand = state.players[role]?.hand;
    if (!hand) return null;
    const idx = hand.findIndex(c => c.id === cardId);
    if (idx === -1) return null;

    const pairIndex = state.table.findIndex(p => p && p.attack.id === targetAttackId);
    if (pairIndex === -1) return null;

    const pair = state.table[pairIndex];
    if (pair.defend) return null;

    const defCard = hand[idx];
    if (!cardBeats(state.trumpSuit, defCard, pair.attack)) return null;

    pair.defend = hand.splice(idx, 1)[0];

	if (state.table.filter(Boolean).every(p => p.defend)) {
      state.phase = "taking";
      state.message = "Отбито. Подкиньте или нажмите ПАС.";
      state.takingReason = "bito";
      resetTakingPass(room, state);
    } else {
      state.phase = "defend";
    }
    if (state.phase !== "taking") state.message = "";
    if (checkImmediateClassicWin(room, state)) return state;
    return state;
  }

  // take
  if (action.kind === "take") {
    if (role !== defender) return null;
    const curPairs = state.table.filter(Boolean).length;
    if (curPairs === 0) return null;
    if (state.phase === "taking") return null;

    state.phase = "taking";
    state.message = "Защитник берёт. Подкиньте или нажмите ПАС.";
    state.takingReason = "take";
    resetTakingPass(room, state);
    return state;
  }

  // pass
  if (action.kind === "pass") {
if (state.phase !== "taking") return null;
    if (!roleCanActInTaking(state, role, attacker, defender)) return null;

    if (Object.prototype.hasOwnProperty.call(state.takingPass, role)) {
      state.takingPass[role] = true;
    }
    if (role === attacker) {
      state.takingLeaderPassed = true;
    }

    if (!allAttackersPassed(room, state)) return state;

return finalizeTakingRound(room, state, defender);
  }

  // end (bito)
  if (action.kind === "end") {
    if (state.phase === "taking") {
      if (!roleCanActInTaking(state, role, attacker, defender)) return null;

      if (Object.prototype.hasOwnProperty.call(state.takingPass, role)) {
        state.takingPass[role] = true;
      }
      if (role === attacker) state.takingLeaderPassed = true;
      if (!allAttackersPassed(room, state)) return state;
      return finalizeTakingRound(room, state, defender);
    }

    if (role !== attacker) return null;
    const curPairs = state.table.filter(Boolean).length;
    if (curPairs === 0) return null;
    if (!state.table.filter(Boolean).every(p => p.defend)) return null;

    state.phase = "taking";
    state.message = "Отбито. Подкиньте или нажмите ПАС.";
    state.takingReason = "bito";
    resetTakingPass(room, state);
    return state;
  }

  return null;
}

/** ======================
 *  Timers
 ======================= */

function clearRoomTimer(room) {
  if (room.timer) clearTimeout(room.timer);
  room.timer = null;
}

function scheduleTurnTimer(room) {
  if (!room.state) return;
  clearRoomTimer(room);

  const state = room.state;
  if (state.phase === "finished") return;

  normalizeTurn(room, state);

  state.activeRole = computeActiveRole(state, room);
  state.deadlineTs = now() + TURN_MS;

  room.timer = setTimeout(() => onTimeout(room), TURN_MS + 80);
}

function onTimeout(room) {
  if (!room.state) return;
  const state = room.state;
  if (state.phase === "finished") return;

  normalizeTurn(room, state);

  const active = computeActiveRole(state, room);
  if (!active) return;

  const anyOnTable = state.table.some(Boolean);
  const allDef = anyOnTable && state.table.filter(Boolean).every(p => p.defend);

  if (active === state.defender && anyOnTable) {
    const next = applyAction(room, state, state.defender, { kind:"take" });
    if (next) room.state = next;
    scheduleTurnTimer(room);
    broadcastGameState(room);
    return;
  }

  if (active === state.attacker && allDef && state.phase === "attack") {
    const next = applyAction(room, state, state.attacker, { kind:"end" });
    if (next) room.state = next;
    scheduleTurnTimer(room);
    broadcastGameState(room);
    return;
  }

  // resign active
  const next = applyAction(room, state, active, { kind:"resign" });
  if (next) room.state = next;
  clearRoomTimer(room);
  broadcastGameState(room);
}

/** ======================
 *  State sanitize
 ======================= */

function sanitizeStateFor(room, role, state) {
  const profiles = {};
  for (const r of room.roles) profiles[r] = getPlayer(room, r)?.profile || { name:"", username:"", photoUrl:"" };

  const handsCount = {};
  for (const r of room.roles) handsCount[r] = state.players[r]?.hand?.length ?? 0;

  // include allowTransfer so UI can show zone
  return {
    youRole: role,
    roles: room.roles.slice(),
    profiles,
    winMode: room.winMode,
    allowTransfer: !!room.allowTransfer,
    throwInMode: room.throwInMode || "all",

    trumpSuit: state.trumpSuit,
    trumpCard: state.trumpCard ? { suit: state.trumpCard.suit, rank: state.trumpCard.rank } : null,
    deckCount: state.deck.length,
    discardCount: state.discard.length,

    attacker: state.attacker,
    defender: state.defender,
    attackers: throwInRoles(room, state),

    phase: state.phase,
    roundIndex: state.roundIndex,
    message: state.message || "",
    takingPass: state.takingPass || {},
    takingLeaderPassed: !!state.takingLeaderPassed,
    takingReason: state.takingReason || null,

    activeRole: state.activeRole,
    deadlineTs: state.deadlineTs,
    turnMs: TURN_MS,

    table: state.table.map(p => {
      if (!p) return null;
      return {
        attack: { id:p.attack.id, suit:p.attack.suit, rank:p.attack.rank },
        defend: p.defend ? { id:p.defend.id, suit:p.defend.suit, rank:p.defend.rank } : null
      };
    }),

    yourHand: state.players[role].hand.map(c => ({ id:c.id, suit:c.suit, rank:c.rank })),

    handsCount,
    winners: state.winners || [],
    loser: state.loser || null,
    winnersSeat: (state.winners || []).map(r => room.roles.indexOf(r)).filter(i => i >= 0),
    loserSeat: state.loser ? (room.roles.indexOf(state.loser) >= 0 ? room.roles.indexOf(state.loser) : null) : null
  };
}

/** ======================
 *  Lobby state
 ======================= */

function sendLobbyState(room) {
  const profiles = {};
  for (const r of room.roles) profiles[r] = { name:"", username:"", photoUrl:"" };
  for (const p of room.players) profiles[p.role] = p.profile;

  const players = room.roles.map(role => {
    const pl = getPlayer(room, role);
    return pl ? { role, userKey: pl.userKey, ready: !!pl.ready, connected: !!pl.ws } : { role, userKey:null, ready:false, connected:false };
  });

  for (const pl of room.players) {
    if (pl.ws && pl.ws.readyState === 1) {
      send(pl.ws, {
        type:"lobby_state",
        roomId: room.id,
        isPrivate: !!room.isPrivate,
        maxPlayers: room.maxPlayers,
        roles: room.roles,
        winMode: room.winMode,
        allowTransfer: !!room.allowTransfer,
        throwInMode: room.throwInMode || "all",
        readyDeadlineTs: room.readyDeadlineTs,
        readyTimeoutMs: READY_MS,
        youRole: pl.role,
        players,
        profiles
      });
    }
  }
}

function tryStartGame(room) {
  if (room.state) return;
  const filled = room.roles.every(r => !!getPlayer(room, r));
  if (!filled) return;
  const allReady = room.roles.every(r => !!getPlayer(room, r)?.ready);
  if (!allReady) return;

  room.startedAt = now();
  room.state = createNewGame(room);
  clearReadyTimer(room);
  for (const p of room.players) p.ready = false;

  broadcastLobbyLists(); // hide from list now
  scheduleTurnTimer(room);
  broadcastGameState(room);
}

function broadcastGameState(room) {
  const state = room.state;
  if (!state) return;

  normalizeTurn(room, state);
  state.activeRole = computeActiveRole(state, room);

  for (const pl of room.players) {
    if (pl.ws && pl.ws.readyState === 1) {
      send(pl.ws, { type:"state", state: sanitizeStateFor(room, pl.role, state) });
    }
  }

  // if finished -> schedule reset back to lobby mode
  if (state.phase === "finished") {
    clearRoomTimer(room);
    scheduleFinishToLobby(room);
  }
}

function scheduleFinishToLobby(room) {
  if (room.finishResetTimer) return;
  room.finishResetTimer = setTimeout(() => {
    room.finishResetTimer = null;

    // Convert room back to lobby (so remaining can wait and start again)
    // Keep roles/players, but if roles < maxPlayers we still show in list
    room.state = null;
    room.startedAt = null;

    // ready flags reset
    for (const p of room.players) p.ready = false;

    scheduleReadyCountdown(room);
    sendLobbyState(room);
    broadcastLobbyLists();
  }, FINISH_TO_LOBBY_MS);
}

/** ======================
 *  Reconnect & leave
 ======================= */

function attachToRoomByUserKey(ws, userKey, profile) {
  const roomId = userToRoom.get(userKey);
  if (!roomId) return false;

  const room = rooms.get(roomId);
  if (!room) return false;

  const pl = room.players.find(p => p.userKey === userKey);
  if (!pl) return false;

  if (pl.ws && pl.ws !== ws) kickWs(pl.ws);
  pl.ws = ws;
  pl.profile = profile;
  ws.userKey = userKey;
  ws.roomId = roomId;

  if (room.disconnectTimers?.has(userKey)) {
    clearTimeout(room.disconnectTimers.get(userKey));
    room.disconnectTimers.delete(userKey);
  }

  broadcastLobbyLists();

  if (room.state) {
    send(ws, { type:"state", state: sanitizeStateFor(room, pl.role, room.state) });
  } else {
    scheduleReadyCountdown(room);
    sendLobbyState(room);
  }
  return true;
}

function deleteRoomIfEmpty(room, { broadcast = true } = {}) {
  const count = room.players.filter(p => !!p.userKey).length;
  if (count !== 0) return false;

  clearRoomTimer(room);
  clearReadyTimer(room);
  if (room.finishResetTimer) clearTimeout(room.finishResetTimer);
  rooms.delete(room.id);

  if (broadcast) broadcastLobbyLists();
  return true;
}

function leaveRoom(ws) {
  const roomId = ws.roomId;
  const userKey = ws.userKey;
  if (!roomId || !userKey) return;

  const room = rooms.get(roomId);
  if (!room) return;

  const pl = room.players.find(p => p.userKey === userKey);
  if (!pl) return;

  pl.ws = null;

  if (!room.disconnectTimers) room.disconnectTimers = new Map();

  if (room.disconnectTimers.has(userKey)) clearTimeout(room.disconnectTimers.get(userKey));

  if (room.state && room.state.phase !== "finished") {
    const t = setTimeout(() => {
      const still = room.players.find(p => p.userKey === userKey && !p.ws);
      if (!still) return;

      userToRoom.delete(userKey);

      const st = room.state;
      const next = applyAction(room, st, still.role, { kind:"resign" });
      if (next) room.state = next;

      room.players = room.players.filter(p => p.userKey !== userKey);

      if (room.players.length === 0) {
        deleteRoomIfEmpty(room);
        return;
      }

      broadcastGameState(room);
      broadcastLobbyLists();
    }, REJOIN_GRACE_MS);

    room.disconnectTimers.set(userKey, t);
    broadcastLobbyLists();
    return;
  }

  // lobby or finished state: remove immediately
  userToRoom.delete(userKey);
  room.players = room.players.filter(p => p.userKey !== userKey);
  if (room.players.length === 0) {
    deleteRoomIfEmpty(room);
    return;
  }
  scheduleReadyCountdown(room);
  sendLobbyState(room);
  broadcastLobbyLists();
}

/** ======================
 *  Create / Join / Quick
 ======================= */

function createRoomForHost({ hostWs, userKey, profile, maxPlayers, winMode, allowTransfer, throwInMode, isPrivate, password }) {
  const roomId = randId(isPrivate ? "PR" : "PU");
  const roles = buildRoles(maxPlayers);

  const room = {
    id: roomId,
    isPrivate: !!isPrivate,
    password: isPrivate ? safeStr(password || "", 32) : "",
    maxPlayers,
    winMode,
    allowTransfer: !!allowTransfer,
    throwInMode: throwInMode || "all",
    roles,
    createdAt: now(),
    startedAt: null,
    players: [],
    state: null,
    timer: null,
    disconnectTimers: new Map(),
    finishResetTimer: null,
    readyTimer: null,
    readyDeadlineTs: null
  };

  const host = { ws: hostWs, userKey, role:"p1", profile, ready:false };
  room.players.push(host);

  rooms.set(roomId, room);
  userToRoom.set(userKey, roomId);

  hostWs.roomId = roomId;
  hostWs.userKey = userKey;

  send(hostWs, { type:"lobby_created", roomId, youRole:"p1" });
  scheduleReadyCountdown(room);
  sendLobbyState(room);
  broadcastLobbyLists();
  return room;
}

function joinRoom({ ws, userKey, profile, roomId, password }) {
  const room = rooms.get(roomId);
  if (!room) {
    send(ws, { type:"error", message:"Комната не найдена" });
    return null;
  }

  if (room.isPrivate) {
    const pass = safeStr(password || "", 32);
    if (!room.password || pass !== room.password) {
      send(ws, { type:"error", message:"Неверный пароль" });
      return null;
    }
  }

  // reconnect if exists
  const existing = room.players.find(p => p.userKey === userKey);
  if (existing) {
    if (existing.ws && existing.ws !== ws) kickWs(existing.ws);
    existing.ws = ws;
    existing.profile = profile;

    userToRoom.set(userKey, room.id);
    ws.roomId = room.id;
    ws.userKey = userKey;

    if (room.disconnectTimers.has(userKey)) {
      clearTimeout(room.disconnectTimers.get(userKey));
      room.disconnectTimers.delete(userKey);
    }

    send(ws, { type:"joined", roomId: room.id, youRole: existing.role });
    if (room.state) send(ws, { type:"state", state: sanitizeStateFor(room, existing.role, room.state) });
    else {
      scheduleReadyCountdown(room);
      sendLobbyState(room);
    }

    broadcastLobbyLists();
    return room;
  }

  // Can't join while game active (hidden anyway), unless finished reset in progress: allow join if no active game
  if (room.state && room.state.phase !== "finished") {
    send(ws, { type:"error", message:"Игра уже началась" });
    return null;
  }

  // if finished but not yet reset -> allow join (he will see finished state then lobby)
  // find free role
  const used = new Set(room.players.map(p => p.role));
  const free = room.roles.find(r => !used.has(r));
  if (!free) {
    send(ws, { type:"error", message:"Комната заполнена" });
    return null;
  }

  room.players.push({ ws, userKey, role: free, profile, ready:false });
  userToRoom.set(userKey, room.id);
  ws.roomId = room.id;
  ws.userKey = userKey;

  send(ws, { type:"joined", roomId: room.id, youRole: free });

  if (room.state) send(ws, { type:"state", state: sanitizeStateFor(room, free, room.state) });
  else {
    scheduleReadyCountdown(room);
    sendLobbyState(room);
  }

  broadcastLobbyLists();
  return room;
}

/** ======================
 *  WS handlers
 ======================= */

wss.on("connection", (ws) => {
  ws.userKey = null;
  ws.roomId = null;

  send(ws, { type:"hello_ack" });
  send(ws, { type:"lobbies", ...buildLobbyLists() });

  ws.on("message", (raw) => {
    let msg;
    try { msg = JSON.parse(raw); } catch { return; }

    // HELLO
    if (msg.type === "hello") {
      const userKey = safeStr(msg.userKey || "", 128);
      if (!userKey) return;
      const profile = sanitizeProfile(msg.profile);

      if (attachToRoomByUserKey(ws, userKey, profile)) return;

      ws.userKey = userKey;
      send(ws, { type:"lobbies", ...buildLobbyLists() });
      return;
    }

    if (msg.type === "profile_update") {
      const userKey = safeStr(ws.userKey || "", 128);
      if (!userKey) return;
      const profile = sanitizeProfile(msg.profile);
      const roomId = ws.roomId;
      if (!roomId) return;
      const room = rooms.get(roomId);
      if (!room) return;
      const pl = room.players.find(p => p.userKey === userKey);
      if (!pl) return;
      pl.profile = profile;
      if (room.state) broadcastGameState(room);
      else sendLobbyState(room);
      broadcastLobbyLists();
      return;
    }

    if (msg.type === "list_lobbies") {
      send(ws, { type:"lobbies", ...buildLobbyLists() });
	  return;
  }

  // QUICK
  if (msg.type === "quick_start") {
      const userKey = safeStr(ws.userKey || msg.userKey || "", 128);
      if (!userKey) { send(ws, { type:"error", message:"Нет userKey" }); return; }

      const profile = sanitizeProfile(msg.profile);

      // Быстрая игра: переводной + подкид ото всех (стандарт)
      const winMode = randomChoice(["classic", "draw"]);
      const allowTransfer = true;
      const throwInMode = "all";

      // 1) ищем любое свободное публичное лобби (не приватное), где игра ещё не идёт
      let found = null;
      for (const r of rooms.values()) {
        if (!r || r.isPrivate) continue;
        if (r.state?.phase && r.state.phase !== "lobby") continue; // игра уже идёт/закончена
        const playersCount = (r.players || []).length;
        if (playersCount >= (r.maxPlayers || 2)) continue;
        found = r;
        break;
      }

      if (found) {
        joinRoom({ ws, userKey, profile, roomId: found.id });
        send(ws, { type:"quick_status", searching:false, matched:true, roomId: found.id });
        return;
      }

      // 2) иначе — создать новое публичное лобби на 2
      const room = createRoomForHost({
        hostWs: ws,
        userKey,
        profile,
        maxPlayers: 2,
        winMode,
        allowTransfer,
        throwInMode,
        isPrivate: false,
        password: ""
      });

      send(ws, { type:"quick_status", searching:false, matched:true, roomId: room.id });
      return;
    }


    if (msg.type === "quick_cancel") {
      const userKey = safeStr(ws.userKey || msg.userKey || "", 128);
      if (quickQueue.waiting && quickQueue.waiting.userKey === userKey) quickQueue.waiting = null;
      send(ws, { type:"quick_status", searching:false });
      return;
    }
    // AVATAR EMOJI (broadcast)
    if (msg.type === "emoji") {
      const userKey = safeStr(ws.userKey || "", 128);
      if (!userKey) return;

      const emoji = safeStr(msg.emoji || "", 8);
      const allowed = new Set(["🤡","😎","😘"]);
      if (!allowed.has(emoji)) return;

      const room = getRoomByUserKey(userKey);
      if (!room) return;

      // role of this user in this room
      const pl = (room.players || []).find(p => p && p.userKey === userKey) || null;
      const role = pl?.role || "";
      if (!role) return;

      const seat = room.roles.indexOf(role);
      broadcastToRoom(room, { type:"emoji", seat, role, emoji });
      return;
    }

    // CREATE
    if (msg.type === "create_lobby") {
      const userKey = safeStr(ws.userKey || msg.userKey || "", 128);
      if (!userKey) { send(ws, { type:"error", message:"Нет userKey" }); return; }

      const profile = sanitizeProfile(msg.profile);
      const requestedPlayers = Number(msg.maxPlayers);
      if (!Number.isInteger(requestedPlayers) || requestedPlayers < 2 || requestedPlayers > 6) {
        send(ws, { type:"error", message:"Некорректное количество игроков" });
        return;
      }
      const maxPlayers = requestedPlayers;
      const winMode = (msg.winMode === "draw") ? "draw" : "classic";
      const allowTransfer = !!msg.allowTransfer;
      const throwInMode = (msg.throwInMode === "neighbors" && maxPlayers >= 4) ? "neighbors" : "all";
      const isPrivate = !!msg.isPrivate;
      const password = safeStr(msg.password || "", 32);

      createRoomForHost({ hostWs: ws, userKey, profile, maxPlayers, winMode, allowTransfer, throwInMode, isPrivate, password });
      return;
    }

    // JOIN
    if (msg.type === "join_lobby") {
      const userKey = safeStr(ws.userKey || msg.userKey || "", 128);
      if (!userKey) { send(ws, { type:"error", message:"Нет userKey" }); return; }

      const profile = sanitizeProfile(msg.profile);
      const roomId = safeStr(msg.roomId || "", 64);
      const password = safeStr(msg.password || "", 32);

      joinRoom({ ws, userKey, profile, roomId, password });
      return;
    }

    if (msg.type === "leave_lobby") {
      const roomId = ws.roomId;
      if (!roomId) return;
      const room = rooms.get(roomId);
      if (!room) { ws.roomId = null; return; }
      if (room.state && room.state.phase !== "finished") return;

      const userKey = ws.userKey;
      const pl = room.players.find(p => p.userKey === userKey);
      if (!pl) { ws.roomId = null; return; }

      if (room.disconnectTimers?.has(userKey)) {
        clearTimeout(room.disconnectTimers.get(userKey));
        room.disconnectTimers.delete(userKey);
      }

      userToRoom.delete(userKey);
      room.players = room.players.filter(p => p.userKey !== userKey);
      ws.roomId = null;

      if (room.players.length === 0) {
        deleteRoomIfEmpty(room);
        return;
      }

      scheduleReadyCountdown(room);
      sendLobbyState(room);
      broadcastLobbyLists();
      return;
    }

    // READY
    if (msg.type === "ready") {
      const roomId = ws.roomId;
      if (!roomId) return;
      const room = rooms.get(roomId);
      if (!room) return;
      if (room.state) return;

      const userKey = ws.userKey;
      const pl = room.players.find(p => p.userKey === userKey);
      if (!pl) return;

      if (playersCount(room) !== room.maxPlayers) return;

      pl.ready = !!msg.ready;
      scheduleReadyCountdown(room);
      sendLobbyState(room);
      tryStartGame(room);
      return;
    }

    // ACTION
    if (msg.type === "action") {
      const roomId = ws.roomId;
      if (!roomId) return;
      const room = rooms.get(roomId);
      if (!room || !room.state) return;

      const userKey = ws.userKey;
      const pl = room.players.find(p => p.userKey === userKey);
      if (!pl) return;

      if (room.state.phase === "finished") return;

      const next = applyAction(room, room.state, pl.role, msg.action);
      if (!next) return;

      room.state = next;

      if (room.state.phase === "finished") {
        clearRoomTimer(room);
      } else {
        scheduleTurnTimer(room);
      }

      broadcastGameState(room);
      return;
    }
  });

  ws.on("close", () => {
    if (quickQueue.waiting && quickQueue.waiting.ws === ws) quickQueue.waiting = null;
    if (ws.kicked) return;
    if (ws.roomId && ws.userKey) leaveRoom(ws);
  });
});

console.log(`🃏 Durak WS started on ws://0.0.0.0:${PORT}`);