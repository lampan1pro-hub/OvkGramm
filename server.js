const express = require('express');
const http = require('http');
const WebSocket = require('ws');
const fs = require('fs');
const path = require('path');
const cors = require('cors');
const crypto = require('crypto');

const app = express();
const server = http.createServer(app);
const wss = new WebSocket.Server({ server });

app.use(cors());
app.use(express.json({ limit: '1mb' }));

// Каждый запрос к /api (кроме входа и регистрации) подписан токеном. Личность берём из токена,
// а не из userId, который прислал клиент: иначе можно действовать от чужого имени (и тратить чужие Mars).
const OPEN_API = new Set(['/register', '/register/code', '/login', '/health', '/config']);
app.use('/api', (req, res, next) => {
    if (OPEN_API.has(req.path)) return next();
    const token = String(req.headers.authorization || '').replace(/^Bearer /, '');
    const s = token && read(F.sessions)[token];
    if (!s) return res.status(401).json({ error: 'Сессия устарела, войдите снова' });
    req.uid = s.uid;
    req.query.userId = String(s.uid);
    if (req.body && typeof req.body === 'object' && !Buffer.isBuffer(req.body)) req.body.userId = s.uid;
    next();
});

// ---------- Хранилище ----------
// Папку с данными можно вынести на постоянный диск хостинга: DATA_DIR=/путь/к/диску
const DIR = process.env.DATA_DIR ? path.resolve(process.env.DATA_DIR) : path.join(__dirname, 'data');
const UP = path.join(DIR, 'uploads');
fs.mkdirSync(UP, { recursive: true });
const F = {
    users: path.join(DIR, 'users.json'),
    messages: path.join(DIR, 'messages.json'),
    chats: path.join(DIR, 'chats.json'),
    gifts: path.join(DIR, 'gifts.json'),
    sessions: path.join(DIR, 'sessions.json'),
    blocks: path.join(DIR, 'blocks.json')
};

// ---- Хранение ----
// Данные живут в памяти и сохраняются в PostgreSQL (если задан DATABASE_URL) или в файлы.
// Файлы на бесплатных хостингах стираются при перезапуске и засыпании сервера,
// поэтому для постоянного хранения нужна внешняя база.
let pool = null;
const mem = {};              // users / chats / messages в виде JSON-строк
const dirty = new Set();     // что ещё не записано в базу
const keyOf = f => path.basename(f, '.json');

// Чтение файла. Если он повреждён — берём резервную копию (.bak), пустым список не подменяем
function loadFile(f) {
    const found = [f, f + '.bak'].filter(p => fs.existsSync(p));
    for (const p of found) {
        try {
            const d = JSON.parse(fs.readFileSync(p, 'utf8'));
            if (p !== f) {
                console.error(`⚠️ ${path.basename(f)} повреждён или потерян — восстановлен из резервной копии`);
                if (fs.existsSync(f)) fs.copyFileSync(f, `${f}.corrupt-${Date.now()}`);
                fs.copyFileSync(p, f);
            }
            return d;
        } catch {}
    }
    if (fs.existsSync(f)) {
        fs.renameSync(f, `${f}.corrupt-${Date.now()}`);
        console.error(`⚠️ ${path.basename(f)} нечитаем, сохранён как .corrupt-*`);
    }
    return [];
}

// Запись файла: временный файл, fsync, атомарное переименование
const lastBak = {};
function saveFile(f, json) {
    const tmp = f + '.tmp';
    const fd = fs.openSync(tmp, 'w');
    fs.writeSync(fd, json);
    fs.fsyncSync(fd);
    fs.closeSync(fd);
    if (fs.existsSync(f) && Date.now() - (lastBak[f] || 0) > 60000) {
        fs.copyFileSync(f, f + '.bak');
        lastBak[f] = Date.now();
    }
    fs.renameSync(tmp, f);
}

// Каждый read() отдаёт свежую копию, поэтому незавершённые правки не «протекают» в данные
const read = f => JSON.parse(mem[keyOf(f)]);
function write(f, d) {
    const k = keyOf(f), json = JSON.stringify(d);
    mem[k] = json;
    if (pool) { dirty.add(k); scheduleFlush(); } else saveFile(f, json);
}

let flushT = null, flushing = null;
function scheduleFlush() {
    if (!flushT) flushT = setTimeout(() => { flushT = null; persist(); }, 400);
}
// Записать накопленное в базу прямо сейчас (не бросает исключений)
async function persist() {
    if (!pool) return;
    if (flushT) { clearTimeout(flushT); flushT = null; }
    while (flushing) await flushing;
    if (!dirty.size) return;
    const keys = [...dirty];
    dirty.clear();
    flushing = (async () => {
        for (const k of keys) {
            try {
                await pool.query(
                    'insert into kv (key, value) values ($1, $2::jsonb) on conflict (key) do update set value = excluded.value, updated_at = now()',
                    [k, mem[k]]);
            } catch (e) {
                console.error('DB: не удалось сохранить', k, '-', e.message);
                dirty.add(k);
                setTimeout(scheduleFlush, 3000);
            }
        }
    })();
    try { await flushing; } finally { flushing = null; }
}

// Отдаём только страницу и загруженные файлы (но не папку data целиком)
app.get('/', (req, res) => res.sendFile(path.join(__dirname, 'index.html')));
app.get('/uploads/:name', async (req, res) => {
    const name = req.params.name;
    if (!/^[\w-]+\.(jpg|png|webp|gif|mp4|webm|mov|weba|m4a|ogg|mp3)$/.test(name)) return res.sendStatus(404);
    const opts = { root: UP, dotfiles: 'allow', maxAge: '30d', immutable: true, headers: { 'X-Content-Type-Options': 'nosniff' } };
    try {
        if (!fs.existsSync(path.join(UP, name))) {
            if (!pool) return res.sendStatus(404);
            const r = await pool.query('select data from files where name = $1', [name]);
            if (!r.rowCount) return res.sendStatus(404);
            await fs.promises.writeFile(path.join(UP, name), r.rows[0].data); // кэш на диске, Range для видео работает
        }
        res.sendFile(name, opts);
    } catch (e) { console.error('uploads:', e.message); res.sendStatus(500); }
});

const directId = (a, b) => `d_${Math.min(a, b)}_${Math.max(a, b)}`;
const HANDLE_RE = /^[a-z0-9_]{3,20}$/;
// Занят ли handle кем-то ещё: проверяем ВСЕ юзернеймы пользователей (основной + «а также»)
const handleTaken = (users, h, exceptId) =>
    users.some(u => u.id !== exceptId && (u.handles || [u.handle]).includes(h));

// Файлы
const MIME = {
    'image/jpeg': 'jpg', 'image/png': 'png', 'image/webp': 'webp', 'image/gif': 'gif',
    'video/mp4': 'mp4', 'video/webm': 'webm', 'video/quicktime': 'mov',
    'audio/webm': 'weba', 'audio/ogg': 'ogg', 'audio/mp4': 'm4a', 'audio/mpeg': 'mp3'
};
const URL_RE = /^\/uploads\/[\w-]+\.(jpg|png|webp|gif|mp4|webm|mov|weba|m4a|ogg|mp3)$/;
const fileOk = u => typeof u === 'string' && URL_RE.test(u) && fs.existsSync(path.join(UP, path.basename(u)));
const kindOf = u => /\.(mp4|webm|mov)$/.test(u) ? 'video' : /\.(weba|m4a|ogg|mp3)$/.test(u) ? 'voice' : 'image';
const imageOk = u => fileOk(u) && kindOf(u) === 'image';

// Пароли: scrypt. Старые (открытым текстом) принимаются и сразу заменяются на хэш
const hashPass = (p, salt = crypto.randomBytes(8).toString('hex')) =>
    `${salt}:${crypto.scryptSync(p, salt, 32).toString('hex')}`;
const isHash = s => /^[0-9a-f]{16}:[0-9a-f]{64}$/.test(s);
const checkPass = (p, stored) => isHash(stored) ? hashPass(p, stored.split(':')[0]) === stored : p === stored;

const START_MARS = Number(process.env.START_MARS ?? 10); // стартовый баланс новых аккаунтов

function newSession(uid) {
    const sessions = read(F.sessions), token = crypto.randomBytes(24).toString('hex');
    sessions[token] = { uid, at: Date.now() };
    write(F.sessions, sessions);
    return token;
}

// Миграция старых данных
function migrate() {
    const users = read(F.users);
    users.forEach(u => {
        if (!u.handle) u.handle = 'user' + String(u.id).slice(-6);
        if (u.mars === undefined) u.mars = START_MARS;
    });
    write(F.users, users);

    const chats = read(F.chats).map(c => c.type ? c :
        { id: directId(...c.participants), type: 'direct', members: c.participants, createdAt: c.createdAt });
    const msgs = read(F.messages);
    msgs.forEach(m => {
        if (typeof m.chatId === 'string' && /^\d+-\d+$/.test(m.chatId)) {
            const [a, b] = m.chatId.split('-').map(Number);
            m.chatId = directId(a, b);
            if (!chats.some(c => c.id === m.chatId))
                chats.push({ id: m.chatId, type: 'direct', members: [a, b], createdAt: m.timestamp });
        }
    });
    // Всё, что было до обновления, считаем прочитанным
    chats.forEach(c => {
        if (c.reads) return;
        const last = msgs.filter(m => m.chatId === c.id).pop();
        c.reads = {};
        c.members.forEach(id => { c.reads[id] = last ? last.timestamp : ''; });
    });
    // Подарки, купленные до появления фонов, получают случайный фон; без узора — 'none'
    const gifts = read(F.gifts);
    let gch = false;
    gifts.forEach(g => {
        if (!g.bd) { g.bd = pickBackdrop().id; gch = true; }
        if (!g.pt) { g.pt = 'none'; gch = true; }
    });
    msgs.forEach(m => {
        if (m.gift && !m.gift.bd) {
            const g = gifts.find(x => x.kind === m.gift.kind && x.serial === m.gift.serial);
            if (g) m.gift.bd = g.bd;
        }
    });
    if (gch) write(F.gifts, gifts);
    write(F.messages, msgs);
    write(F.chats, chats);
}

// Метка «когда создана база»: если после перезапусков она каждый раз новая — данные не сохраняются
let meta = { createdAt: new Date().toISOString() };

app.get('/api/health', (req, res) => res.json({ ok: true, storage: pool ? 'database' : 'files', users: read(F.users).length, dbCreatedAt: meta.createdAt }));

// ---------- Онлайн ----------
const sockets = new Map(); // userId -> Set<ws>

// Галочка у аккаунтов с этими юзернеймами. Свой список: VERIFIED_HANDLES=saimon,durov,lesha
const VERIFIED = new Set((process.env.VERIFIED_HANDLES || 'saimon,durov,lesha,anna')
    .split(',').map(x => x.trim().replace(/^@/, '').toLowerCase()).filter(Boolean));

// Бесконечные Mars. Свой список: UNLIMITED_MARS_HANDLES=saimon,другой
const UNLIMITED = new Set((process.env.UNLIMITED_MARS_HANDLES || 'saimon')
    .split(',').map(x => x.trim().replace(/^@/, '').toLowerCase()).filter(Boolean));
const isUnlimited = u => UNLIMITED.has(u.handle);
const walletOf = u => ({ mars: u.mars || 0, unlimited: isUnlimited(u) });

const pub = u => ({
    id: u.id, username: u.username, handle: u.handle, verified: VERIFIED.has(u.handle), bio: u.bio || '',
    ...((u.handles || [u.handle]).length > 1 ? { others: u.handles.filter(h => h !== u.handle) } : {}),
    avatar: u.avatar || '', createdAt: u.createdAt, online: sockets.has(u.id), ...(u.bot && { bot: true }), ...(u.acc && { acc: u.acc })
});
const self = u => ({ ...pub(u), email: u.email, ...walletOf(u),
    ...(u.uListings && u.uListings.length ? { uListing: u.uListings.find(l => l.handle === u.handle) || u.uListings[0] } : {}) });

function sendTo(ids, payload, exceptWs) {
    const json = JSON.stringify(payload);
    ids.forEach(id => sockets.get(id)?.forEach(s => {
        if (s !== exceptWs && s.readyState === WebSocket.OPEN) s.send(json);
    }));
}
function broadcast(payload) {
    const json = JSON.stringify(payload);
    wss.clients.forEach(s => s.readyState === WebSocket.OPEN && s.send(json));
}

// Чат в виде, удобном клиенту. list — сообщения этого чата
// Реакции и просмотры постов канала
const REACTIONS = ['👍', '❤️', '🔥', '😂', '😮', '😢', '🎉', '👎'];
// Индекс пользователей: read() каждый раз парсит файл целиком, поэтому строим его лениво и один раз на запрос
const lazyIdx = () => { let mp; return () => mp || (mp = new Map(read(F.users).map(u => [u.id, u]))); };
const ixCache = new WeakMap();
const ixOf = arr => { let m = ixCache.get(arr); if (!m) { m = new Map(arr.map(u => [u.id, u])); ixCache.set(arr, m); } return m; };
const userLite = u => ({ id: u.id, username: u.username, avatar: u.avatar || '', verified: VERIFIED.has(u.handle), ...(u.acc && { acc: u.acc }) });
// Наружу отдаём сообщение без служебных полей.
// Лента канала: просмотры (настоящие + «накрученные» подписчики, которые просматривают пост постепенно) и комментарии.
// Текстовый канал (open): как группа — у сообщения есть имя отправителя.
function outMsg(m, chat, ix = lazyIdx()) {
    if (!m || !chat || chat.type !== 'channel') return m;
    const { viewers, fv, comments, ...o } = m;
    if (chat.open) {
        const u = ix().get(m.senderId);
        if (u) { o.sn = u.username; if (VERIFIED.has(u.handle)) o.sv = true; if (u.acc) o.sacc = u.acc; }
        return o;
    }
    const age = Date.now() - new Date(m.timestamp).getTime();
    o.views = (viewers ? viewers.length : 0) + Math.round((fv || 0) * Math.min(1, 0.25 + age / 240000));
    o.cc = comments ? comments.length : 0;
    if (o.cc) {
        const seen = [];
        for (let i = comments.length - 1; i >= 0 && seen.length < 3; i--) if (!seen.includes(comments[i].userId)) seen.push(comments[i].userId);
        o.ca = seen.map(id => ix().get(id)).filter(Boolean).map(u => ({ id: u.id, username: u.username, avatar: u.avatar || '' }));
    }
    return o;
}
const fakeInChannel = (chat, users) => {
    const fk = new Set(users.filter(u => u.fake).map(u => u.id));
    return chat.members.reduce((n, id) => n + (fk.has(id) ? 1 : 0), 0);
};
const rollFv = n => Math.round(n * (0.35 + Math.random() * 0.5));
// Участник открыл канал — все посты получают его просмотр
function markViews(chat, uid) {
    const msgs = read(F.messages);
    let items = null, fakeN = null;
    msgs.forEach(x => {
        if (x.chatId !== chat.id) return;
        let ch = false;
        if (x.fv === undefined) { if (fakeN === null) fakeN = fakeInChannel(chat, read(F.users)); x.fv = rollFv(fakeN); ch = true; }
        x.viewers = x.viewers || [];
        if (!x.viewers.includes(uid)) { x.viewers.push(uid); ch = true; }
        if (ch) (items = items || {})[x.id] = outMsg(x, chat).views;
    });
    if (!items) return;
    write(F.messages, msgs);
    sendTo([...new Set([uid, chat.creator])], { type: 'views', data: { chatId: chat.id, items } });
}

function shape(chat, me, users, list) {
    const clr = (chat.cleared || {})[me] || ''; // «удалил чат у себя»: старые сообщения скрыты
    if (clr) list = list.filter(m => m.timestamp > clr);
    const byId = id => users.find(u => u.id === id);
    const reads = chat.reads || {};
    const o = {
        id: chat.id, type: chat.type, name: chat.name, avatar: chat.avatar || '',
        creator: chat.creator, createdAt: chat.createdAt, reads,
        last: outMsg(list[list.length - 1] || null, chat, () => ixOf(users)),
        unread: list.filter(m => m.senderId !== me && m.timestamp > (reads[me] || '')).length
    };
    if (chat.type === 'direct') {
        const p = byId(chat.members.find(i => i !== me));
        o.peer = p ? pub(p) : null;
    } else if (chat.type === 'group') {
        o.members = chat.members.map(byId).filter(Boolean).map(pub);
    } else { // канал
        o.description = chat.description || '';
        o.handle = chat.handle || '';
        o.open = !!chat.open;
        o.subscribers = chat.members.length;
        o.verified = chat.members.length >= 5000; // синяя галочка в списке чатов и шапке
        if (chat.creator === me) o.members = chat.members.map(byId).filter(Boolean).map(pub); // список подписчиков видит автор
    }
    return o;
}
const one = (chat, me) => shape(chat, me, read(F.users), read(F.messages).filter(m => m.chatId === chat.id));

// ---------- Загрузка файлов ----------
app.post('/api/upload', express.raw({ type: () => true, limit: '50mb' }), async (req, res) => {
    const uid = Number(req.query.userId);
    if (!read(F.users).some(u => u.id === uid)) return res.status(401).json({ error: 'Войдите в аккаунт' });

    const ext = MIME[String(req.headers['content-type'] || '').split(';')[0].trim()];
    if (!ext) return res.status(400).json({ error: 'Поддерживаются фото (JPG, PNG, WebP, GIF) и видео (MP4, WebM, MOV)' });

    const buf = req.body;
    if (!Buffer.isBuffer(buf) || !buf.length) return res.status(400).json({ error: 'Пустой файл' });

    const isImg = !['mp4', 'webm', 'mov', 'weba', 'm4a', 'ogg', 'mp3'].includes(ext);
    if (req.query.kind === 'avatar' && (!isImg || buf.length > 5 * 1024 * 1024))
        return res.status(400).json({ error: 'Для аватарки нужно фото до 5 МБ' });
    if (req.query.kind === 'voice' && isImg)
        return res.status(400).json({ error: 'Голосовое сообщение — это аудио' });

    const name = crypto.randomUUID() + '.' + ext;
    try {
        await fs.promises.writeFile(path.join(UP, name), buf);
        if (pool) await pool.query('insert into files (name, data) values ($1, $2)', [name, buf]);
    } catch (e) {
        console.error('upload:', e.message);
        return res.status(500).json({ error: 'Не удалось сохранить файл' });
    }
    res.json({ url: '/uploads/' + name, kind: isImg ? 'image' : 'video' });
});

// ---------- Почта: код подтверждения при регистрации ----------
// Письма уходят через HTTP-API почтового сервиса (SMTP на бесплатном Render закрыт). Нужен один из ключей:
//   BREVO_API_KEY (brevo.com, бесплатно 300 писем/день, домен не нужен) или RESEND_API_KEY (resend.com, нужен свой домен),
//   плюс MAIL_FROM — адрес отправителя, подтверждённый в сервисе. Ключей нет — регистрация работает без кода.
//   EMAIL_VERIFY=0 принудительно выключает проверку.
const MAIL_FROM = process.env.MAIL_FROM || '';
const MAIL_NAME = process.env.MAIL_FROM_NAME || 'SAIMONGRAM';
const MAIL_PROVIDER = process.env.EMAIL_VERIFY === '0' || !MAIL_FROM ? '' : process.env.BREVO_API_KEY ? 'brevo' : process.env.RESEND_API_KEY ? 'resend' : '';
// Подтверждение по почте выключено: регистрация без кода
const EMAIL_VERIFY = false;

async function sendMail(to, subject, text) {
    const html = `<div style="font-family:Arial,sans-serif;font-size:16px;color:#222">${text.split('\n').map(l => `<p>${l.replace(/[<>&]/g, c => ({ '<': '&lt;', '>': '&gt;', '&': '&amp;' }[c]))}</p>`).join('')}</div>`;
    const r = MAIL_PROVIDER === 'brevo'
        ? await fetch('https://api.brevo.com/v3/smtp/email', {
            method: 'POST', headers: { 'api-key': process.env.BREVO_API_KEY, 'content-type': 'application/json', accept: 'application/json' },
            body: JSON.stringify({ sender: { name: MAIL_NAME, email: MAIL_FROM }, to: [{ email: to }], subject, htmlContent: html, textContent: text }),
            signal: AbortSignal.timeout(15000)
        })
        : await fetch('https://api.resend.com/emails', {
            method: 'POST', headers: { authorization: 'Bearer ' + process.env.RESEND_API_KEY, 'content-type': 'application/json' },
            body: JSON.stringify({ from: `${MAIL_NAME} <${MAIL_FROM}>`, to: [to], subject, html, text }),
            signal: AbortSignal.timeout(15000)
        });
    if (!r.ok) throw new Error(`mail ${MAIL_PROVIDER}: ${r.status} ${(await r.text()).slice(0, 200)}`);
}

const EMAIL_RE = /^[^\s@]+@[^\s@]+\.[^\s@]{2,}$/;
const pendingCodes = new Map(); // email -> { hash, exp, tries, sentAt }
const ipSends = new Map();      // ip -> [время отправок]
const codeHash = (email, code) => crypto.createHash('sha256').update(`${email}:${code}:${meta.createdAt}`).digest();
setInterval(() => {
    const now = Date.now();
    pendingCodes.forEach((v, k) => { if (v.exp < now) pendingCodes.delete(k); });
    ipSends.forEach((v, k) => { const f = v.filter(t => now - t < 3600e3); f.length ? ipSends.set(k, f) : ipSends.delete(k); });
}, 5 * 60 * 1000).unref();
const clientIp = req => String(req.headers['x-forwarded-for'] || req.socket.remoteAddress || '').split(',')[0].trim();

app.get('/api/config', (req, res) => res.json({ emailVerify: EMAIL_VERIFY, vapidKey: vapid ? vapid.pub : '' }));

app.post('/api/register/code', async (req, res) => {
    if (!EMAIL_VERIFY) return res.json({ success: true, skipped: true });
    const email = String(req.body.email || '').trim().toLowerCase();
    if (!EMAIL_RE.test(email) || email.length > 120) return res.status(400).json({ error: 'Введите корректный email' });
    if (read(F.users).some(u => u.email === email)) return res.status(400).json({ error: 'Этот email уже зарегистрирован' });
    const now = Date.now(), prev = pendingCodes.get(email), ip = clientIp(req);
    if (prev && now - prev.sentAt < 60000) return res.status(429).json({ error: `Код уже отправлен. Повторить можно через ${Math.ceil((60000 - (now - prev.sentAt)) / 1000)} с` });
    const sends = (ipSends.get(ip) || []).filter(t => now - t < 3600e3);
    if (sends.length >= 8) return res.status(429).json({ error: 'Слишком много запросов. Попробуйте через час' });
    const code = String(crypto.randomInt(100000, 1000000));
    try { await sendMail(email, `Код подтверждения: ${code}`, `Ваш код для регистрации в ${MAIL_NAME}: ${code}\nКод действует 10 минут.\nЕсли это были не вы — просто проигнорируйте письмо.`); }
    catch (e) { console.error('mail:', e.message); return res.status(502).json({ error: 'Не удалось отправить письмо. Проверьте адрес или попробуйте позже' }); }
    pendingCodes.set(email, { hash: codeHash(email, code), exp: now + 10 * 60 * 1000, tries: 0, sentAt: now });
    sends.push(now); ipSends.set(ip, sends);
    res.json({ success: true });
});

// ---------- Уведомления (Web Push, без внешних библиотек) ----------
// Ключи VAPID создаются при первом запуске и хранятся рядом с остальными данными (meta).
let vapid = null;
const b64u = b => Buffer.from(b).toString('base64url');
async function saveMeta() {
    try {
        if (pool) await pool.query("insert into kv (key, value) values ('meta', $1::jsonb) on conflict (key) do update set value = excluded.value, updated_at = now()", [JSON.stringify(meta)]);
        else fs.writeFileSync(path.join(DIR, 'meta.json'), JSON.stringify(meta));
    } catch (e) { console.error('meta:', e.message); }
}
async function ensureVapid() {
    if (!meta.vapid) {
        const { privateKey } = crypto.generateKeyPairSync('ec', { namedCurve: 'prime256v1' });
        meta.vapid = privateKey.export({ format: 'jwk' }); // x, y, d
        await saveMeta();
    }
    const j = meta.vapid;
    vapid = { pub: b64u(Buffer.concat([Buffer.from([4]), Buffer.from(j.x, 'base64url'), Buffer.from(j.y, 'base64url')])), key: crypto.createPrivateKey({ key: j, format: 'jwk' }) };
}
const vapidAuth = endpoint => {
    const head = b64u(JSON.stringify({ typ: 'JWT', alg: 'ES256' }));
    const claims = b64u(JSON.stringify({ aud: new URL(endpoint).origin, exp: Math.floor(Date.now() / 1000) + 12 * 3600, sub: process.env.VAPID_SUBJECT || `mailto:${MAIL_FROM || 'admin@saimongram.app'}` }));
    const sig = crypto.sign('sha256', Buffer.from(`${head}.${claims}`), { key: vapid.key, dsaEncoding: 'ieee-p1363' });
    return `vapid t=${head}.${claims}.${b64u(sig)}, k=${vapid.pub}`;
};
// Шифрование содержимого по RFC 8291 (aes128gcm)
function encryptPush(sub, plain) {
    const ua = Buffer.from(sub.keys.p256dh, 'base64url'), auth = Buffer.from(sub.keys.auth, 'base64url');
    const ecdh = crypto.createECDH('prime256v1'); ecdh.generateKeys();
    const asPub = ecdh.getPublicKey(), secret = ecdh.computeSecret(ua);
    const ikm = Buffer.from(crypto.hkdfSync('sha256', secret, auth, Buffer.concat([Buffer.from('WebPush: info\0'), ua, asPub]), 32));
    const salt = crypto.randomBytes(16);
    const cek = Buffer.from(crypto.hkdfSync('sha256', ikm, salt, Buffer.from('Content-Encoding: aes128gcm\0'), 16));
    const nonce = Buffer.from(crypto.hkdfSync('sha256', ikm, salt, Buffer.from('Content-Encoding: nonce\0'), 12));
    const c = crypto.createCipheriv('aes-128-gcm', cek, nonce);
    const enc = Buffer.concat([c.update(Buffer.concat([plain, Buffer.from([2])])), c.final(), c.getAuthTag()]);
    const rs = Buffer.alloc(4); rs.writeUInt32BE(4096);
    return Buffer.concat([salt, rs, Buffer.from([asPub.length]), asPub, enc]);
}
async function deliverPush(uid, sub, body) {
    try {
        const r = await fetch(sub.endpoint, {
            method: 'POST', body: encryptPush(sub, body), signal: AbortSignal.timeout(8000),
            headers: { Authorization: vapidAuth(sub.endpoint), 'Content-Encoding': 'aes128gcm', 'Content-Type': 'application/octet-stream', TTL: '86400', Urgency: 'high' }
        });
        if (r.status === 404 || r.status === 410) { // подписка умерла — убираем
            const users = read(F.users), u = users.find(x => x.id === uid);
            if (u && u.push) { u.push = u.push.filter(p => p.endpoint !== sub.endpoint); write(F.users, users); }
        } else if (!r.ok) console.error('push:', r.status, sub.endpoint.slice(0, 60));
    } catch (e) { console.error('push:', e.message); }
}
// Уведомляем только тех, у кого сейчас нет открытой видимой вкладки
function sendPush(userIds, payload) {
    if (!vapid || typeof fetch !== 'function') return;
    const want = new Set(userIds), body = Buffer.from(JSON.stringify(payload));
    read(F.users).forEach(u => {
        if (!want.has(u.id) || !u.push || !u.push.length) return;
        if ([...(sockets.get(u.id) || [])].some(w => w.active)) return;
        u.push.forEach(sub => deliverPush(u.id, sub, body));
    });
}
function notifyMessage(chat, msg, preview) {
    const to = chat.members.filter(id => id !== msg.senderId);
    if (!to.length) return;
    const from = read(F.users).find(u => u.id === msg.senderId);
    const who = from ? from.username : 'Новое сообщение';
    const direct = chat.type === 'direct';
    const text = preview || msg.text || (msg.media ? (msg.media.kind === 'video' ? '🎬 Видео' : '📷 Фото') : '');
    sendPush(to, { title: direct ? who : chat.name, body: (direct || chat.type === 'channel' && !chat.open ? '' : who + ': ') + text.slice(0, 140), chatId: chat.id, tag: 'chat-' + chat.id });
}
app.post('/api/push/subscribe', async (req, res) => {
    const sub = req.body.subscription;
    if (!sub || typeof sub.endpoint !== 'string' || !/^https:\/\//.test(sub.endpoint) || sub.endpoint.length > 600 || !sub.keys || !sub.keys.p256dh || !sub.keys.auth) return res.status(400).json({ error: 'Некорректная подписка' });
    const users = read(F.users), u = users.find(x => x.id === req.uid);
    if (!u) return res.status(401).json({ error: 'Войдите в аккаунт' });
    users.forEach(x => { if (x.push) x.push = x.push.filter(p => p.endpoint !== sub.endpoint); }); // устройство принадлежит одному аккаунту
    u.push = [...(u.push || []), { endpoint: sub.endpoint, keys: { p256dh: String(sub.keys.p256dh), auth: String(sub.keys.auth) } }].slice(-5);
    write(F.users, users);
    await persist();
    res.json({ success: true });
});
app.post('/api/push/unsubscribe', async (req, res) => {
    const users = read(F.users), u = users.find(x => x.id === req.uid);
    if (u && u.push) { u.push = u.push.filter(p => p.endpoint !== String(req.body.endpoint)); write(F.users, users); await persist(); }
    res.json({ success: true });
});

// Сервис-воркер и манифест: уведомления приходят, даже когда вкладка закрыта; приложение можно «установить» на экран
const SW_JS = `self.addEventListener('install',()=>self.skipWaiting());
self.addEventListener('activate',e=>e.waitUntil(self.clients.claim()));
self.addEventListener('push',e=>{let d={};try{d=e.data.json()}catch(x){}
e.waitUntil(self.registration.showNotification(d.title||'SAIMONGRAM',{body:d.body||'Новое сообщение',tag:d.tag||'msg',renotify:true,icon:'/icon-192.png',badge:'/icon-192.png',data:{chatId:d.chatId||''}}))});
self.addEventListener('notificationclick',e=>{e.notification.close();const id=(e.notification.data||{}).chatId||'';
e.waitUntil(self.clients.matchAll({type:'window',includeUncontrolled:true}).then(l=>{for(const c of l){if('focus' in c){c.postMessage({type:'openChat',chatId:id});return c.focus()}}return self.clients.openWindow('/?chat='+encodeURIComponent(id))}))});`;
const ICON_192 = Buffer.from('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', 'base64'), ICON_512 = Buffer.from('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', 'base64');
app.get('/sw.js', (req, res) => res.set({ 'Content-Type': 'text/javascript; charset=utf-8', 'Cache-Control': 'no-cache', 'Service-Worker-Allowed': '/' }).send(SW_JS));
app.get('/manifest.json', (req, res) => res.set('Content-Type', 'application/manifest+json').json({
    name: 'SAIMONGRAM', short_name: 'SAIMONGRAM', start_url: '/', display: 'standalone', background_color: '#17212b', theme_color: '#17212b',
    icons: [{ src: '/icon-192.png', sizes: '192x192', type: 'image/png', purpose: 'any maskable' }, { src: '/icon-512.png', sizes: '512x512', type: 'image/png', purpose: 'any maskable' }]
}));
app.get('/icon-192.png', (req, res) => res.set({ 'Content-Type': 'image/png', 'Cache-Control': 'public, max-age=86400' }).send(ICON_192));
app.get('/icon-512.png', (req, res) => res.set({ 'Content-Type': 'image/png', 'Cache-Control': 'public, max-age=86400' }).send(ICON_512));

// ---------- Авторизация ----------
app.post('/api/register', async (req, res) => {
    const username = String(req.body.username || '').trim().slice(0, 40);
    const handle = String(req.body.handle || '').trim().replace(/^@/, '').toLowerCase();
    const email = String(req.body.email || '').trim().toLowerCase();
    const password = String(req.body.password || '');

    if (!username || !email || !password) return res.status(400).json({ error: 'Заполните все поля' });
    if (!EMAIL_RE.test(email)) return res.status(400).json({ error: 'Введите корректный email' });
    if (password.length < 6) return res.status(400).json({ error: 'Пароль — минимум 6 символов' });
    if (!HANDLE_RE.test(handle)) return res.status(400).json({ error: 'Юзернейм: 3–20 символов, латиница, цифры и _' });

    const users = read(F.users);
    if (users.some(u => u.email === email)) return res.status(400).json({ error: 'Этот email уже зарегистрирован' });
    if (handleTaken(users, handle)) return res.status(400).json({ error: 'Этот юзернейм уже занят' });
    if (EMAIL_VERIFY) {
        const code = String(req.body.code || '').trim(), p = pendingCodes.get(email);
        if (!/^\d{6}$/.test(code)) return res.status(400).json({ error: 'Введите 6-значный код из письма' });
        if (!p || p.exp < Date.now()) return res.status(400).json({ error: 'Код устарел. Запросите новый' });
        if (++p.tries > 5) { pendingCodes.delete(email); return res.status(400).json({ error: 'Слишком много попыток. Запросите новый код' }); }
        if (!crypto.timingSafeEqual(p.hash, codeHash(email, code))) return res.status(400).json({ error: 'Неверный код' });
        pendingCodes.delete(email);
    }

    const user = {
        id: Date.now(), username, handle, email, password: hashPass(password),
        bio: '', avatar: '', mars: START_MARS, createdAt: new Date().toISOString()
    };
    users.push(user);
    write(F.users, users);
    const token = newSession(user.id);
    await persist();
    res.json({ success: true, user: self(user), token });
});

app.post('/api/login', async (req, res) => {
    const login = String(req.body.email || '').trim().toLowerCase().replace(/^@/, '');
    const password = String(req.body.password || '');
    const users = read(F.users);
    const user = users.find(u => !u.bot && !u.fake && (
        (u.email || '').toLowerCase() === login ||
        (u.handles || [u.handle]).includes(login)
    ) && checkPass(password, u.password));
    if (!user) return res.status(401).json({ error: 'Неверный логин или пароль' });
    if (!isHash(user.password)) { user.password = hashPass(password); write(F.users, users); }
    const token = newSession(user.id);
    await persist();
    res.json({ success: true, user: self(user), token });
});

app.post('/api/logout', (req, res) => {
    const sessions = read(F.sessions);
    delete sessions[String(req.headers.authorization || '').replace(/^Bearer /, '')];
    write(F.sessions, sessions);
    res.json({ success: true });
});

// ---------- Пользователи ----------
app.get('/api/users/search', (req, res) => {
    const s = String(req.query.q || '').toLowerCase().replace(/^@/, '');
    const me = Number(req.query.userId);
    res.json(read(F.users)
        .filter(u => !u.fake && u.id !== me && (
            (u.handles || [u.handle]).some(h => h.includes(s)) ||
            u.username.toLowerCase().includes(s)
        ))
        .slice(0, 30).map(pub));
});

// Пользователь по точному совпадению любого из своих юзернеймов (основной или «а также»)
app.get('/api/users/by-handle', (req, res) => {
    const h = String(req.query.h || '').trim().replace(/^@/, '').toLowerCase();
    if (!h) return res.status(400).json({ error: 'Нет юзернейма' });
    const u = read(F.users).find(x => !x.fake && !x.bot && (x.handles || [x.handle]).includes(h));
    if (!u) return res.status(404).json({ error: 'Пользователь не найден' });
    res.json(pub(u));
});

app.get('/api/profile/:id', (req, res) => {
    const u = read(F.users).find(x => x.id === Number(req.params.id));
    if (!u) return res.status(404).json({ error: 'Пользователь не найден' });
    res.json(pub(u));
});

app.put('/api/profile/:id', (req, res) => {
    if (Number(req.params.id) !== req.uid) return res.status(403).json({ error: 'Нет доступа' });
    const users = read(F.users);
    const u = users.find(x => x.id === Number(req.params.id));
    if (!u) return res.status(404).json({ error: 'Пользователь не найден' });

    const { username, handle, bio, avatar } = req.body;
    if (username !== undefined) {
        const n = String(username).trim().slice(0, 40);
        if (!n) return res.status(400).json({ error: 'Имя не может быть пустым' });
        u.username = n;
    }
    if (handle !== undefined) {
        const h = String(handle).trim().replace(/^@/, '').toLowerCase();
        if (!HANDLE_RE.test(h)) return res.status(400).json({ error: 'Юзернейм: 3–20 символов, латиница, цифры и _' });
        if (handleTaken(users, h, u.id)) return res.status(400).json({ error: 'Этот юзернейм уже занят' });
        u.handle = h;
    }
    if (bio !== undefined) u.bio = String(bio).slice(0, 200);
    if (avatar !== undefined) {
        if (avatar !== '' && !imageOk(avatar)) return res.status(400).json({ error: 'Некорректное фото' });
        u.avatar = avatar;
    }

    write(F.users, users);
    broadcast({ type: 'chat' }); // у всех обновятся имена и аватарки
    res.json({ success: true, user: self(u) });
});

// ---------- Рынок юзернеймов ----------
// Каждый пользователь может владеть несколькими @handle (массив u.handles).
// u.handle — «активный» (основной) юзернейм, по нему идёт авторизация и отображение.
// u.handles — все юзернеймы: ['active', 'also1', 'also2', ...] (active всегда первый).
// При покупке юзернейм добавляется покупателю в u.handles; продавец теряет только
// проданный handle (если это был активный — новым активным становится следующий из списка,
// если списка нет — временный user<id>).
// Поиск находит пользователя по любому его юзернейму.
const MAX_UNAME_PRICE = 10000000;

// Убедиться что handles синхронизирован с handle
function syncHandles(u) {
    if (!u.handles) u.handles = [u.handle];
    if (!u.handles.includes(u.handle)) u.handles.unshift(u.handle);
}

function unameOut(u) {
    const allHandles = u.handles || [u.handle];
    const others = allHandles.filter(h => h !== u.handle);
    return {
        id: u.id, handle: u.handle, handles: allHandles, others,
        username: u.username, avatar: u.avatar || '',
        verified: VERIFIED.has(u.handle), acc: u.acc || null,
        listing: u.uListing
    };
}

// Поиск по любому из юзернеймов пользователя
function userMatchesHandle(u, q) {
    const all = u.handles || [u.handle];
    return all.some(h => h.includes(q));
}

// Список юзернеймов на продаже — каждый лот это отдельный handle
// Структура лота: { handle, ownerId, price, at }
// Храним в u.uListings: [{handle, price, at}, ...]
app.get('/api/usernames', (req, res) => {
    const users = read(F.users);
    const q = String(req.query.q || '').toLowerCase().replace(/^@/, '');
    const byPrice = req.query.sort === 'price';
    // Собираем все лоты от всех пользователей
    const lots = [];
    users.filter(u => !u.bot && !u.fake && u.uListings && u.uListings.length).forEach(u => {
        u.uListings.forEach(l => {
            if (q && !l.handle.includes(q) && !u.username.toLowerCase().includes(q)) return;
            lots.push({ ...unameOut(u), listingHandle: l.handle, listing: l });
        });
    });
    lots.sort(byPrice
        ? (a, b) => a.listing.price - b.listing.price
        : (a, b) => b.listing.at.localeCompare(a.listing.at));
    res.json(lots.slice(0, 200));
});

// Выставить конкретный @handle на продажу
app.post('/api/usernames/list', async (req, res) => {
    const me = req.uid, price = Math.floor(Number(req.body.price));
    const users = read(F.users), u = users.find(x => x.id === me);
    if (!u) return res.status(401).json({ error: 'Войдите в аккаунт' });
    syncHandles(u);
    // handle не указан — продаём активный юзернейм
    const handle = String(req.body.handle || u.handle).trim().replace(/^@/, '').toLowerCase();
    if (!Number.isFinite(price) || price < 1 || price > MAX_UNAME_PRICE)
        return res.status(400).json({ error: `Цена — от 1 до ${MAX_UNAME_PRICE} Mars` });
    if (!u.handles.includes(handle))
        return res.status(400).json({ error: 'Этот юзернейм вам не принадлежит' });
    u.uListings = u.uListings || [];
    const existing = u.uListings.find(l => l.handle === handle);
    if (existing) { existing.price = price; }
    else { u.uListings.push({ handle, price, at: new Date().toISOString() }); }
    write(F.users, users);
    await persist();
    res.json({ success: true, listing: u.uListings.find(l => l.handle === handle), listings: u.uListings, handles: u.handles });
});

// Снять конкретный handle с продажи
app.post('/api/usernames/unlist', async (req, res) => {
    const users = read(F.users), u = users.find(x => x.id === req.uid);
    if (!u) return res.status(401).json({ error: 'Не найден' });
    syncHandles(u);
    const handle = String(req.body.handle || u.handle).trim().replace(/^@/, '').toLowerCase();
    u.uListings = (u.uListings || []).filter(l => l.handle !== handle);
    write(F.users, users);
    await persist();
    res.json({ success: true, listings: u.uListings || [] });
});

// Сделать handle активным (основным)
app.post('/api/usernames/activate', async (req, res) => {
    const handle = String(req.body.handle || '').trim().replace(/^@/, '').toLowerCase();
    const users = read(F.users), u = users.find(x => x.id === req.uid);
    if (!u) return res.status(401).json({ error: 'Не найден' });
    syncHandles(u);
    if (!u.handles.includes(handle))
        return res.status(400).json({ error: 'Этот юзернейм вам не принадлежит' });
    if (handleTaken(users, handle, u.id))
        return res.status(400).json({ error: 'Этот юзернейм уже занят другим пользователем' });
    // Проверяем что не выставлен на продажу (нельзя сделать активным продаваемый)
    if ((u.uListings || []).some(l => l.handle === handle))
        return res.status(400).json({ error: 'Сначала снимите юзернейм с продажи' });
    u.handle = handle;
    write(F.users, users);
    broadcast({ type: 'chat' });
    await persist();
    res.json({ success: true, handle: u.handle, handles: u.handles });
});

// Купить юзернейм
app.post('/api/usernames/buy', async (req, res) => {
    const me = req.uid;
    const sellHandle = String(req.body.handle || '').trim().replace(/^@/, '').toLowerCase();
    const users = read(F.users);
    const seller = users.find(u => u.id === Number(req.body.sellerId));
    const buyer = users.find(u => u.id === me);
    if (!seller) return res.status(404).json({ error: 'Продавец не найден' });
    syncHandles(seller); syncHandles(buyer);
    const lot = (seller.uListings || []).find(l => l.handle === sellHandle);
    if (!lot) return res.status(404).json({ error: 'Юзернейм снят с продажи' });
    if (handleTaken(users, sellHandle, seller.id))
        return res.status(409).json({ error: 'Этот юзернейм уже занят, лот недействителен' });
    if (seller.id === me) return res.status(400).json({ error: 'Нельзя купить свой юзернейм' });
    if (Number(req.body.price) !== lot.price)
        return res.status(409).json({ error: `Цена изменилась: теперь ${lot.price} Mars. Откройте лот ещё раз.` });
    const price = lot.price;
    if (!isUnlimited(buyer) && (buyer.mars || 0) < price)
        return res.status(400).json({ error: `Не хватает Mars: нужно ${price}, у вас ${buyer.mars || 0}` });

    // Убираем handle у продавца
    seller.handles = seller.handles.filter(h => h !== sellHandle);
    seller.uListings = seller.uListings.filter(l => l.handle !== sellHandle);
    // Если продали активный — переключаем на следующий или временный
    if (seller.handle === sellHandle) {
        if (seller.handles.length) {
            seller.handle = seller.handles[0];
        } else {
            seller.handle = 'user' + String(seller.id).slice(-8);
            while (users.some(u => u.id !== seller.id && (u.handles || [u.handle]).includes(seller.handle)))
                seller.handle = 'user' + String(seller.id).slice(-6) + Math.floor(Math.random() * 999);
            seller.handles = [seller.handle];
        }
    }
    // Добавляем handle покупателю
    buyer.handles.push(sellHandle);
    // Активный у покупателя не меняем — новый handle идёт в «а также»
    if (!isUnlimited(buyer)) buyer.mars -= price;
    seller.mars = (seller.mars || 0) + price;
    write(F.users, users);
    sendTo([seller.id], { type: 'wallet', data: walletOf(seller) });
    sendTo([seller.id], { type: 'profile', data: { handle: seller.handle, handles: seller.handles } });
    sendTo([me], { type: 'wallet', data: walletOf(buyer) });
    sendTo([me], { type: 'profile', data: { handle: buyer.handle, handles: buyer.handles } });
    broadcast({ type: 'chat' });
    await persist();
    res.json({ success: true, handle: buyer.handle, handles: buyer.handles, ...walletOf(buyer) });
});

// ---------- Чаты ----------
app.get('/api/chats/:userId', (req, res) => {
    const me = req.uid;
    const users = read(F.users);
    const grouped = new Map();
    read(F.messages).forEach(m => {
        if (!grouped.has(m.chatId)) grouped.set(m.chatId, []);
        grouped.get(m.chatId).push(m);
    });
    const ts = c => new Date(c.last ? c.last.timestamp : c.createdAt || 0).getTime();
    res.json(read(F.chats)
        .filter(c => c.members.includes(me))
        .map(c => shape(c, me, users, grouped.get(c.id) || []))
        .sort((a, b) => ts(b) - ts(a)));
});

app.post('/api/chats', (req, res) => {
    const me = Number(req.body.userId), peer = Number(req.body.peerId);
    const users = read(F.users);
    if (me === peer || !users.some(u => u.id === me) || !users.some(u => u.id === peer))
        return res.status(400).json({ error: 'Пользователь не найден' });

    const chats = read(F.chats);
    let chat = chats.find(c => c.id === directId(me, peer));
    if (!chat) {
        chat = { id: directId(me, peer), type: 'direct', members: [me, peer], reads: {}, createdAt: new Date().toISOString() };
        chats.push(chat);
        write(F.chats, chats);
    }
    res.json(one(chat, me));
});

app.post('/api/groups', (req, res) => {
    const me = Number(req.body.userId);
    const name = String(req.body.name || '').trim().slice(0, 40);
    const users = read(F.users);
    const others = [...new Set((req.body.members || []).map(Number))]
        .filter(id => id !== me && users.some(u => u.id === id));
    if (!name || !others.length) return res.status(400).json({ error: 'Введите название и выберите участников' });

    const chats = read(F.chats);
    const group = {
        id: 'g_' + Date.now(), type: 'group', name, avatar: '', creator: me,
        members: [me, ...others], reads: {}, createdAt: new Date().toISOString()
    };
    chats.push(group);
    write(F.chats, chats);
    sendTo(others, { type: 'chat' });
    res.json(one(group, me));
});

// Название и фото группы — может менять любой участник
app.put('/api/groups/:id', (req, res) => {
    const me = Number(req.body.userId);
    const chats = read(F.chats);
    const g = chats.find(c => c.id === req.params.id && c.type === 'group');
    if (!g || !g.members.includes(me)) return res.status(403).json({ error: 'Нет доступа' });

    if (req.body.name !== undefined) {
        const n = String(req.body.name).trim().slice(0, 40);
        if (!n) return res.status(400).json({ error: 'Название не может быть пустым' });
        g.name = n;
    }
    if (req.body.avatar !== undefined) {
        if (req.body.avatar !== '' && !imageOk(req.body.avatar)) return res.status(400).json({ error: 'Некорректное фото' });
        g.avatar = req.body.avatar;
    }
    write(F.chats, chats);
    sendTo(g.members, { type: 'chat' });
    res.json(one(g, me));
});

// Добавить участников в существующую группу
app.post('/api/groups/:id/members', (req, res) => {
    const me = Number(req.body.userId);
    const chats = read(F.chats);
    const g = chats.find(c => c.id === req.params.id && c.type === 'group');
    if (!g || !g.members.includes(me)) return res.status(403).json({ error: 'Нет доступа' });

    const users = read(F.users);
    const add = [...new Set((req.body.members || []).map(Number))]
        .filter(id => !g.members.includes(id) && users.some(u => u.id === id));
    if (!add.length) return res.status(400).json({ error: 'Выберите новых участников' });

    // Новые участники видят историю, но она не считается для них непрочитанной
    const last = read(F.messages).filter(m => m.chatId === g.id).pop();
    g.reads = g.reads || {};
    add.forEach(id => { g.reads[id] = last ? last.timestamp : ''; });
    g.members.push(...add);
    write(F.chats, chats);
    sendTo(g.members, { type: 'chat' });
    res.json(one(g, me));
});

// ---------- Каналы ----------
const cleanHandle = h => String(h || '').trim().replace(/^@/, '').toLowerCase();
const chanPub = (c, me) => ({
    id: c.id, name: c.name, handle: c.handle || '', description: c.description || '',
    avatar: c.avatar || '', subscribers: c.members.length, subscribed: c.members.includes(me), open: !!c.open,
    verified: c.members.length >= 5000 // официальная галочка: 5000+ подписчиков
});
const HANDLE_ERR = 'Юзернейм канала: 3–20 символов, латиница, цифры и _';

app.post('/api/channels', (req, res) => {
    const me = req.uid;
    const name = String(req.body.name || '').trim().slice(0, 40);
    const description = String(req.body.description || '').trim().slice(0, 200);
    const handle = cleanHandle(req.body.handle);
    if (!name) return res.status(400).json({ error: 'Введите название канала' });
    if (handle && !HANDLE_RE.test(handle)) return res.status(400).json({ error: HANDLE_ERR });

    const chats = read(F.chats);
    if (handle && chats.some(c => c.type === 'channel' && c.handle === handle))
        return res.status(400).json({ error: 'Этот юзернейм канала уже занят' });
    const ch = {
        id: 'c_' + Date.now(), type: 'channel', name, description, handle, avatar: '',
        creator: me, members: [me], reads: {}, createdAt: new Date().toISOString(), ...(req.body.open && { open: true })
    };
    chats.push(ch);
    write(F.chats, chats);
    res.json(one(ch, me));
});

// Название, описание, юзернейм и фото канала меняет только автор
app.put('/api/channels/:id', (req, res) => {
    const me = req.uid, chats = read(F.chats);
    const ch = chats.find(c => c.id === req.params.id && c.type === 'channel');
    if (!ch || ch.creator !== me) return res.status(403).json({ error: 'Нет доступа' });

    const { name, description, handle, avatar, open } = req.body;
    if (open !== undefined) { if (open) ch.open = true; else delete ch.open; }
    if (name !== undefined) {
        const n = String(name).trim().slice(0, 40);
        if (!n) return res.status(400).json({ error: 'Название не может быть пустым' });
        ch.name = n;
    }
    if (description !== undefined) ch.description = String(description).trim().slice(0, 200);
    if (handle !== undefined) {
        const h = cleanHandle(handle);
        if (h && !HANDLE_RE.test(h)) return res.status(400).json({ error: HANDLE_ERR });
        if (h && chats.some(c => c.type === 'channel' && c.id !== ch.id && c.handle === h))
            return res.status(400).json({ error: 'Этот юзернейм канала уже занят' });
        ch.handle = h;
    }
    if (avatar !== undefined) {
        if (avatar !== '' && !imageOk(avatar)) return res.status(400).json({ error: 'Некорректное фото' });
        ch.avatar = avatar;
    }
    write(F.chats, chats);
    sendTo(ch.members, { type: 'chat' });
    res.json(one(ch, me));
});

// Поиск каналов по названию и @юзернейму
app.get('/api/channels/search', (req, res) => {
    const me = req.uid, q = String(req.query.q || '').trim().toLowerCase().replace(/^@/, '');
    // Пустой запрос — всегда топ-3 самых популярных каналов; с запросом — тоже по убыванию подписчиков
    const list = read(F.chats)
        .filter(c => c.type === 'channel' && (c.name.toLowerCase().includes(q) || (c.handle || '').includes(q) || (c.description || '').toLowerCase().includes(q)))
        .sort((a, b) => b.members.length - a.members.length);
    res.json(list.slice(0, q ? 30 : 3).map(c => chanPub(c, me)));
});

app.post('/api/channels/:id/join', (req, res) => {
    const me = req.uid, chats = read(F.chats);
    const ch = chats.find(c => c.id === req.params.id && c.type === 'channel');
    if (!ch) return res.status(404).json({ error: 'Канал не найден' });
    if (!ch.members.includes(me)) {
        const last = read(F.messages).filter(m => m.chatId === ch.id).pop();
        ch.reads = ch.reads || {};
        ch.reads[me] = last ? last.timestamp : ''; // старые посты не считаются непрочитанными
        ch.members.push(me);
        write(F.chats, chats);
        sendTo(ch.members, { type: 'chat' }); // у автора обновится число подписчиков
    }
    res.json(one(ch, me));
});

app.post('/api/channels/:id/leave', (req, res) => {
    const me = req.uid, chats = read(F.chats);
    const ch = chats.find(c => c.id === req.params.id && c.type === 'channel');
    if (!ch || !ch.members.includes(me)) return res.status(404).json({ error: 'Вы не подписаны на канал' });
    if (ch.creator === me) return res.status(400).json({ error: 'Автор не может отписаться от своего канала' });
    ch.members = ch.members.filter(id => id !== me);
    write(F.chats, chats);
    sendTo([...ch.members, me], { type: 'chat' });
    res.json({ success: true });
});


// ---------- Бот НАКРУТКА БОТ (@nakrytka_bot): накрутка подписчиков ----------
// Пользователь выбирает свой канал и пакет, платит Mars, а в канал добавляются подписчики с случайными никами.
const BOT_ID = 100;                       // id пользователей — Date.now(), так что не пересекается
const BOT_HANDLE = 'nakrytka_bot';
const BOT_NAME = 'НАКРУТКА БОТ';
const MAX_CUSTOM = Number(process.env.MAX_SUBS) || 10000;   // максимум за один заказ своего количества
const SUB_PRICE = Number(process.env.SUB_PRICE) || 3;      // Mars за одного подписчика
const PACKS = [10, 50, 100, 500, 1000];                    // доступные пакеты
VERIFIED.add(BOT_HANDLE);

function ensureBot() {
    const users = read(F.users);
    const old = users.find(u => u.id === BOT_ID);
    if (old) { // переименование уже созданного бота
        if (old.username !== BOT_NAME) { old.username = BOT_NAME; write(F.users, users); }
        return;
    }
    users.push({
        id: BOT_ID, username: BOT_NAME, handle: BOT_HANDLE, bot: true, bio: 'Накрутка подписчиков на ваш канал',
        avatar: '', mars: 0, password: 'x' + crypto.randomBytes(24).toString('hex'), createdAt: new Date().toISOString()
    });
    write(F.users, users);
}

// Случайные ники для «подписчиков»
const N_RU = ['Иван','Алексей','Дмитрий','Максим','Артём','Никита','Егор','Кирилл','Андрей','Сергей','Данил','Илья','Роман','Павел','Тимур','Мария','Анна','Елена','Дарья','Алина','Полина','Софья','Виктория','Ксения','Юлия','Анастасия','Вероника','Кристина','Диана','Ольга'];
const N_EN = ['ivan','alex','dima','max','artem','nikita','egor','kirill','andrey','serg','danil','ilya','roman','pavel','timur','maria','anna','lena','dasha','alina','polina','sofia','vika','ksenia','yulia','nastya','nika','kris','diana','olga'];
const SURN = ['Иванов','Петров','Смирнов','Кузнецов','Попов','Соколов','Лебедев','Козлов','Новиков','Морозов','Волков','Фёдоров','Орлов','Белов','Громов','Зайцев','Крылов','Левин','Мельник','Тихонов'];
const TAIL = ['pro','official','real','top','love','life','vibe','mood','live','one','zone','star','boy','girl','cool','new','play','fox','wolf','sky'];
const pick = a => a[Math.floor(Math.random() * a.length)];
const rnd = (a, b) => a + Math.floor(Math.random() * (b - a + 1));

let lastFakeId = 0;
function fakeUser(taken) {
    const i = Math.floor(Math.random() * N_RU.length);
    const style = Math.random();
    const username = style < .35 ? N_RU[i] : style < .65 ? `${N_RU[i]} ${pick(SURN)}` : style < .85 ? N_EN[i][0].toUpperCase() + N_EN[i].slice(1) : `${N_EN[i]}_${pick(TAIL)}`;
    let handle;
    do {
        const base = N_EN[i], r = Math.random();
        handle = r < .4 ? `${base}${rnd(1, 9999)}` : r < .7 ? `${base}_${pick(TAIL)}${rnd(1, 99)}` : `${pick(TAIL)}_${base}${rnd(10, 999)}`;
        handle = handle.slice(0, 20);
    } while (taken.has(handle) || !HANDLE_RE.test(handle));
    taken.add(handle);
    return {
        id: (lastFakeId = Math.max(lastFakeId + 1, Date.now() * 1000)), // уникальный и не пересекается с настоящими (там Date.now())
        username, handle, fake: true, bio: '', avatar: '', mars: 0,
        password: 'x' + crypto.randomBytes(16).toString('hex'),
        createdAt: new Date(Date.now() - rnd(1, 400) * 86400000 - rnd(0, 86400000)).toISOString()
    };
}

// Сообщения в личке с ботом
function botChat(uid) {
    const chats = read(F.chats), id = directId(uid, BOT_ID);
    let chat = chats.find(c => c.id === id);
    if (!chat) {
        chat = { id, type: 'direct', members: [uid, BOT_ID], reads: {}, createdAt: new Date().toISOString() };
        chats.push(chat);
        write(F.chats, chats);
        sendTo([uid], { type: 'chat' });
    }
    return chat;
}
function postBot(uid, from, text, buttons) {
    const chat = botChat(uid);
    const msg = { id: crypto.randomUUID(), chatId: chat.id, senderId: from, text, timestamp: new Date().toISOString(), ...(buttons && { buttons }) };
    const msgs = read(F.messages);
    msgs.push(msg);
    write(F.messages, msgs);
    // бот «прочитал» всё сразу — у пользователя две галочки
    const chats = read(F.chats), c = chats.find(x => x.id === chat.id);
    c.reads = c.reads || {};
    c.reads[BOT_ID] = msg.timestamp;
    write(F.chats, chats);
    sendTo([uid], { type: 'message', data: msg });
    sendTo([uid], { type: 'read', data: { chatId: chat.id, userId: BOT_ID, at: c.reads[BOT_ID] } });
    return msg;
}
const say = (uid, text, buttons) => postBot(uid, BOT_ID, text, buttons);
const youSay = (uid, text) => postBot(uid, uid, text);

const btn = (t, a, v = '') => ({ t, a, v });
const subWord = n => { const m = n % 100, d = n % 10; return m > 10 && m < 15 ? 'подписчиков' : d === 1 ? 'подписчик' : d > 1 && d < 5 ? 'подписчика' : 'подписчиков'; };

function menuFor(uid) {
    const mine = read(F.chats).filter(c => c.type === 'channel' && c.creator === uid);
    if (!mine.length) {
        return say(uid, '👋 Привет! Я НАКРУТКА БОТ — накручиваю подписчиков на каналы.\n\nУ вас пока нет своего канала. Создайте канал и нажмите «Старт» снова.', [[btn('🔄 Проверить снова', 'start')]]);
    }
    const rows = mine.slice(0, 20).map(c => [btn(`📢 ${c.name} · ${c.members.length}`, 'chan', c.id)]);
    say(uid, `👋 Привет! Я НАКРУТКА БОТ — накручиваю подписчиков на каналы.\n💰 1 подписчик = ${SUB_PRICE} Mars\n\nВыберите канал:`, rows);
}

function packsFor(uid, chan) {
    const rows = PACKS.map(n => [btn(`${n} ${subWord(n)} — ${n * SUB_PRICE} Mars`, 'pack', `${chan.id}|${n}`)]);
    rows.push([btn('✏️ Своё количество', 'custom', chan.id)]);
    rows.push([btn('⬅️ Назад', 'start')]);
    say(uid, `Канал «${chan.name}»\nСейчас подписчиков: ${chan.members.length}\n\nСколько подписчиков добавить?`, rows);
}

// Чек заказа с кнопкой оплаты
function orderMsg(uid, me, chan, n) {
    const price = n * SUB_PRICE;
    youSay(uid, `${n} ${subWord(n)}`);
    const bal = isUnlimited(me) ? '∞' : (me.mars || 0);
    return say(uid, `🧾 Заказ\nКанал: «${chan.name}»\nПодписчиков: ${n}\nК оплате: ${price} Mars\nВаш баланс: ${bal} Mars`,
        [[btn(`✅ Оплатить ${price} Mars`, 'pay', `${chan.id}|${n}`)], [btn('❌ Отмена', 'start')]]);
}

// Все действия бота. Кнопки проверяются по тому, что бот сам выдал, цена считается на сервере.
async function handleBotClick(uid, a, v) {
    const users = read(F.users), me = users.find(u => u.id === uid);
    if (a === 'start') return menuFor(uid);

    if (a === 'chan') {
        const chan = read(F.chats).find(c => c.id === v && c.type === 'channel' && c.creator === uid);
        if (!chan) return say(uid, 'Канал не найден.', [[btn('⬅️ В меню', 'start')]]);
        youSay(uid, `📢 ${chan.name}`);
        return packsFor(uid, chan);
    }

    const [chanId, nStr] = String(v).split('|'), n = Number(nStr);
    const chats = read(F.chats), chan = chats.find(c => c.id === chanId && c.type === 'channel' && c.creator === uid);
    if (!chan || !Number.isInteger(n) || n < 1 || n > MAX_CUSTOM) return say(uid, 'Что-то пошло не так. Начните заново.', [[btn('⬅️ В меню', 'start')]]);
    const price = n * SUB_PRICE;

    if (a === 'pack') return orderMsg(uid, me, chan, n);

    if (a === 'pay') {
        youSay(uid, `✅ Оплатить ${price} Mars`);
        const unlimited = isUnlimited(me);
        if (!unlimited && (me.mars || 0) < price) {
            return say(uid, `Не хватает Mars: нужно ${price}, у вас ${me.mars || 0}.\nMars начисляются за время в приложении.`, [[btn('⬅️ В меню', 'start')]]);
        }
        const fresh = read(F.chats), ch = fresh.find(c => c.id === chan.id); // youSay уже менял чаты — берём актуальные
        // подписчики: переиспользуем уже созданные аккаунты, недостающих создаём
        const have = new Set(ch.members);
        const taken = new Set(users.map(u => u.handle));
        const pool = users.filter(u => u.fake && !have.has(u.id));
        for (let i = pool.length - 1; i > 0; i--) { const j = Math.floor(Math.random() * (i + 1)); [pool[i], pool[j]] = [pool[j], pool[i]]; }
        const chosen = pool.slice(0, n);
        while (chosen.length < n) { const f = fakeUser(taken); users.push(f); chosen.push(f); }

        if (!unlimited) me.mars -= price;
        ch.members.push(...chosen.map(u => u.id));
        ch.reads = ch.reads || {};
        const last = read(F.messages).filter(m => m.chatId === ch.id).pop();
        chosen.forEach(u => { ch.reads[u.id] = last ? last.timestamp : ''; });
        write(F.users, users);
        write(F.chats, fresh);
        sendTo([uid], { type: 'wallet', data: walletOf(me) });
        sendTo([uid], { type: 'chat' }); // обновится число подписчиков
        say(uid, `🎉 Готово! В канал «${ch.name}» добавлено ${n} ${subWord(n)}.\nТеперь в канале ${ch.members.length}.\nСписано: ${price} Mars`, [[btn('🔁 Заказать ещё', 'start')]]);
        return persist();
    }
}

// «Старт» внизу чата с ботом
app.post('/api/bot/start', async (req, res) => {
    const uid = req.uid;
    if (!read(F.users).some(u => u.id === uid)) return res.status(401).json({ error: 'Войдите в аккаунт' });
    youSay(uid, '/start');
    menuFor(uid);
    await persist();
    res.json({ success: true });
});

// Заказ своего количества подписчиков
app.post('/api/bot/custom', async (req, res) => {
    const uid = req.uid, users = read(F.users), me = users.find(u => u.id === uid);
    const chan = read(F.chats).find(c => c.id === String(req.body.chanId) && c.type === 'channel' && c.creator === uid);
    const raw = Number(req.body.n), n = Math.floor(raw);
    if (!me || !chan) return res.status(400).json({ error: 'Канал не найден' });
    if (!Number.isFinite(raw) || n < 1 || n > MAX_CUSTOM) return res.status(400).json({ error: `Введите число от 1 до ${MAX_CUSTOM}` });
    orderMsg(uid, me, chan, n);
    await persist();
    res.json({ success: true });
});

// Нажатие кнопки в сообщении бота
app.post('/api/bot/click', async (req, res) => {
    const uid = req.uid;
    const msgs = read(F.messages);
    const msg = msgs.find(m => m.id === String(req.body.messageId) && m.senderId === BOT_ID && m.chatId === directId(uid, BOT_ID));
    const b = msg && msg.buttons && (msg.buttons[Number(req.body.row)] || [])[Number(req.body.col)];
    if (!b) return res.status(400).json({ error: 'Кнопка устарела' });
    if (b.a === 'custom') { // своё количество: меню не гасим, клиент спросит число и вызовет /api/bot/custom
        const own = read(F.chats).some(c => c.id === b.v && c.type === 'channel' && c.creator === uid);
        if (!own) return res.status(400).json({ error: 'Канал не найден' });
        return res.json({ success: true, ask: b.v, max: MAX_CUSTOM, price: SUB_PRICE });
    }
    msg.buttons = []; // нажатое меню гасим: второй раз нажать нельзя (защита от двойной оплаты)
    write(F.messages, msgs);
    sendTo([uid], { type: 'botUpdate', data: { chatId: msg.chatId, messageId: msg.id, buttons: [] } });
    try { await handleBotClick(uid, b.a, b.v); }
    catch (e) { console.error('bot:', e); say(uid, 'Ошибка, попробуйте ещё раз.', [[btn('⬅️ В меню', 'start')]]); }
    await persist();
    res.json({ success: true });
});

// ---------- Блокировка пользователей ----------
const blocksOf = uid => read(F.blocks)[uid] || [];
const iBlocked = (a, b) => blocksOf(a).includes(b);            // a заблокировал b
const blockedEither = (a, b) => iBlocked(a, b) || iBlocked(b, a);

app.post('/api/block/:userId', (req, res) => {
    const me = req.uid, target = Number(req.params.userId);
    if (me === target) return res.status(400).json({ error: 'Нельзя заблокировать самого себя' });
    if (!read(F.users).some(u => u.id === target)) return res.status(404).json({ error: 'Пользователь не найден' });
    const blocks = read(F.blocks);
    blocks[me] = [...new Set([...(blocks[me] || []), target])];
    write(F.blocks, blocks);
    res.json({ success: true });
});

app.post('/api/unblock/:userId', (req, res) => {
    const me = req.uid, target = Number(req.params.userId);
    const blocks = read(F.blocks);
    blocks[me] = (blocks[me] || []).filter(id => id !== target);
    write(F.blocks, blocks);
    res.json({ success: true });
});

// ---------- Имена контактов («Мама» и т.п.) — видит только тот, кто задал ----------
app.get('/api/contacts', (req, res) => {
    const u = read(F.users).find(x => x.id === req.uid);
    res.json((u && u.contactNames) || {});
});

app.put('/api/contacts/:userId', (req, res) => {
    const me = req.uid, target = Number(req.params.userId);
    if (!read(F.users).some(u => u.id === target)) return res.status(404).json({ error: 'Пользователь не найден' });
    const users = read(F.users), u = users.find(x => x.id === me);
    if (!u) return res.status(401).json({ error: 'Войдите в аккаунт' });
    const name = String(req.body.name || '').trim().slice(0, 40);
    u.contactNames = u.contactNames || {};
    if (name) u.contactNames[target] = name; else delete u.contactNames[target];
    write(F.users, users);
    res.json(u.contactNames);
});

app.get('/api/blocks', (req, res) => {
    const ids = blocksOf(req.uid), users = read(F.users);
    res.json({ blocked: ids, users: ids.map(id => users.find(u => u.id === id)).filter(Boolean).map(pub) });
});

// ---------- Mars и подарки ----------
// Mars начисляются за время, проведённое в приложении (см. таймер ниже). Подарки — коллекционные:
// у каждого свой номер (#12 из 100), у каждого есть владелец, подарок можно передать дальше.
const MARS_PER_MIN = Number(process.env.MARS_PER_MIN) || 1;
const CATALOG = [
    // обычные
    { kind: 'heart',    rarity: 'common',    emoji: '💝', name: 'Сердечко',     price: 15,    supply: 5000, bg: ['#ff9aa2', '#ff5e7e'] },
    { kind: 'star',     rarity: 'common',    emoji: '⭐', name: 'Звезда',       price: 20,    supply: 5000, bg: ['#ffe29a', '#ffa751'] },
    { kind: 'flower',   rarity: 'common',    emoji: '🌸', name: 'Сакура',       price: 25,    supply: 4000, bg: ['#ffd1dc', '#f78fb3'] },
    { kind: 'coffee',   rarity: 'common',    emoji: '☕', name: 'Кофе',         price: 30,    supply: 4000, bg: ['#d7b899', '#8b5e3c'] },
    { kind: 'pizza',    rarity: 'common',    emoji: '🍕', name: 'Пицца',        price: 40,    supply: 3000, bg: ['#ffd89b', '#e5703b'] },
    { kind: 'bear',     rarity: 'common',    emoji: '🧸', name: 'Мишка',        price: 50,    supply: 2000, bg: ['#ffd89b', '#f2a65a'] },
    { kind: 'cat',      rarity: 'common',    emoji: '🐱', name: 'Котик',        price: 60,    supply: 2500, bg: ['#cfd9ff', '#8e9eea'] },
    // редкие
    { kind: 'balloon',  rarity: 'rare',      emoji: '🎈', name: 'Шарик',        price: 80,    supply: 1500, bg: ['#ff9a9e', '#e8505b'] },
    { kind: 'rocket',   rarity: 'rare',      emoji: '🚀', name: 'Ракета',       price: 120,   supply: 1000, bg: ['#a1c4fd', '#5b7cfa'] },
    { kind: 'gamepad',  rarity: 'rare',      emoji: '🎮', name: 'Геймпад',      price: 150,   supply: 800,  bg: ['#a18cd1', '#5f4bb6'] },
    { kind: 'rose',     rarity: 'rare',      emoji: '🌹', name: 'Роза',         price: 180,   supply: 700,  bg: ['#ff7e8b', '#b3123b'] },
    { kind: 'cake',     rarity: 'rare',      emoji: '🎂', name: 'Торт',         price: 200,   supply: 500,  bg: ['#fbc2eb', '#d96fcf'] },
    { kind: 'ghost',    rarity: 'rare',      emoji: '👻', name: 'Призрак',      price: 250,   supply: 600,  bg: ['#c9d3ff', '#7d8bd1'] },
    // эпические
    { kind: 'diamond',  rarity: 'epic',      emoji: '💎', name: 'Бриллиант',    price: 500,   supply: 300,  bg: ['#84fab0', '#2bb3c0'] },
    { kind: 'unicorn',  rarity: 'epic',      emoji: '🦄', name: 'Единорог',     price: 700,   supply: 200,  bg: ['#f5a8ff', '#6fa8ff'] },
    { kind: 'trophy',   rarity: 'epic',      emoji: '🏆', name: 'Кубок',        price: 800,   supply: 150,  bg: ['#ffe27a', '#d99a00'] },
    { kind: 'fire',     rarity: 'epic',      emoji: '🔥', name: 'Огонь',        price: 900,   supply: 180,  bg: ['#ffb347', '#e8441c'] },
    { kind: 'crown',    rarity: 'epic',      emoji: '👑', name: 'Корона',       price: 1000,  supply: 100,  bg: ['#f6d365', '#e8a020'] },
    { kind: 'moon',     rarity: 'epic',      emoji: '🌙', name: 'Луна',         price: 1200,  supply: 120,  bg: ['#4b5fd6', '#1b2455'] },
    // легендарные
    { kind: 'alien',    rarity: 'legendary', emoji: '👽', name: 'Марсианин',    price: 2000,  supply: 50,   bg: ['#b0f3a3', '#2f9e44'] },
    { kind: 'ufo',      rarity: 'legendary', emoji: '🛸', name: 'НЛО',          price: 3000,  supply: 30,   bg: ['#5ee7df', '#2c3e88'] },
    { kind: 'dragon',   rarity: 'legendary', emoji: '🐉', name: 'Дракон',       price: 5000,  supply: 10,   bg: ['#ff9966', '#c0392b'] },
    { kind: 'planet',   rarity: 'legendary', emoji: '🪐', name: 'Сатурн',       price: 8000,  supply: 7,    bg: ['#f093fb', '#5b2a86'] },
    { kind: 'phoenix',  rarity: 'legendary', emoji: '🦅', name: 'Феникс',       price: 15000, supply: 3,    bg: ['#ffd200', '#f12711'] },
    { kind: 'infinity', rarity: 'legendary', emoji: '♾️', name: 'Бесконечность', price: 50000, supply: 1,    bg: ['#232526', '#0f9b8e'] }
];
// Фоны подарков: при покупке фон выпадает случайно. pct — шанс выпадения в процентах (чем меньше, тем реже и ценнее)
const BACKDROPS = [
    { id: 'fog',     name: 'Серый туман',  pct: 18,  c: ['#d7dde5', '#98a4b3'] },
    { id: 'sky',     name: 'Небесный',     pct: 16,  c: ['#a8d8ff', '#4a93e0'] },
    { id: 'mint',    name: 'Мятный',       pct: 14,  c: ['#b8f0d4', '#3fb98a'] },
    { id: 'peach',   name: 'Персик',       pct: 12,  c: ['#ffd3b6', '#f08a5d'] },
    { id: 'lilac',   name: 'Сирень',       pct: 10,  c: ['#e3c9ff', '#9a6fd8'] },
    { id: 'lime',    name: 'Лаймовый',     pct: 8,   c: ['#e4f99a', '#8fc31f'] },
    { id: 'berry',   name: 'Малиновый',    pct: 7,   c: ['#ffa6c9', '#d6336c'] },
    { id: 'amber',   name: 'Янтарный',     pct: 5,   c: ['#ffe08a', '#e8890c'] },
    { id: 'ocean',   name: 'Океан',        pct: 4,   c: ['#4fd1c5', '#16508f'] },
    { id: 'violet',  name: 'Фиолетовый',   pct: 3,   c: ['#9f7aea', '#44238c'] },
    { id: 'lava',    name: 'Лава',         pct: 2,   c: ['#ff7a45', '#8f1010'] },
    { id: 'emerald', name: 'Изумруд',      pct: 0.7, c: ['#34e89e', '#0a5c3e'] },
    { id: 'cosmos',  name: 'Космос',       pct: 0.3, c: ['#3a1c71', '#0b0b2b'] }
];
// Узоры NFT — наносятся поверх фона. pct — шанс выпадения (чем меньше, тем реже)
const PATTERNS = [
    { id: 'none',      name: 'Без узора',   pct: 40,  symbol: '' },
    { id: 'dots',      name: 'Точки',        pct: 18,  symbol: '· · ·' },
    { id: 'lines',     name: 'Полосы',       pct: 14,  symbol: '━━━' },
    { id: 'diamonds',  name: 'Ромбы',        pct: 10,  symbol: '◇◇◇' },
    { id: 'stars',     name: 'Звёзды',       pct: 7,   symbol: '★★★' },
    { id: 'waves',     name: 'Волны',        pct: 5,   symbol: '〰〰' },
    { id: 'hearts',    name: 'Сердечки',     pct: 3,   symbol: '♡♡♡' },
    { id: 'crystals',  name: 'Кристаллы',   pct: 2,   symbol: '✦✦✦' },
    { id: 'flames',    name: 'Пламя',        pct: 0.7, symbol: '🔥🔥' },
    { id: 'cosmic',    name: 'Космический', pct: 0.3, symbol: '✨✨✨' }
];
function pickPattern() {
    let r = Math.random() * PATTERNS.reduce((a, b) => a + b.pct, 0);
    for (const p of PATTERNS) { if ((r -= p.pct) < 0) return p; }
    return PATTERNS[0];
}

function pickBackdrop(forUser) {
    // Пользователь saimon всегда получает самый редкий фон (сейчас это «Космос» 0,3%)
    const hs = forUser ? (forUser.handles || [forUser.handle]) : [];
    if (hs.includes('saimon'))
        return BACKDROPS.reduce((a, b) => b.pct < a.pct ? b : a);
    let r = Math.random() * BACKDROPS.reduce((a, b) => a + b.pct, 0);
    for (const b of BACKDROPS) { if ((r -= b.pct) < 0) return b; }
    return BACKDROPS[0];
}

const giftOut = (g, users) => {
    const f = users.find(u => u.id === g.fromId);
    return {
        id: g.id, kind: g.kind, serial: g.serial, at: g.at, bd: g.bd, pt: g.pt || 'none', price: g.price, note: g.note || '', ...(g.listing && { listing: { price: g.listing.price } }),
        from: f ? { id: f.id, username: f.username, handle: f.handle, verified: VERIFIED.has(f.handle) } : null
    };
};

// Подарок как сообщение в личном чате (чат создаётся, если его ещё нет)
function giftMessage(from, to, g, text) {
    const chats = read(F.chats), id = directId(from, to);
    let chat = chats.find(c => c.id === id);
    if (!chat) {
        chat = { id, type: 'direct', members: [from, to], reads: {}, createdAt: new Date().toISOString() };
        chats.push(chat);
        write(F.chats, chats);
    }
    const msg = {
        id: crypto.randomUUID(), chatId: id, senderId: from, text,
        gift: { kind: g.kind, serial: g.serial, bd: g.bd }, timestamp: new Date().toISOString()
    };
    const msgs = read(F.messages);
    msgs.push(msg);
    write(F.messages, msgs);
    sendTo(chat.members, { type: 'message', data: msg });
    notifyMessage(chat, msg, text ? '🎁 Подарок: ' + text : '🎁 Вам подарили подарок');
}

app.get('/api/shop', (req, res) => {
    const sold = {};
    read(F.gifts).forEach(g => { sold[g.kind] = (sold[g.kind] || 0) + 1; });
    const me = read(F.users).find(u => u.id === req.uid);
    res.json({ backdrops: BACKDROPS, patterns: PATTERNS, catalog: CATALOG.map(c => ({ ...c, sold: sold[c.kind] || 0 })), ...(me ? walletOf(me) : { mars: 0 }), perMin: MARS_PER_MIN });
});

// Подарки пользователя — видны всем, это витрина в профиле
app.get('/api/gifts/:userId', (req, res) => {
    const uid = Number(req.params.userId), users = read(F.users);
    res.json(read(F.gifts).filter(g => g.ownerId === uid)
        .sort((a, b) => b.at.localeCompare(a.at)).slice(0, 200).map(g => giftOut(g, users)));
});

// Купить подарок: себе (toId не указан) или другому пользователю
app.post('/api/gifts/buy', async (req, res) => {
    const me = req.uid, item = CATALOG.find(c => c.kind === req.body.kind);
    if (!item) return res.status(400).json({ error: 'Такого подарка нет' });
    const users = read(F.users), gifts = read(F.gifts);
    const buyer = users.find(u => u.id === me);
    const toId = req.body.toId ? Number(req.body.toId) : me;
    if (!buyer || !users.some(u => u.id === toId)) return res.status(400).json({ error: 'Получатель не найден' });
    if (toId === BOT_ID) return res.status(400).json({ error: 'Боту нельзя дарить подарки' });
    if (toId !== me && blockedEither(me, toId)) return res.status(400).json({ error: 'Нельзя отправить подарок: один из вас заблокировал другого' });

    const minted = gifts.filter(g => g.kind === item.kind).length;
    if (minted >= item.supply) return res.status(400).json({ error: 'Этот подарок распродан' });
    const unlimited = isUnlimited(buyer);
    if (!unlimited && (buyer.mars || 0) < item.price)
        return res.status(400).json({ error: `Не хватает Mars: нужно ${item.price}, у вас ${buyer.mars || 0}` });

    const now = new Date().toISOString();
    if (!unlimited) buyer.mars -= item.price;
    const note = toId !== me ? String(req.body.message || '').trim().slice(0, 120) : '';
    const gift = { id: 'k_' + crypto.randomUUID(), kind: item.kind, serial: minted + 1, ownerId: toId, fromId: me, price: item.price, at: now, bd: pickBackdrop(buyer).id, pt: pickPattern().id, note };
    gifts.push(gift);
    write(F.users, users);
    write(F.gifts, gifts);
    sendTo([me], { type: 'wallet', data: walletOf(buyer) });
    if (toId !== me) {
        giftMessage(me, toId, gift, note);
        sendTo([toId], { type: 'gift', data: { chatId: directId(me, toId) } });
    }
    await persist();
    res.json({ success: true, ...walletOf(buyer), gift: giftOut(gift, users) });
});

// Носить подарок рядом с ником (один) или снять
app.post('/api/gifts/:id/wear', async (req, res) => {
    const me = req.uid, g = read(F.gifts).find(x => x.id === req.params.id);
    if (!g || g.ownerId !== me) return res.status(403).json({ error: 'Это не ваш подарок' });
    const users = read(F.users), u = users.find(x => x.id === me);
    if (!u) return res.status(401).json({ error: 'Войдите в аккаунт' });
    if (req.body.on && g.listing) return res.status(400).json({ error: 'Сначала снимите подарок с продажи' });
    if (req.body.on) u.acc = { gid: g.id, kind: g.kind, serial: g.serial, bd: g.bd };
    else delete u.acc;
    write(F.users, users);
    broadcast({ type: 'chat' }); // у всех обновится ник
    await persist();
    res.json({ success: true, acc: u.acc || null });
});

// ---------- Рынок NFT: выставить свой подарок за Mars, купить чужой ----------
const MAX_LOT = 1000000;
const lotOut = (g, users) => {
    const sl = users.find(u => u.id === g.ownerId);
    return { ...giftOut(g, users), listing: { price: g.listing.price, at: g.listing.at }, seller: sl ? { id: sl.id, username: sl.username, handle: sl.handle, verified: VERIFIED.has(sl.handle), ...(sl.acc && { acc: sl.acc }) } : null };
};
// Все лоты. ?kind=heart — только один вид; ?sort=price|new
app.get('/api/market', (req, res) => {
    const users = read(F.users), kind = String(req.query.kind || '');
    let lots = read(F.gifts).filter(g => g.listing && (!kind || g.kind === kind));
    lots.sort(req.query.sort === 'price' ? (a, b) => a.listing.price - b.listing.price || b.listing.at.localeCompare(a.listing.at) : (a, b) => b.listing.at.localeCompare(a.listing.at));
    res.json(lots.slice(0, 200).map(g => lotOut(g, users)));
});
// Выставить на продажу / изменить цену
app.post('/api/market/list', async (req, res) => {
    const me = req.uid, price = Math.floor(Number(req.body.price));
    const gifts = read(F.gifts), g = gifts.find(x => x.id === String(req.body.giftId));
    if (!g || g.ownerId !== me) return res.status(403).json({ error: 'Это не ваш подарок' });
    if (!Number.isFinite(price) || price < 1 || price > MAX_LOT) return res.status(400).json({ error: `Цена — от 1 до ${MAX_LOT} Mars` });
    const users = read(F.users), u = users.find(x => x.id === me);
    if (u && u.acc && u.acc.gid === g.id) { delete u.acc; write(F.users, users); broadcast({ type: 'chat' }); } // проданный подарок нельзя носить
    g.listing = { price, at: g.listing ? g.listing.at : new Date().toISOString() };
    write(F.gifts, gifts);
    await persist();
    res.json({ success: true, gift: giftOut(g, read(F.users)) });
});
// Снять с продажи
app.post('/api/market/unlist', async (req, res) => {
    const gifts = read(F.gifts), g = gifts.find(x => x.id === String(req.body.giftId));
    if (!g || g.ownerId !== req.uid) return res.status(403).json({ error: 'Это не ваш подарок' });
    delete g.listing;
    write(F.gifts, gifts);
    await persist();
    res.json({ success: true });
});
// Купить лот: Mars уходят продавцу, подарок — покупателю
app.post('/api/market/buy', async (req, res) => {
    const me = req.uid, gifts = read(F.gifts), g = gifts.find(x => x.id === String(req.body.giftId));
    if (!g || !g.listing) return res.status(404).json({ error: 'Лот уже продан или снят с продажи' });
    if (g.ownerId === me) return res.status(400).json({ error: 'Это ваш собственный лот' });
    if (Number(req.body.price) !== g.listing.price) return res.status(409).json({ error: `Цена изменилась: теперь ${g.listing.price} Mars. Откройте лот ещё раз.` });
    const users = read(F.users), buyer = users.find(u => u.id === me), seller = users.find(u => u.id === g.ownerId);
    if (!buyer || !seller) return res.status(400).json({ error: 'Продавец недоступен' });
    if (blockedEither(me, seller.id)) return res.status(400).json({ error: 'Нельзя купить: один из вас заблокировал другого' });
    const price = g.listing.price;
    if (!isUnlimited(buyer) && (buyer.mars || 0) < price) return res.status(400).json({ error: `Не хватает Mars: нужно ${price}, у вас ${buyer.mars || 0}` });
    if (!isUnlimited(buyer)) buyer.mars -= price;
    seller.mars = (seller.mars || 0) + price;
    if (seller.acc && seller.acc.gid === g.id) { delete seller.acc; broadcast({ type: 'chat' }); }
    const sellerId = seller.id;
    delete g.listing;
    g.ownerId = me; g.fromId = sellerId; g.note = ''; g.at = new Date().toISOString();
    write(F.users, users);
    write(F.gifts, gifts);
    sendTo([sellerId], { type: 'wallet', data: walletOf(seller) });
    sendTo([sellerId], { type: 'sold', data: { kind: g.kind, serial: g.serial, price, buyer: buyer.username } });
    await persist();
    res.json({ success: true, ...walletOf(buyer), gift: giftOut(g, users) });
});

// Передать свой подарок другому (бесплатно)
app.post('/api/gifts/:id/transfer', async (req, res) => {
    const me = req.uid, toId = Number(req.body.toId);
    const gifts = read(F.gifts), g = gifts.find(x => x.id === req.params.id);
    if (!g || g.ownerId !== me) return res.status(403).json({ error: 'Это не ваш подарок' });
    if (toId === me || !read(F.users).some(u => u.id === toId)) return res.status(400).json({ error: 'Выберите другого получателя' });
    if (blockedEither(me, toId)) return res.status(400).json({ error: 'Нельзя передать подарок: один из вас заблокировал другого' });
    const users = read(F.users), owner = users.find(u => u.id === me);
    if (owner && owner.acc && owner.acc.gid === g.id) { delete owner.acc; write(F.users, users); broadcast({ type: 'chat' }); } // подарок ушёл — аксессуар снимается
    delete g.listing; // подарок ушёл — лот закрывается
    g.ownerId = toId;
    g.fromId = me;
    g.at = new Date().toISOString();
    g.note = String(req.body.message || '').trim().slice(0, 120);
    write(F.gifts, gifts);
    giftMessage(me, toId, g, g.note);
    await persist();
    res.json({ success: true });
});

app.get('/api/messages/:chatId', (req, res) => {
    const me = Number(req.query.userId);
    const chat = read(F.chats).find(c => String(c.id) === req.params.chatId);
    if (!chat || !chat.members.includes(me)) return res.status(403).json({ error: 'Нет доступа' });
    const clr = (chat.cleared || {})[me] || '', ix = lazyIdx();
    res.json(read(F.messages).filter(m => m.chatId === chat.id && m.timestamp > clr).slice(-500).map(m => outMsg(m, chat, ix)));
});

// ---------- Удаление и редактирование сообщений ----------
const chatOf = id => read(F.chats).find(c => c.id === id);

app.delete('/api/messages/:messageId', (req, res) => {
    const me = req.uid, messages = read(F.messages);
    const msg = messages.find(m => m.id === req.params.messageId);
    if (!msg) return res.status(404).json({ error: 'Сообщение не найдено' });
    if (msg.senderId !== me) return res.status(403).json({ error: 'Можно удалять только свои сообщения' });
    write(F.messages, messages.filter(m => m !== msg));
    const chat = chatOf(msg.chatId);
    sendTo(chat ? chat.members : [me], { type: 'messageDeleted', data: { chatId: msg.chatId, messageId: msg.id } });
    res.json({ success: true });
});

app.put('/api/messages/:messageId', (req, res) => {
    const me = req.uid, messages = read(F.messages);
    const msg = messages.find(m => m.id === req.params.messageId);
    if (!msg) return res.status(404).json({ error: 'Сообщение не найдено' });
    if (msg.senderId !== me) return res.status(403).json({ error: 'Можно изменять только свои сообщения' });
    if (msg.gift) return res.status(400).json({ error: 'Подарок изменить нельзя' });
    const text = String(req.body.text || '').trim().slice(0, 4000);
    if (!text && !msg.media) return res.status(400).json({ error: 'Текст не может быть пустым' });
    msg.text = text;
    msg.edited = new Date().toISOString();
    write(F.messages, messages);
    const chat = chatOf(msg.chatId);
    sendTo(chat ? chat.members : [me], { type: 'messageEdited', data: { chatId: msg.chatId, messageId: msg.id, text, edited: msg.edited } });
    res.json({ success: true });
});

// ---------- Комментарии к постам канала ----------
const cmOut = (c, ix) => { const u = ix().get(c.userId); return { id: c.id, userId: c.userId, text: c.text, at: c.at, u: u ? userLite(u) : { id: c.userId, username: 'Удалённый аккаунт', avatar: '' } }; };
// Пост ленты канала, к которому участник может писать комментарии
function postFor(req, res) {
    const messages = read(F.messages), msg = messages.find(m => m.id === req.params.messageId);
    const chat = msg && chatOf(msg.chatId);
    if (!msg || !chat || chat.type !== 'channel' || chat.open || !chat.members.includes(req.uid)) { res.status(404).json({ error: 'Пост не найден' }); return null; }
    return { messages, msg, chat };
}
app.get('/api/messages/:messageId/comments', (req, res) => {
    const p = postFor(req, res); if (!p) return;
    const ix = lazyIdx(), list = p.msg.comments || [];
    res.json({ count: list.length, comments: list.slice(-200).map(c => cmOut(c, ix)) });
});
app.post('/api/messages/:messageId/comments', async (req, res) => {
    const p = postFor(req, res); if (!p) return;
    const text = String(req.body.text || '').trim().slice(0, 500);
    if (!text) return res.status(400).json({ error: 'Напишите комментарий' });
    const c = { id: crypto.randomUUID(), userId: req.uid, text, at: new Date().toISOString() };
    p.msg.comments = (p.msg.comments || []).concat(c).slice(-1000);
    write(F.messages, p.messages);
    const ix = lazyIdx(), o = outMsg(p.msg, p.chat, ix), out = cmOut(c, ix);
    sendTo(p.chat.members, { type: 'comments', data: { chatId: p.chat.id, messageId: p.msg.id, cc: o.cc, ca: o.ca || [], comment: out } });
    await persist();
    res.json({ success: true, comment: out });
});
app.delete('/api/messages/:messageId/comments/:cid', async (req, res) => {
    const p = postFor(req, res); if (!p) return;
    const c = (p.msg.comments || []).find(x => x.id === req.params.cid);
    if (!c) return res.status(404).json({ error: 'Комментарий не найден' });
    if (c.userId !== req.uid && p.chat.creator !== req.uid) return res.status(403).json({ error: 'Нельзя удалить чужой комментарий' });
    p.msg.comments = p.msg.comments.filter(x => x !== c);
    write(F.messages, p.messages);
    const o = outMsg(p.msg, p.chat);
    sendTo(p.chat.members, { type: 'comments', data: { chatId: p.chat.id, messageId: p.msg.id, cc: o.cc, ca: o.ca || [], removed: c.id } });
    await persist();
    res.json({ success: true });
});

// Реакция на сообщение: одна на человека; тот же смайл ещё раз — снять
app.post('/api/messages/:messageId/react', async (req, res) => {
    const me = req.uid, emoji = String(req.body.emoji || '');
    if (!REACTIONS.includes(emoji)) return res.status(400).json({ error: 'Такой реакции нет' });
    const messages = read(F.messages), msg = messages.find(m => m.id === req.params.messageId);
    const chat = msg && chatOf(msg.chatId);
    if (!msg || !chat || !chat.members.includes(me)) return res.status(404).json({ error: 'Сообщение не найдено' });
    if (chat.type === 'direct') {
        const other = chat.members.find(i => i !== me);
        if (other === BOT_ID || (other && blockedEither(me, other))) return res.status(400).json({ error: 'Нельзя поставить реакцию' });
    }
    const r = msg.reactions = msg.reactions || {};
    const had = (r[emoji] || []).includes(me);
    for (const e of Object.keys(r)) { r[e] = r[e].filter(id => id !== me); if (!r[e].length) delete r[e]; }
    if (!had) (r[emoji] = r[emoji] || []).push(me);
    if (!Object.keys(r).length) delete msg.reactions;
    write(F.messages, messages);
    sendTo(chat.members, { type: 'reaction', data: { chatId: msg.chatId, messageId: msg.id, reactions: msg.reactions || {} } });
    await persist();
    res.json({ success: true });
});

// ---------- Удаление чатов ----------
// Личный: «у меня» скрывает историю только для меня, both=1 удаляет у обоих.
// Группа: выйти (создатель с both=1 удаляет для всех). Канал: автор удаляет, подписчик отписывается.
app.delete('/api/chats/:chatId', async (req, res) => {
    const me = req.uid, both = req.query.both === '1';
    const chats = read(F.chats), chat = chats.find(c => String(c.id) === req.params.chatId);
    if (!chat || !chat.members.includes(me)) return res.status(404).json({ error: 'Чат не найден' });
    const members = [...chat.members];
    const dropAll = () => {
        write(F.chats, chats.filter(c => c !== chat));
        write(F.messages, read(F.messages).filter(m => m.chatId !== chat.id));
    };
    const leave = () => {
        chat.members = chat.members.filter(id => id !== me);
        if (chat.members.length) write(F.chats, chats); else dropAll();
    };
    if (chat.type === 'direct') {
        if (both) dropAll();
        else { chat.cleared = chat.cleared || {}; chat.cleared[me] = new Date().toISOString(); write(F.chats, chats); }
    } else if (chat.type === 'group') {
        if (both && chat.creator === me) dropAll(); else leave();
    } else if (chat.creator === me) dropAll();
    else leave();
    sendTo(members, { type: 'chat' });
    await persist();
    res.json({ success: true });
});

// ---------- WebSocket ----------
wss.on('connection', ws => {
    let uid = null;
    ws.isAlive = true;
    ws.on('pong', () => { ws.isAlive = true; });
    ws.on('error', () => {}); // ошибка одного клиента не должна ронять сервер

    ws.on('message', raw => {
        try {
            const m = JSON.parse(raw);

            if (m.type === 'connect') {
                const sess = read(F.sessions)[m.token];
                if (!sess) { ws.send(JSON.stringify({ type: 'auth', data: { ok: false } })); return ws.close(); }
                uid = sess.uid;
                ws.active = m.active !== false;
                if (!sockets.has(uid)) sockets.set(uid, new Set());
                sockets.get(uid).add(ws);
                broadcast({ type: 'presence', data: { userId: uid, online: true } });
                const u = read(F.users).find(x => x.id === uid);
                if (u) ws.send(JSON.stringify({ type: 'wallet', data: walletOf(u) }));
                return;
            }
            if (!uid) return;
            if (m.type === 'active') { ws.active = !!m.active; return; } // вкладка открыта и видна — идёт время для Mars

            const chats = read(F.chats);
            const chat = chats.find(c => String(c.id) === String(m.chatId));
            if (!chat || !chat.members.includes(uid)) return;

            if (m.type === 'message') {
                if (chat.type === 'channel' && chat.creator !== uid && !chat.open) return; // в канал пишет только автор (в текстовом — все)
                if (chat.type === 'direct') {
                    const other = chat.members.find(i => i !== uid);
                    if (other === BOT_ID) return; // с ботом общаются кнопками
                    if (other && blockedEither(uid, other)) {
                        return ws.send(JSON.stringify({ type: 'error', data: { cid: m.cid, text: iBlocked(uid, other) ? 'Вы заблокировали этого пользователя' : 'Не удалось отправить сообщение' } }));
                    }
                }
                const text = String(m.text || '').trim().slice(0, 4000);
                let media;
                if (m.media && fileOk(m.media.url)) {
                    media = { kind: kindOf(m.media.url), url: m.media.url };
                    const w = Number(m.media.w), h = Number(m.media.h);
                    if (w > 0 && h > 0 && w < 20000 && h < 20000) { media.w = Math.round(w); media.h = Math.round(h); }
                    const dur = Number(m.media.dur);
                    if (media.kind === 'voice' && dur > 0 && dur < 3600) media.dur = Math.round(dur);
                }
                if (!text && !media) return;

                const msg = {
                    id: crypto.randomUUID(), chatId: chat.id, senderId: uid, text,
                    ...(media && { media }), timestamp: new Date().toISOString()
                };
                if (chat.type === 'channel' && !chat.open) { msg.viewers = [uid]; msg.fv = rollFv(fakeInChannel(chat, read(F.users))); }
                const msgs = read(F.messages);
                msgs.push(msg);
                write(F.messages, msgs);
                sendTo(chat.members, { type: 'message', data: outMsg(msg, chat), cid: m.cid ? String(m.cid).slice(0, 40) : undefined }); // только участникам чата; cid — чтобы отправитель заменил своё временное
                notifyMessage(chat, msg); сообщение

            } else if (m.type === 'read') {
                if (chat.type === 'channel' && !chat.open) markViews(chat, uid);
                // Помечаем прочитанным всё до последнего сообщения включительно
                const last = read(F.messages).filter(x => x.chatId === chat.id).pop();
                const at = last ? last.timestamp : '';
                chat.reads = chat.reads || {};
                if (!at || (chat.reads[uid] || '') >= at) return;
                chat.reads[uid] = at;
                write(F.chats, chats);
                sendTo(chat.members, { type: 'read', data: { chatId: chat.id, userId: uid, at } });

            } else if (m.type === 'typing') {
                if (chat.type === 'channel') return;
                if (chat.type === 'direct' && blockedEither(uid, chat.members.find(i => i !== uid))) return;
                sendTo(chat.members, { type: 'typing', data: { chatId: chat.id, userId: uid } }, ws);
            }
        } catch (err) {
            console.error('WS error:', err);
        }
    });

    ws.on('close', () => {
        if (!uid || !sockets.has(uid)) return;
        sockets.get(uid).delete(ws);
        if (!sockets.get(uid).size) {
            sockets.delete(uid);
            broadcast({ type: 'presence', data: { userId: uid, online: false } });
        }
    });
});

// Ошибки в JSON, чтобы клиент мог показать понятный текст
app.use((err, req, res, next) => {
    const big = err.type === 'entity.too.large';
    res.status(big ? 413 : err.status || 500).json({ error: big ? 'Файл слишком большой (максимум 50 МБ)' : 'Ошибка сервера' });
});

// Пинг раз в 25 секунд: хостинги закрывают «молчащие» соединения, а мёртвые мы убираем из «в сети»
setInterval(() => wss.clients.forEach(ws => {
    if (!ws.isAlive) return ws.terminate();
    ws.isAlive = false;
    ws.ping();
}), 25000);

// Mars за время: раз в минуту каждому, у кого приложение открыто и видно (несколько вкладок считаются за одну)
setInterval(() => {
    const ids = new Set();
    sockets.forEach((set, id) => { if ([...set].some(w => w.active)) ids.add(id); });
    if (!ids.size) return;
    const users = read(F.users);
    users.forEach(u => {
        if (!ids.has(u.id) || isUnlimited(u)) return;
        u.mars = (u.mars || 0) + MARS_PER_MIN;
        sendTo([u.id], { type: 'wallet', data: walletOf(u) });
    });
    write(F.users, users);
}, 60000);

process.on('uncaughtException', e => console.error('Необработанная ошибка:', e));
process.on('unhandledRejection', e => console.error('Необработанный промис:', e));

async function init() {
    let hasMeta = false;
    if (process.env.DATABASE_URL) {
        const { Pool } = require('pg');
        const cs = new URL(process.env.DATABASE_URL);
        cs.searchParams.delete('channel_binding');
        cs.searchParams.delete('sslmode');
        const local = ['localhost', '127.0.0.1'].includes(cs.hostname);
        pool = new Pool({
            connectionString: cs.toString(), ssl: local ? false : { rejectUnauthorized: false },
            max: 4, keepAlive: true, connectionTimeoutMillis: 20000
        });
        pool.on('error', e => console.error('DB:', e.message)); // обрыв простаивающего соединения не должен ронять сервер
        await pool.query('create table if not exists kv (key text primary key, value jsonb not null, updated_at timestamptz not null default now())');
        await pool.query('create table if not exists files (name text primary key, data bytea not null, created_at timestamptz not null default now())');
        const { rows } = await pool.query('select key, value from kv');
        rows.forEach(r => {
            if (r.key === 'meta') { meta = r.value; hasMeta = true; } else mem[r.key] = JSON.stringify(r.value);
        });
    }
    // Чего нет в базе (первый запуск) — берём из локальных файлов, иначе пустой список
    for (const k of ['users', 'messages', 'chats', 'gifts', 'sessions', 'blocks']) {
        if (mem[k] === undefined) {
            const d = loadFile(F[k]);
            mem[k] = JSON.stringify((k === 'sessions' || k === 'blocks') && Array.isArray(d) ? {} : d); // объекты {токен: …}, {кто: [кого]}
            if (pool) dirty.add(k);
        }
    }
    if (pool) {
        if (!hasMeta) await pool.query("insert into kv (key, value) values ('meta', $1::jsonb) on conflict (key) do nothing", [JSON.stringify(meta)]);
    } else {
        const mf = path.join(DIR, 'meta.json');
        try { meta = JSON.parse(fs.readFileSync(mf, 'utf8')); } catch { fs.writeFileSync(mf, JSON.stringify(meta)); }
    }
    migrate();
    ensureBot();
    await ensureVapid();
    await persist();

    console.log(`💾 Хранилище: ${pool ? 'база данных PostgreSQL' : 'файлы в ' + DIR}`);
    console.log(EMAIL_VERIFY ? `✉️ Код на почту при регистрации: включён (${MAIL_PROVIDER})` : '✉️ Код на почту: выключен (нет BREVO_API_KEY/RESEND_API_KEY и MAIL_FROM)');
    if (!pool) console.warn('⚠️ DATABASE_URL не задан: данные лежат во временных файлах и пропадут при перезапуске на бесплатном хостинге');
    console.log(`👤 Пользователей: ${read(F.users).length}, база создана: ${meta.createdAt}`);
}

// При остановке сервера дописываем в базу всё, что не успело сохраниться
for (const sig of ['SIGTERM', 'SIGINT']) process.on(sig, async () => { await persist(); process.exit(0); });

// Render усыпляет бесплатный сервис через 15 минут без входящих запросов.
// Пока сервер работает, он сам обращается к своему адресу раз в 8 минут (выключить: KEEP_ALIVE=0).
function keepAlive() {
    const url = process.env.RENDER_EXTERNAL_URL; // Render задаёт её сам
    if (!url || process.env.KEEP_ALIVE === '0' || typeof fetch !== 'function') return;
    setInterval(() => fetch(url.replace(/\/$/, '') + '/api/health').catch(e => console.error('keep-alive:', e.message)), 8 * 60 * 1000);
    console.log('⏰ keep-alive включён:', url);
}

const PORT = process.env.PORT || 3000;
init()
    .then(() => server.listen(PORT, () => { console.log(`🚀 SAIMONGRAM запущен: http://localhost:${PORT}`); keepAlive(); }))
    .catch(e => { console.error('Не удалось запустить сервер:', e); process.exit(1); }); // без данных не стартуем, чтобы не затереть базу пустым состоянием
