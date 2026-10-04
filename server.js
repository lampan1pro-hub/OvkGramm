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
const OPEN_API = new Set(['/register', '/login', '/health']);
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
    if (!/^[\w-]+\.(jpg|png|webp|gif|mp4|webm|mov)$/.test(name)) return res.sendStatus(404);
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

// Файлы
const MIME = {
    'image/jpeg': 'jpg', 'image/png': 'png', 'image/webp': 'webp', 'image/gif': 'gif',
    'video/mp4': 'mp4', 'video/webm': 'webm', 'video/quicktime': 'mov'
};
const URL_RE = /^\/uploads\/[\w-]+\.(jpg|png|webp|gif|mp4|webm|mov)$/;
const fileOk = u => typeof u === 'string' && URL_RE.test(u) && fs.existsSync(path.join(UP, path.basename(u)));
const kindOf = u => /\.(mp4|webm|mov)$/.test(u) ? 'video' : 'image';
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
    // Подарки, купленные до появления фонов, получают случайный фон (и в сообщениях тоже)
    const gifts = read(F.gifts);
    let gch = false;
    gifts.forEach(g => { if (!g.bd) { g.bd = pickBackdrop().id; gch = true; } });
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
const VERIFIED = new Set((process.env.VERIFIED_HANDLES || 'saimon,durov,lesha')
    .split(',').map(x => x.trim().replace(/^@/, '').toLowerCase()).filter(Boolean));

// Бесконечные Mars. Свой список: UNLIMITED_MARS_HANDLES=saimon,другой
const UNLIMITED = new Set((process.env.UNLIMITED_MARS_HANDLES || 'saimon')
    .split(',').map(x => x.trim().replace(/^@/, '').toLowerCase()).filter(Boolean));
const isUnlimited = u => UNLIMITED.has(u.handle);
const walletOf = u => ({ mars: u.mars || 0, unlimited: isUnlimited(u) });

const pub = u => ({
    id: u.id, username: u.username, handle: u.handle, verified: VERIFIED.has(u.handle), bio: u.bio || '',
    avatar: u.avatar || '', createdAt: u.createdAt, online: sockets.has(u.id), ...(u.bot && { bot: true }), ...(u.acc && { acc: u.acc })
});
const self = u => ({ ...pub(u), email: u.email, ...walletOf(u) });

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

    const isImg = !['mp4', 'webm', 'mov'].includes(ext);
    if (req.query.kind === 'avatar' && (!isImg || buf.length > 5 * 1024 * 1024))
        return res.status(400).json({ error: 'Для аватарки нужно фото до 5 МБ' });

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

// ---------- Авторизация ----------
app.post('/api/register', async (req, res) => {
    const username = String(req.body.username || '').trim().slice(0, 40);
    const handle = String(req.body.handle || '').trim().replace(/^@/, '').toLowerCase();
    const email = String(req.body.email || '').trim().toLowerCase();
    const password = String(req.body.password || '');

    if (!username || !email || !password) return res.status(400).json({ error: 'Заполните все поля' });
    if (!email.includes('@')) return res.status(400).json({ error: 'Введите корректный email' });
    if (password.length < 6) return res.status(400).json({ error: 'Пароль — минимум 6 символов' });
    if (!HANDLE_RE.test(handle)) return res.status(400).json({ error: 'Юзернейм: 3–20 символов, латиница, цифры и _' });

    const users = read(F.users);
    if (users.some(u => u.email === email)) return res.status(400).json({ error: 'Этот email уже зарегистрирован' });
    if (users.some(u => u.handle === handle)) return res.status(400).json({ error: 'Этот юзернейм уже занят' });

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
    const user = users.find(u => !u.bot && !u.fake && ((u.email || '').toLowerCase() === login || u.handle === login) && checkPass(password, u.password));
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
        .filter(u => !u.fake && u.id !== me && (u.handle.includes(s) || u.username.toLowerCase().includes(s)))
        .slice(0, 30).map(pub));
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
        if (users.some(x => x.id !== u.id && x.handle === h)) return res.status(400).json({ error: 'Этот юзернейм уже занят' });
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
    avatar: c.avatar || '', subscribers: c.members.length, subscribed: c.members.includes(me), open: !!c.open
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
    if (!q) return res.json([]);
    res.json(read(F.chats)
        .filter(c => c.type === 'channel' && (c.name.toLowerCase().includes(q) || (c.handle || '').includes(q) || (c.description || '').toLowerCase().includes(q)))
        .sort((a, b) => b.members.length - a.members.length)
        .slice(0, 30).map(c => chanPub(c, me)));
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
function pickBackdrop() {
    let r = Math.random() * BACKDROPS.reduce((a, b) => a + b.pct, 0);
    for (const b of BACKDROPS) { if ((r -= b.pct) < 0) return b; }
    return BACKDROPS[0];
}

const giftOut = (g, users) => {
    const f = users.find(u => u.id === g.fromId);
    return {
        id: g.id, kind: g.kind, serial: g.serial, at: g.at, bd: g.bd, price: g.price, note: g.note || '',
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
}

app.get('/api/shop', (req, res) => {
    const sold = {};
    read(F.gifts).forEach(g => { sold[g.kind] = (sold[g.kind] || 0) + 1; });
    const me = read(F.users).find(u => u.id === req.uid);
    res.json({ backdrops: BACKDROPS, catalog: CATALOG.map(c => ({ ...c, sold: sold[c.kind] || 0 })), ...(me ? walletOf(me) : { mars: 0 }), perMin: MARS_PER_MIN });
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
    const gift = { id: 'k_' + crypto.randomUUID(), kind: item.kind, serial: minted + 1, ownerId: toId, fromId: me, price: item.price, at: now, bd: pickBackdrop().id, note };
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
    if (req.body.on) u.acc = { gid: g.id, kind: g.kind, serial: g.serial, bd: g.bd };
    else delete u.acc;
    write(F.users, users);
    broadcast({ type: 'chat' }); // у всех обновится ник
    await persist();
    res.json({ success: true, acc: u.acc || null });
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
                sendTo(chat.members, { type: 'message', data: outMsg(msg, chat), cid: m.cid ? String(m.cid).slice(0, 40) : undefined }); // только участникам чата; cid — чтобы отправитель заменил своё временное сообщение

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
    await persist();

    console.log(`💾 Хранилище: ${pool ? 'база данных PostgreSQL' : 'файлы в ' + DIR}`);
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
