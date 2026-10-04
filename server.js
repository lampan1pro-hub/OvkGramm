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
    blocks: path.join(DIR, 'blocks.json'),
    walls: path.join(DIR, 'walls.json') // NFT-обои: коллекция экземпляров {kind, serial, ownerId}
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
    write(F.messages, msgs);
    write(F.chats, chats);
}

// Метка «когда создана база»: если после перезапусков она каждый раз новая — данные не сохраняются
let meta = { createdAt: new Date().toISOString() };

app.get('/api/health', (req, res) => res.json({ ok: true, storage: pool ? 'database' : 'files', users: read(F.users).length, dbCreatedAt: meta.createdAt }));

// ---------- Онлайн ----------
const sockets = new Map(); // userId -> Set<ws>

// Галочка у аккаунтов с этими юзернеймами. Свой список: VERIFIED_HANDLES=saimon,durov,lesha
const VERIFIED = new Set((process.env.VERIFIED_HANDLES || 'saimon,durov,lesha,NAKRYTKA_BOT')
    .split(',').map(x => x.trim().replace(/^@/, '').toLowerCase()).filter(Boolean));

// Бесконечные Mars. Свой список: UNLIMITED_MARS_HANDLES=saimon,другой
const UNLIMITED = new Set((process.env.UNLIMITED_MARS_HANDLES || 'saimon')
    .split(',').map(x => x.trim().replace(/^@/, '').toLowerCase()).filter(Boolean));
const isUnlimited = u => UNLIMITED.has(u.handle);
const walletOf = u => ({ mars: u.mars || 0, unlimited: isUnlimited(u) });

const pub = u => ({
    id: u.id, username: u.username, handle: u.handle, verified: VERIFIED.has(u.handle), bio: u.bio || '',
    avatar: u.avatar || '', createdAt: u.createdAt, online: sockets.has(u.id), bot: !!u.isBot
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
function shape(chat, me, users, list) {
    const clr = (chat.cleared || {})[me] || ''; // «удалил чат у себя»: старые сообщения скрыты
    if (clr) list = list.filter(m => m.timestamp > clr);
    const byId = id => users.find(u => u.id === id);
    const reads = chat.reads || {};
    const o = {
        id: chat.id, type: chat.type, name: chat.name, avatar: chat.avatar || '',
        creator: chat.creator, createdAt: chat.createdAt, reads,
        last: list[list.length - 1] || null,
        unread: list.filter(m => m.senderId !== me && m.timestamp > (reads[me] || '')).length,
        wall: (chat.walls || {})[me] || null // NFT-обои, которые я поставил на этот чат
    };
    if (chat.type === 'direct') {
        const p = byId(chat.members.find(i => i !== me));
        o.peer = p ? pub(p) : null;
    } else if (chat.type === 'group') {
        o.members = chat.members.map(byId).filter(Boolean).map(pub);
    } else { // канал
        o.description = chat.description || '';
        o.handle = chat.handle || '';
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
    const user = users.find(u => ((u.email || '').toLowerCase() === login || u.handle === login) && checkPass(password, u.password));
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
        .filter(u => u.id !== me && (u.handle.includes(s) || u.username.toLowerCase().includes(s)))
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
        const peerU = users.find(u => u.id === peer);
        if (peerU && peerU.isBot) {
            const msgs = read(F.messages);
            msgs.push({ id: crypto.randomUUID(), chatId: chat.id, senderId: peer,
                text: `👋 Привет! Я бот накрутки подписчиков.\nНажми «▶️ Старт» внизу, выбери канал и количество — 1 подписчик = ${BOOST_PRICE} Mars. Подписчики появятся в твоём канале, как настоящие.`,
                timestamp: new Date().toISOString() });
            write(F.messages, msgs);
        }
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
    avatar: c.avatar || '', subscribers: c.members.length, subscribed: c.members.includes(me)
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
        creator: me, members: [me], reads: {}, createdAt: new Date().toISOString()
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

    const { name, description, handle, avatar } = req.body;
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
const giftOut = (g, users) => {
    const f = users.find(u => u.id === g.fromId);
    return {
        id: g.id, kind: g.kind, serial: g.serial, at: g.at,
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
        gift: { kind: g.kind, serial: g.serial }, timestamp: new Date().toISOString()
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
    res.json({ catalog: CATALOG.map(c => ({ ...c, sold: sold[c.kind] || 0 })), ...(me ? walletOf(me) : { mars: 0 }), perMin: MARS_PER_MIN, boostPrice: BOOST_PRICE });
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
    if (toId !== me && blockedEither(me, toId)) return res.status(400).json({ error: 'Нельзя отправить подарок: один из вас заблокировал другого' });

    const minted = gifts.filter(g => g.kind === item.kind).length;
    if (minted >= item.supply) return res.status(400).json({ error: 'Этот подарок распродан' });
    const unlimited = isUnlimited(buyer);
    if (!unlimited && (buyer.mars || 0) < item.price)
        return res.status(400).json({ error: `Не хватает Mars: нужно ${item.price}, у вас ${buyer.mars || 0}` });

    const now = new Date().toISOString();
    if (!unlimited) buyer.mars -= item.price;
    const gift = { id: 'k_' + crypto.randomUUID(), kind: item.kind, serial: minted + 1, ownerId: toId, fromId: me, price: item.price, at: now };
    gifts.push(gift);
    write(F.users, users);
    write(F.gifts, gifts);
    sendTo([me], { type: 'wallet', data: walletOf(buyer) });
    if (toId !== me) {
        giftMessage(me, toId, gift, String(req.body.message || '').trim().slice(0, 120));
        sendTo([toId], { type: 'gift', data: { chatId: directId(me, toId) } });
    }
    await persist();
    res.json({ success: true, ...walletOf(buyer), gift: giftOut(gift, users) });
});

// Передать свой подарок другому (бесплатно)
app.post('/api/gifts/:id/transfer', async (req, res) => {
    const me = req.uid, toId = Number(req.body.toId);
    const gifts = read(F.gifts), g = gifts.find(x => x.id === req.params.id);
    if (!g || g.ownerId !== me) return res.status(403).json({ error: 'Это не ваш подарок' });
    if (toId === me || !read(F.users).some(u => u.id === toId)) return res.status(400).json({ error: 'Выберите другого получателя' });
    if (blockedEither(me, toId)) return res.status(400).json({ error: 'Нельзя передать подарок: один из вас заблокировал другого' });
    g.ownerId = toId;
    g.fromId = me;
    g.at = new Date().toISOString();
    write(F.gifts, gifts);
    giftMessage(me, toId, g, String(req.body.message || '').trim().slice(0, 120));
    await persist();
    res.json({ success: true });
});



// ---------- Бот накрутки NAKRYTKA_BOT ----------
// Обычный пользователь с флагом isBot: находится в поиске, виден в списке подписчиков,
// онлайн не бывает. За Mars создаёт аккаунты со случайными никами и подписывает на канал.
const BOOST_PRICE = Number(process.env.BOOST_PRICE ?? 3); // Mars за одного подписчика
const BOT_HANDLE = 'NAKRYTKA_BOT';
const MAX_BOTS = 20000; // потолок, чтобы база не разрасталась бесконечно
const NICK_A = ['Cool','Super','Mega','Pro','Lucky','Crazy','Happy','Neon','Cyber','Dark','Swift','Clever','Cosmic','Royal','Iron','Wild','Silent','Golden','Pixel','Turbo'];
const NICK_B = ['Fox','Wolf','Bear','Cat','Dragon','Tiger','Hawk','Shark','Panda','Robot','Wizard','Ninja','Phoenix','Raccoon','Owl','Viper','Falcon','Whale','Raven','Crow'];
const botUser = () => read(F.users).find(u => u.handle === BOT_HANDLE);

function ensureBot() {
    const users = read(F.users);
    if (users.some(u => u.handle === BOT_HANDLE)) return;
    users.push({
        id: Date.now(), username: 'Накрутка 🤖', handle: BOT_HANDLE,
        email: 'nakrytka@bot.local', password: hashPass(crypto.randomBytes(16).toString('hex')),
        bio: `Бот накрутки подписчиков · 1 подписчик = ${BOOST_PRICE} Mars`,
        avatar: '', mars: 0, isBot: true, createdAt: new Date().toISOString()
    });
    write(F.users, users);
    console.log('🤖 Бот накрутки создан: @' + BOT_HANDLE);
}

app.post('/api/boost', async (req, res) => {
    const chId = String(req.body.channelId || '');
    const count = Math.floor(Number(req.body.count));
    const chats = read(F.chats);
    const ch = chats.find(c => c.id === chId && c.type === 'channel');
    if (!ch || ch.creator !== req.uid) return res.status(400).json({ error: 'Выберите свой канал' });
    if (!(count >= 1)) return res.status(400).json({ error: 'Минимум 1 подписчик' });
    if (count > 500) return res.status(400).json({ error: 'За раз можно добавить не больше 500 подписчиков' });

    const users = read(F.users);
    const buyer = users.find(u => u.id === req.uid);
    const bot = botUser();
    if (!bot) return res.status(500).json({ error: 'Бот не найден' });
    const cost = count * BOOST_PRICE;
    const unl = isUnlimited(buyer);
    if (!unl && (buyer.mars || 0) < cost) return res.status(400).json({ error: `Не хватает Mars: нужно ${cost}, у вас ${buyer.mars || 0}` });
    if (users.filter(u => u.isBot).length + count > MAX_BOTS) return res.status(400).json({ error: 'Лимит подписчиков исчерпан, попробуйте меньше' });

    // Случайные ники, как у живых: Прилагательное + Существо + цифры
    const handles = new Set(users.map(u => u.handle));
    const fresh = [];
    for (let i = 0; i < count; i++) {
        let a, b, handle;
        do {
            a = NICK_A[Math.floor(Math.random() * NICK_A.length)];
            b = NICK_B[Math.floor(Math.random() * NICK_B.length)];
            handle = (a + b + '_' + Math.floor(1000 + Math.random() * 9000)).toLowerCase();
        } while (handles.has(handle));
        handles.add(handle);
        fresh.push({
            id: Date.now() + Math.floor(Math.random() * 1e6) + i,
            username: a + ' ' + b, handle,
            email: handle + '@bot.local', password: hashPass(crypto.randomBytes(16).toString('hex')),
            bio: '', avatar: '', mars: 0, isBot: true, createdAt: new Date().toISOString()
        });
    }

    users.push(...fresh);
    // Новые подписчики не «непрочитанные»: история для них уже прочитана
    const last = read(F.messages).filter(m => m.chatId === ch.id).pop();
    ch.reads = ch.reads || {};
    fresh.forEach(u => { ch.reads[u.id] = last ? last.timestamp : ''; ch.members.push(u.id); });
    if (!unl) buyer.mars -= cost;
    write(F.users, users);
    write(F.chats, chats);

    // Подтверждение от бота в личном чате с покупателем
    const bchatId = directId(bot.id, buyer.id);
    let bchat = read(F.chats).find(c => c.id === bchatId);
    if (!bchat) {
        const chats2 = read(F.chats);
        bchat = { id: bchatId, type: 'direct', members: [bot.id, buyer.id], reads: {}, createdAt: new Date().toISOString() };
        chats2.push(bchat);
        write(F.chats, chats2);
    }
    const msg = {
        id: crypto.randomUUID(), chatId: bchatId, senderId: bot.id,
        text: `✅ Готово! Начислено ${count} подписчиков на канал «${ch.name}». Списано 👽 ${cost} Mars.`,
        timestamp: new Date().toISOString()
    };
    const msgs = read(F.messages);
    msgs.push(msg);
    write(F.messages, msgs);

    sendTo([buyer.id], { type: 'message', data: msg });
    sendTo(ch.members, { type: 'chat' }); // у всех обновится число подписчиков
    sendTo([buyer.id], { type: 'wallet', data: walletOf(buyer) });
    await persist();
    res.json({ success: true, added: count, cost, ...walletOf(buyer) });
});

// ---------- NFT-обои ----------
// Пак обоев: случайный дроп, вероятность зависит от редкости (сумма chance = 100%).
// Каждый выпавший экземпляр уникален: у него свой серийный номер внутри вида.
// Обои ставятся на все чаты сразу (wallDefault у пользователя) или на один чат (chat.walls[uid]).
const WALL_PACK_PRICE = Number(process.env.WALL_PACK_PRICE ?? 200);
const WALL_CATALOG = [
    // обычные — падают чаще всего
    { kind: 'aurora',  rarity: 'common',    name: 'Сияние',    chance: 16, bg: ['#43e97b', '#38f9d7'], pattern: '🌌' },
    { kind: 'sunset',  rarity: 'common',    name: 'Закат',     chance: 15, bg: ['#ff9966', '#ff5e62'], pattern: '🌇' },
    { kind: 'ocean',   rarity: 'common',    name: 'Океан',     chance: 14, bg: ['#2bc0e4', '#085078'], pattern: '🌊' },
    { kind: 'sakura',  rarity: 'common',    name: 'Сакура',    chance: 10, bg: ['#f6d5f7', '#fbe9d7'], pattern: '🌸' },
    // редкие
    { kind: 'neon',    rarity: 'rare',      name: 'Неон',      chance: 12, bg: ['#f857a6', '#7a5cff'], pattern: '💠' },
    { kind: 'matrix',  rarity: 'rare',      name: 'Матрица',   chance: 9,  bg: ['#000428', '#004e92'], pattern: '👾' },
    { kind: 'lava',    rarity: 'rare',      name: 'Лава',      chance: 6,  bg: ['#e8505b', '#f7b733'], pattern: '🔥' },
    // эпические
    { kind: 'galaxy',  rarity: 'epic',      name: 'Галактика', chance: 7,  bg: ['#360033', '#0b8793'], pattern: '🌠' },
    { kind: 'ice',     rarity: 'epic',      name: 'Лёд',       chance: 5,  bg: ['#83a4d4', '#b6fbff'], pattern: '❄️' },
    // легендарные
    { kind: 'gold',    rarity: 'legendary', name: 'Золото',    chance: 4,  bg: ['#bf953f', '#fcf6ba'], pattern: '✨' },
    { kind: 'phoenix', rarity: 'legendary', name: 'Феникс',    chance: 2,  bg: ['#f12711', '#f5af19'], pattern: '🦅' },
    // мифические — почти не падают
    { kind: 'void',    rarity: 'mythic',    name: 'Пустота',   chance: 1,  bg: ['#000000', '#130f26'], pattern: '👁' }
];
const wallInfo = k => WALL_CATALOG.find(w => w.kind === k);
const ownWalls = uid => read(F.walls).filter(w => w.ownerId === uid);
const wallOwned = (uid, ref) => !!ref && ownWalls(uid).some(w => w.kind === ref.kind && w.serial === ref.serial);

function rollWall() {
    const total = WALL_CATALOG.reduce((s, w) => s + w.chance, 0);
    let r = Math.random() * total;
    for (const w of WALL_CATALOG) { r -= w.chance; if (r <= 0) return w; }
    return WALL_CATALOG[0];
}

app.get('/api/walls', (req, res) => {
    const u = read(F.users).find(x => x.id === req.uid);
    res.json({
        packPrice: WALL_PACK_PRICE,
        catalog: WALL_CATALOG.map(w => ({ ...w, sold: read(F.walls).filter(x => x.kind === w.kind).length })),
        mine: ownWalls(req.uid),
        defaultWall: u ? (u.wallDefault || null) : null
    });
});

// Открыть пак: списание Mars и случайный дроп с серийным номером
app.post('/api/walls/open', async (req, res) => {
    const users = read(F.users), u = users.find(x => x.id === req.uid);
    if (!u) return res.status(401).json({ error: 'Войдите в аккаунт' });
    const unl = isUnlimited(u);
    if (!unl && (u.mars || 0) < WALL_PACK_PRICE)
        return res.status(400).json({ error: `Не хватает Mars: нужно ${WALL_PACK_PRICE}, у вас ${u.mars || 0}` });
    const item = rollWall();
    const walls = read(F.walls);
    const w = {
        id: 'w_' + crypto.randomUUID(), kind: item.kind,
        serial: walls.filter(x => x.kind === item.kind).length + 1,
        ownerId: u.id, at: new Date().toISOString()
    };
    walls.push(w);
    if (!unl) u.mars -= WALL_PACK_PRICE;
    write(F.walls, walls);
    write(F.users, users);
    sendTo([u.id], { type: 'wallet', data: walletOf(u) });
    await persist();
    res.json({ success: true, ...walletOf(u), wall: { ...w, info: item } });
});

// Обои по умолчанию — на все чаты, где нет своих
app.put('/api/walls/default', (req, res) => {
    const users = read(F.users), u = users.find(x => x.id === req.uid);
    if (!u) return res.status(401).json({ error: 'Войдите в аккаунт' });
    const ref = req.body.wall || null;
    if (ref && !wallOwned(u.id, ref)) return res.status(400).json({ error: 'Этих обоев нет в вашей коллекции' });
    u.wallDefault = ref;
    write(F.users, users);
    res.json({ success: true, defaultWall: ref });
});

// Обои конкретного чата/канала (видит только тот, кто поставил)
app.put('/api/chats/:chatId/wall', (req, res) => {
    const chats = read(F.chats);
    const chat = chats.find(c => String(c.id) === req.params.chatId);
    if (!chat || !chat.members.includes(req.uid)) return res.status(403).json({ error: 'Нет доступа' });
    const ref = req.body.wall || null;
    if (ref && !wallOwned(req.uid, ref)) return res.status(400).json({ error: 'Этих обоев нет в вашей коллекции' });
    chat.walls = chat.walls || {};
    if (ref) chat.walls[req.uid] = ref; else delete chat.walls[req.uid];
    write(F.chats, chats);
    res.json({ success: true });
});

app.get('/api/messages/:chatId', (req, res) => {
    const me = Number(req.query.userId);
    const chat = read(F.chats).find(c => String(c.id) === req.params.chatId);
    if (!chat || !chat.members.includes(me)) return res.status(403).json({ error: 'Нет доступа' });
    const clr = (chat.cleared || {})[me] || '';
    res.json(read(F.messages).filter(m => m.chatId === chat.id && m.timestamp > clr).slice(-500));
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
                if (chat.type === 'channel' && chat.creator !== uid) return; // в канал пишет только автор
                if (chat.type === 'direct') {
                    const other = chat.members.find(i => i !== uid);
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
                const msgs = read(F.messages);
                msgs.push(msg);
                write(F.messages, msgs);
                sendTo(chat.members, { type: 'message', data: msg, cid: m.cid ? String(m.cid).slice(0, 40) : undefined }); // только участникам чата; cid — чтобы отправитель заменил своё временное сообщение

            } else if (m.type === 'read') {
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
    for (const k of ['users', 'messages', 'chats', 'gifts', 'sessions', 'blocks', 'walls']) {
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
