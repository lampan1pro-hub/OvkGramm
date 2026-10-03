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

// ---------- Хранилище ----------
// Папку с данными можно вынести на постоянный диск хостинга: DATA_DIR=/путь/к/диску
const DIR = process.env.DATA_DIR ? path.resolve(process.env.DATA_DIR) : path.join(__dirname, 'data');
const UP = path.join(DIR, 'uploads');
fs.mkdirSync(UP, { recursive: true });
const F = {
    users: path.join(DIR, 'users.json'),
    messages: path.join(DIR, 'messages.json'),
    chats: path.join(DIR, 'chats.json')
};

// Чтение. Если файл повреждён — берём резервную копию (.bak).
// Раньше при любой ошибке возвращался пустой список, и следующая запись стирала ВСЕХ пользователей.
function read(f) {
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
    if (fs.existsSync(f)) { // нечитаемый файл откладываем в сторону, ничего не удаляя
        fs.renameSync(f, `${f}.corrupt-${Date.now()}`);
        console.error(`⚠️ ${path.basename(f)} нечитаем, сохранён как .corrupt-*`);
    }
    return [];
}

// Запись: во временный файл, fsync, затем атомарное переименование.
// Если сервер остановят посреди записи, основной файл останется целым.
const lastBak = {};
function write(f, d) {
    const tmp = f + '.tmp';
    const fd = fs.openSync(tmp, 'w');
    fs.writeSync(fd, JSON.stringify(d, null, 2));
    fs.fsyncSync(fd);
    fs.closeSync(fd);
    if (fs.existsSync(f) && Date.now() - (lastBak[f] || 0) > 60000) { // резервная копия не чаще раза в минуту
        fs.copyFileSync(f, f + '.bak');
        lastBak[f] = Date.now();
    }
    fs.renameSync(tmp, f);
}

// Отдаём только страницу и загруженные файлы (но не папку data целиком)
app.get('/', (req, res) => res.sendFile(path.join(__dirname, 'index.html')));
app.use('/uploads', express.static(UP, {
    maxAge: '30d', immutable: true,
    setHeaders: r => r.setHeader('X-Content-Type-Options', 'nosniff')
}));

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

// Миграция старых данных
(function migrate() {
    const users = read(F.users);
    users.forEach(u => { if (!u.handle) u.handle = 'user' + String(u.id).slice(-6); });
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
})();

// Метка «когда создана база». Если после каждого перезапуска она новая — хостинг стирает файлы.
let meta;
try { meta = JSON.parse(fs.readFileSync(path.join(DIR, 'meta.json'), 'utf8')); }
catch {
    meta = { createdAt: new Date().toISOString() };
    fs.writeFileSync(path.join(DIR, 'meta.json'), JSON.stringify(meta));
}
console.log(`💾 Папка данных: ${DIR}`);
console.log(`👤 Пользователей: ${read(F.users).length}, база создана: ${meta.createdAt}`);

app.get('/api/health', (req, res) => res.json({ ok: true, users: read(F.users).length, dbCreatedAt: meta.createdAt }));

// ---------- Онлайн ----------
const sockets = new Map(); // userId -> Set<ws>

const pub = u => ({
    id: u.id, username: u.username, handle: u.handle, bio: u.bio || '',
    avatar: u.avatar || '', createdAt: u.createdAt, online: sockets.has(u.id)
});
const self = u => ({ ...pub(u), email: u.email });

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
    const byId = id => users.find(u => u.id === id);
    const reads = chat.reads || {};
    const o = {
        id: chat.id, type: chat.type, name: chat.name, avatar: chat.avatar || '',
        creator: chat.creator, createdAt: chat.createdAt, reads,
        last: list[list.length - 1] || null,
        unread: list.filter(m => m.senderId !== me && m.timestamp > (reads[me] || '')).length
    };
    if (chat.type === 'direct') {
        const p = byId(chat.members.find(i => i !== me));
        o.peer = p ? pub(p) : null;
    } else {
        o.members = chat.members.map(byId).filter(Boolean).map(pub);
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
    await fs.promises.writeFile(path.join(UP, name), buf);
    res.json({ url: '/uploads/' + name, kind: isImg ? 'image' : 'video' });
});

// ---------- Авторизация ----------
app.post('/api/register', (req, res) => {
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
        bio: '', avatar: '', createdAt: new Date().toISOString()
    };
    users.push(user);
    write(F.users, users);
    res.json({ success: true, user: self(user) });
});

app.post('/api/login', (req, res) => {
    const login = String(req.body.email || '').trim().toLowerCase().replace(/^@/, '');
    const password = String(req.body.password || '');
    const users = read(F.users);
    const user = users.find(u => ((u.email || '').toLowerCase() === login || u.handle === login) && checkPass(password, u.password));
    if (!user) return res.status(401).json({ error: 'Неверный логин или пароль' });
    if (!isHash(user.password)) { user.password = hashPass(password); write(F.users, users); }
    res.json({ success: true, user: self(user) });
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
    const me = Number(req.params.userId);
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

app.get('/api/messages/:chatId', (req, res) => {
    const me = Number(req.query.userId);
    const chat = read(F.chats).find(c => String(c.id) === req.params.chatId);
    if (!chat || !chat.members.includes(me)) return res.status(403).json({ error: 'Нет доступа' });
    res.json(read(F.messages).filter(m => m.chatId === chat.id).slice(-500));
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
                uid = Number(m.userId);
                if (!sockets.has(uid)) sockets.set(uid, new Set());
                sockets.get(uid).add(ws);
                broadcast({ type: 'presence', data: { userId: uid, online: true } });
                return;
            }
            if (!uid) return;

            const chats = read(F.chats);
            const chat = chats.find(c => String(c.id) === String(m.chatId));
            if (!chat || !chat.members.includes(uid)) return;

            if (m.type === 'message') {
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
                sendTo(chat.members, { type: 'message', data: msg }); // только участникам чата

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

process.on('uncaughtException', e => console.error('Необработанная ошибка:', e));
process.on('unhandledRejection', e => console.error('Необработанный промис:', e));

const PORT = process.env.PORT || 3000;
server.listen(PORT, () => console.log(`🚀 OVKGRAMM запущен: http://localhost:${PORT}`));
