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
// Раздаём только страницу, а не всю папку: раньше data/users.json был доступен по ссылке
app.get('/', (req, res) => res.sendFile(path.join(__dirname, 'index.html')));

// ---------- Хранилище ----------
const DIR = path.join(__dirname, 'data');
if (!fs.existsSync(DIR)) fs.mkdirSync(DIR);
const F = {
    users: path.join(DIR, 'users.json'),
    messages: path.join(DIR, 'messages.json'),
    chats: path.join(DIR, 'chats.json')
};
const read = f => { try { return JSON.parse(fs.readFileSync(f, 'utf8')); } catch { return []; } };
const write = (f, d) => fs.writeFileSync(f, JSON.stringify(d, null, 2));

const directId = (a, b) => `d_${Math.min(a, b)}_${Math.max(a, b)}`;
const HANDLE_RE = /^[a-z0-9_]{3,20}$/;

// Пароли: scrypt. Старые пароли (открытым текстом) принимаются и сразу заменяются на хэш
const hashPass = (p, salt = crypto.randomBytes(8).toString('hex')) =>
    `${salt}:${crypto.scryptSync(p, salt, 32).toString('hex')}`;
const isHash = s => /^[0-9a-f]{16}:[0-9a-f]{64}$/.test(s);
const checkPass = (p, stored) => isHash(stored) ? hashPass(p, stored.split(':')[0]) === stored : p === stored;

// Миграция старых данных: юзернеймы, единый формат чатов и сообщений
(function migrate() {
    const users = read(F.users);
    users.forEach(u => { if (!u.handle) u.handle = 'user' + String(u.id).slice(-6); });
    write(F.users, users);

    let chats = read(F.chats).map(c => c.type ? c :
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
    write(F.messages, msgs);
    write(F.chats, chats);
})();

// ---------- Онлайн ----------
const sockets = new Map(); // userId -> Set<ws>

const pub = u => ({
    id: u.id, username: u.username, handle: u.handle, bio: u.bio || '',
    createdAt: u.createdAt, online: sockets.has(u.id)
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

// Чат в виде, удобном клиенту
function shape(chat, me, users, msgs) {
    const byId = id => users.find(u => u.id === id);
    let last = null;
    for (let i = msgs.length - 1; i >= 0; i--) if (msgs[i].chatId === chat.id) { last = msgs[i]; break; }
    const o = { id: chat.id, type: chat.type, name: chat.name, creator: chat.creator, createdAt: chat.createdAt, last };
    if (chat.type === 'direct') {
        const p = byId(chat.members.find(i => i !== me));
        o.peer = p ? pub(p) : null;
    } else {
        o.members = chat.members.map(byId).filter(Boolean).map(pub);
    }
    return o;
}

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
        bio: '', createdAt: new Date().toISOString()
    };
    users.push(user);
    write(F.users, users);
    res.json({ success: true, user: self(user) });
});

app.post('/api/login', (req, res) => {
    const login = String(req.body.email || '').trim().toLowerCase().replace(/^@/, '');
    const password = String(req.body.password || '');
    const users = read(F.users);
    const user = users.find(u => (u.email === login || u.handle === login) && checkPass(password, u.password));
    if (!user) return res.status(401).json({ error: 'Неверный логин или пароль' });
    if (!isHash(user.password)) { user.password = hashPass(password); write(F.users, users); }
    res.json({ success: true, user: self(user) });
});

// ---------- Пользователи ----------
// Поиск по имени и @юзернейму; пустой запрос — первые 30 человек
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

    const { username, handle, bio } = req.body;
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

    write(F.users, users);
    broadcast({ type: 'chat' }); // у всех обновятся имена в списках
    res.json({ success: true, user: self(u) });
});

// ---------- Чаты ----------
app.get('/api/chats/:userId', (req, res) => {
    const me = Number(req.params.userId);
    const users = read(F.users);
    const msgs = read(F.messages);
    const ts = c => new Date(c.last ? c.last.timestamp : c.createdAt || 0).getTime();
    res.json(read(F.chats)
        .filter(c => c.members.includes(me))
        .map(c => shape(c, me, users, msgs))
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
        chat = { id: directId(me, peer), type: 'direct', members: [me, peer], createdAt: new Date().toISOString() };
        chats.push(chat);
        write(F.chats, chats);
    }
    res.json(shape(chat, me, users, read(F.messages)));
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
        id: 'g_' + Date.now(), type: 'group', name, creator: me,
        members: [me, ...others], createdAt: new Date().toISOString()
    };
    chats.push(group);
    write(F.chats, chats);
    sendTo(others, { type: 'chat' });
    res.json(shape(group, me, users, []));
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

            const chat = read(F.chats).find(c => String(c.id) === String(m.chatId));
            if (!chat || !chat.members.includes(uid)) return;

            if (m.type === 'message') {
                const text = String(m.text || '').trim().slice(0, 4000);
                if (!text) return;
                const msg = {
                    id: crypto.randomUUID(), chatId: chat.id, senderId: uid,
                    text, timestamp: new Date().toISOString()
                };
                const msgs = read(F.messages);
                msgs.push(msg);
                write(F.messages, msgs);
                sendTo(chat.members, { type: 'message', data: msg }); // только участникам чата
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

const PORT = process.env.PORT || 3000;
server.listen(PORT, () => {
    console.log(`🚀 OVKGRAMM запущен: http://localhost:${PORT}`);
});
