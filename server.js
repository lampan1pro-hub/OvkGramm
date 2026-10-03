const express = require('express');
const http = require('http');
const WebSocket = require('ws');
const fs = require('fs');
const path = require('path');
const cors = require('cors');
const crypto = require('crypto');
const { createClient } = require('@supabase/supabase-js');

const app = express();
const server = http.createServer(app);
const wss = new WebSocket.Server({ server });

app.use(cors());
app.use(express.json({ limit: '1mb' }));

// ========== SUPABASE SETUP ==========
const SUPABASE_URL = process.env.SUPABASE_URL || 'https://your-project.supabase.co';
const SUPABASE_KEY = process.env.SUPABASE_KEY || 'your-anon-key';

let supabase = null;

try {
    supabase = createClient(SUPABASE_URL, SUPABASE_KEY);
    console.log('✅ Supabase подключен');
} catch (e) {
    console.error('❌ Ошибка подключения к Supabase:', e.message);
    process.exit(1);
}

// ========== ФАЙЛЫ (ТОЛЬКО ДЛЯ ЗАГРУЗОК) ==========
const DIR = process.env.DATA_DIR ? path.resolve(process.env.DATA_DIR) : path.join(__dirname, 'uploads');
const UP = path.join(DIR, 'files');
fs.mkdirSync(UP, { recursive: true });

// ========== ФУНКЦИИ БЕЗОПАСНОСТИ ==========
const HANDLE_RE = /^[a-z0-9_]{3,20}$/;
const MIME = {
    'image/jpeg': 'jpg', 'image/png': 'png', 'image/webp': 'webp', 'image/gif': 'gif',
    'video/mp4': 'mp4', 'video/webm': 'webm', 'video/quicktime': 'mov'
};
const URL_RE = /^\/uploads\/[\w-]+\.(jpg|png|webp|gif|mp4|webm|mov)$/;
const fileOk = u => typeof u === 'string' && URL_RE.test(u) && fs.existsSync(path.join(UP, path.basename(u)));
const kindOf = u => /\.(mp4|webm|mov)$/.test(u) ? 'video' : 'image';
const imageOk = u => fileOk(u) && kindOf(u) === 'image';

// ========== ПАРОЛИ ==========
const hashPass = (p, salt = crypto.randomBytes(8).toString('hex')) =>
    `${salt}:${crypto.scryptSync(p, salt, 32).toString('hex')}`;
const isHash = s => /^[0-9a-f]{16}:[0-9a-f]{64}$/.test(s);
const checkPass = (p, stored) => isHash(stored) ? hashPass(p, stored.split(':')[0]) === stored : p === stored;

// ========== ФОРМАТИРОВАНИЕ ПОЛЬЗОВАТЕЛЯ ==========
const directId = (a, b) => `d_${Math.min(a, b)}_${Math.max(a, b)}`;

const pub = u => ({
    id: u.id, username: u.username, handle: u.handle, bio: u.bio || '',
    avatar: u.avatar || '', createdAt: u.created_at, online: sockets.has(u.id)
});
const self = u => ({ ...pub(u), email: u.email });

// ========== ОНЛАЙН СТАТУСЫ ==========
const sockets = new Map();

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

// ========== ФОРМАТИРОВАНИЕ ЧАТА ==========
function shape(chat, me, users, list) {
    const byId = id => users.find(u => u.id === id);
    const reads = chat.reads || {};
    const o = {
        id: chat.id, type: chat.type, name: chat.name, avatar: chat.avatar || '',
        creator: chat.creator, createdAt: chat.created_at, reads,
        last: list[list.length - 1] || null,
        unread: list.filter(m => m.sender_id !== me && m.timestamp > (reads[me] || '')).length
    };
    if (chat.type === 'direct') {
        const p = byId(chat.members.find(i => i !== me));
        o.peer = p ? pub(p) : null;
    } else {
        o.members = chat.members.map(byId).filter(Boolean).map(pub);
    }
    return o;
}

// ========== REST API ==========

app.get('/', (req, res) => res.sendFile(path.join(__dirname, 'index.html')));
app.use('/uploads', express.static(UP, {
    maxAge: '30d', immutable: true,
    setHeaders: r => r.setHeader('X-Content-Type-Options', 'nosniff')
}));

app.get('/api/health', async (req, res) => {
    const { count } = await supabase.from('users').select('id', { count: 'exact' });
    res.json({ ok: true, users: count || 0 });
});

// ========== ФАЙЛЫ ==========
app.post('/api/upload', express.raw({ type: () => true, limit: '50mb' }), async (req, res) => {
    const uid = Number(req.query.userId);
    if (!uid) return res.status(400).json({ error: 'User ID required' });
    
    const mime = req.get('content-type');
    if (!MIME[mime]) return res.status(415).json({ error: 'Unsupported media type' });
    
    const filename = `${crypto.randomUUID()}.${MIME[mime]}`;
    const filepath = path.join(UP, filename);
    
    try {
        fs.writeFileSync(filepath, req.body);
        res.json({ success: true, url: `/uploads/${filename}` });
    } catch (e) {
        res.status(500).json({ error: 'Upload failed' });
    }
});

// ========== РЕГИСТРАЦИЯ И ВХОД ==========
app.post('/api/register', async (req, res) => {
    const { email, password, username, handle } = req.body;
    
    if (!email || !password || !username) {
        return res.status(400).json({ error: 'Email, password and username required' });
    }
    
    const h = String(handle || 'user' + crypto.randomBytes(3).toString('hex')).toLowerCase();
    if (!HANDLE_RE.test(h)) return res.status(400).json({ error: 'Invalid handle' });
    
    const { data: existing } = await supabase.from('users').select('id').eq('email', email).limit(1);
    if (existing?.length) return res.status(400).json({ error: 'Email already exists' });
    
    const { data: existingHandle } = await supabase.from('users').select('id').eq('handle', h).limit(1);
    if (existingHandle?.length) return res.status(400).json({ error: 'Handle taken' });
    
    const userId = Date.now();
    const { data, error } = await supabase.from('users').insert([{
        id: userId,
        email,
        username,
        handle: h,
        password: hashPass(password),
        bio: '',
        avatar: ''
    }]).select().single();
    
    if (error) return res.status(400).json({ error: error.message });
    res.json({ success: true, user: self(data) });
});

app.post('/api/login', async (req, res) => {
    const login = String(req.body.email || '').trim().toLowerCase().replace(/^@/, '');
    const password = String(req.body.password || '');
    
    const { data: users } = await supabase.from('users').select('*');
    const user = users?.find(u => ((u.email || '').toLowerCase() === login || u.handle === login) && checkPass(password, u.password));
    
    if (!user) return res.status(401).json({ error: 'Invalid login or password' });
    
    if (!isHash(user.password)) {
        await supabase.from('users').update({ password: hashPass(password) }).eq('id', user.id);
    }
    
    res.json({ success: true, user: self(user) });
});

// ========== ПОЛЬЗОВАТЕЛИ ==========
app.get('/api/users/search', async (req, res) => {
    const s = String(req.query.q || '').toLowerCase().replace(/^@/, '');
    const me = Number(req.query.userId);
    
    const { data: users } = await supabase.from('users').select('*');
    res.json(users
        .filter(u => u.id !== me && (u.handle.includes(s) || u.username.toLowerCase().includes(s)))
        .slice(0, 30).map(pub));
});

app.get('/api/profile/:id', async (req, res) => {
    const { data: user } = await supabase.from('users').select('*').eq('id', Number(req.params.id)).single();
    if (!user) return res.status(404).json({ error: 'User not found' });
    res.json(pub(user));
});

app.put('/api/profile/:id', async (req, res) => {
    const userId = Number(req.params.id);
    const { username, handle, bio, avatar } = req.body;
    
    const { data: user } = await supabase.from('users').select('*').eq('id', userId).single();
    if (!user) return res.status(404).json({ error: 'User not found' });
    
    const updates = {};
    
    if (username !== undefined) {
        const n = String(username).trim().slice(0, 40);
        if (!n) return res.status(400).json({ error: 'Name cannot be empty' });
        updates.username = n;
    }
    
    if (handle !== undefined) {
        const h = String(handle).trim().replace(/^@/, '').toLowerCase();
        if (!HANDLE_RE.test(h)) return res.status(400).json({ error: 'Invalid handle' });
        const { data: existing } = await supabase.from('users').select('id').eq('handle', h).neq('id', userId);
        if (existing?.length) return res.status(400).json({ error: 'Handle taken' });
        updates.handle = h;
    }
    
    if (bio !== undefined) updates.bio = String(bio).slice(0, 200);
    if (avatar !== undefined) {
        if (avatar !== '' && !imageOk(avatar)) return res.status(400).json({ error: 'Invalid image' });
        updates.avatar = avatar;
    }
    
    await supabase.from('users').update(updates).eq('id', userId);
    broadcast({ type: 'chat' });
    
    const { data: updated } = await supabase.from('users').select('*').eq('id', userId).single();
    res.json({ success: true, user: self(updated) });
});

// ========== ЧАТЫ ==========
app.get('/api/chats/:userId', async (req, res) => {
    const me = Number(req.params.userId);
    
    const { data: users } = await supabase.from('users').select('*');
    const { data: messages } = await supabase.from('messages').select('*');
    const { data: chats } = await supabase.from('chats').select('*');
    
    const grouped = new Map();
    messages?.forEach(m => {
        if (!grouped.has(m.chat_id)) grouped.set(m.chat_id, []);
        grouped.get(m.chat_id).push(m);
    });
    
    const ts = c => new Date(c.last ? c.last.timestamp : c.created_at || 0).getTime();
    res.json(chats
        .filter(c => c.members.includes(me))
        .map(c => shape(c, me, users, grouped.get(c.id) || []))
        .sort((a, b) => ts(b) - ts(a)));
});

app.post('/api/chats', async (req, res) => {
    const { userId1, userId2 } = req.body;
    const id = directId(userId1, userId2);
    
    const { data: existing } = await supabase.from('chats').select('*').eq('id', id).single();
    if (existing) return res.json(existing);
    
    const newChat = {
        id, type: 'direct', members: [userId1, userId2], created_at: new Date().toISOString()
    };
    
    await supabase.from('chats').insert([newChat]);
    res.json(newChat);
});

// ========== ГРУППЫ ==========
app.get('/api/groups/:userId', async (req, res) => {
    const me = Number(req.params.userId);
    const { data: groups } = await supabase.from('chats').select('*').eq('type', 'group');
    res.json(groups?.filter(g => g.members.includes(me)) || []);
});

app.post('/api/groups', async (req, res) => {
    const { userId, groupName, members } = req.body;
    
    if (!groupName || !Array.isArray(members)) {
        return res.status(400).json({ error: 'Invalid' });
    }
    
    const newGroup = {
        id: crypto.randomUUID(),
        type: 'group',
        name: groupName,
        creator: userId,
        members: [userId, ...members],
        created_at: new Date().toISOString(),
        reads: {}
    };
    
    await supabase.from('chats').insert([newGroup]);
    res.json({ success: true, group: newGroup });
});

app.post('/api/groups/:id/members', async (req, res) => {
    const { userId, members } = req.body;
    const groupId = req.params.id;
    
    const { data: group } = await supabase.from('chats').select('*').eq('id', groupId).single();
    if (!group || group.creator !== userId) return res.status(403).json({ error: 'Forbidden' });
    
    const newMembers = [...new Set([...group.members, ...members])];
    await supabase.from('chats').update({ members: newMembers }).eq('id', groupId);
    
    res.json({ success: true });
});

// ========== NFT ==========
app.get('/api/nfts/:userId', async (req, res) => {
    const { data: nfts } = await supabase.from('nfts').select('*').eq('owner_id', Number(req.params.userId));
    res.json(nfts || []);
});

app.post('/api/nfts/send', async (req, res) => {
    const { senderId, receiverId } = req.body;
    if (!senderId || !receiverId) return res.status(400).json({ error: 'Invalid' });
    
    const types = ['🎮', '⭐', '🚀', '👑', '💎', '🏆'];
    const nft = {
        owner_id: receiverId,
        sender_id: senderId,
        type: types[Math.floor(Math.random() * types.length)],
        sent_at: new Date().toISOString()
    };
    
    await supabase.from('nfts').insert([nft]);
    res.json({ success: true, nft });
});

// ========== ВРЕМЯ СЕССИИ ==========
app.get('/api/sessions/:userId', async (req, res) => {
    const { data } = await supabase.from('sessions').select('session_time').eq('user_id', Number(req.params.userId)).single();
    res.json({ time: data?.session_time || 0 });
});

app.post('/api/sessions/update', async (req, res) => {
    const { userId, sessionTime } = req.body;
    if (!userId) return res.status(400).json({ error: 'Invalid' });
    
    const { data: existing } = await supabase.from('sessions').select('*').eq('user_id', userId).single();
    
    if (existing) {
        await supabase.from('sessions').update({ session_time: sessionTime, updated_at: new Date().toISOString() }).eq('user_id', userId);
    } else {
        await supabase.from('sessions').insert([{ user_id: userId, session_time: sessionTime }]);
    }
    
    res.json({ success: true });
});

// ========== WEBSOCKET ==========
wss.on('connection', (ws) => {
    ws.isAlive = true;
    ws.on('pong', () => { ws.isAlive = true; });
    
    let uid = null;
    
    ws.on('message', async (data) => {
        try {
            const m = JSON.parse(data);
            
            if (m.type === 'connect') {
                uid = m.userId;
                if (!sockets.has(uid)) sockets.set(uid, new Set());
                sockets.get(uid).add(ws);
                broadcast({ type: 'presence', data: { userId: uid, online: true } });
                
            } else if (m.type === 'message' && uid) {
                const { chatId, text, media } = m;
                const msg = {
                    chat_id: chatId,
                    sender_id: uid,
                    text,
                    ...(media && { media }),
                    timestamp: new Date().toISOString()
                };
                
                await supabase.from('messages').insert([msg]);
                
                const { data: chat } = await supabase.from('chats').select('members').eq('id', chatId).single();
                sendTo(chat?.members || [], { type: 'message', data: msg });
                
            } else if (m.type === 'read' && uid) {
                const { chatId } = m;
                const { data: messages } = await supabase.from('messages').select('timestamp').eq('chat_id', chatId);
                const last = messages?.[messages.length - 1];
                const at = last?.timestamp || '';
                
                const { data: chat } = await supabase.from('chats').select('reads').eq('id', chatId).single();
                const reads = chat?.reads || {};
                if (!at || (reads[uid] || '') >= at) return;
                
                reads[uid] = at;
                await supabase.from('chats').update({ reads }).eq('id', chatId);
                sendTo([uid], { type: 'read', data: { chatId, userId: uid, at } });
                
            } else if (m.type === 'typing' && uid) {
                const { chatId } = m;
                const { data: chat } = await supabase.from('chats').select('members').eq('id', chatId).single();
                sendTo(chat?.members || [], { type: 'typing', data: { chatId, userId: uid } }, ws);
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

// ========== ОШИБКИ ==========
app.use((err, req, res, next) => {
    const big = err.type === 'entity.too.large';
    res.status(big ? 413 : err.status || 500).json({ error: big ? 'File too large' : 'Server error' });
});

// ========== PING ==========
setInterval(() => wss.clients.forEach(ws => {
    if (!ws.isAlive) return ws.terminate();
    ws.isAlive = false;
    ws.ping();
}), 25000);

process.on('uncaughtException', e => console.error('Uncaught:', e));
process.on('unhandledRejection', e => console.error('Unhandled:', e));

const PORT = process.env.PORT || 3000;
server.listen(PORT, () => console.log(`🚀 SAIMONGRAM запущен: http://localhost:${PORT}`));
