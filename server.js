const express = require('express');
const http = require('http');
const WebSocket = require('ws');
const fs = require('fs');
const path = require('path');
const cors = require('cors');

const app = express();
const server = http.createServer(app);
const wss = new WebSocket.Server({ server });

// Middleware
app.use(cors());
app.use(express.json());
app.use(express.static(__dirname));

// Database files
const usersFile = path.join(__dirname, 'data/users.json');
const messagesFile = path.join(__dirname, 'data/messages.json');
const chatsFile = path.join(__dirname, 'data/chats.json');

// Ensure data directory exists
if (!fs.existsSync(path.join(__dirname, 'data'))) {
    fs.mkdirSync(path.join(__dirname, 'data'));
}

// Initialize data files
function initData() {
    if (!fs.existsSync(usersFile)) {
        fs.writeFileSync(usersFile, JSON.stringify([]));
    }
    if (!fs.existsSync(messagesFile)) {
        fs.writeFileSync(messagesFile, JSON.stringify([]));
    }
    if (!fs.existsSync(chatsFile)) {
        fs.writeFileSync(chatsFile, JSON.stringify([]));
    }
}

// Read/Write helpers
function readUsers() {
    try {
        return JSON.parse(fs.readFileSync(usersFile, 'utf8'));
    } catch {
        return [];
    }
}

function writeUsers(users) {
    fs.writeFileSync(usersFile, JSON.stringify(users, null, 2));
}

function readMessages() {
    try {
        return JSON.parse(fs.readFileSync(messagesFile, 'utf8'));
    } catch {
        return [];
    }
}

function writeMessages(messages) {
    fs.writeFileSync(messagesFile, JSON.stringify(messages, null, 2));
}

function readChats() {
    try {
        return JSON.parse(fs.readFileSync(chatsFile, 'utf8'));
    } catch {
        return [];
    }
}

function writeChats(chats) {
    fs.writeFileSync(chatsFile, JSON.stringify(chats, null, 2));
}

// Store connected users
const connectedUsers = new Map();

// Initialize
initData();

// REST API Routes

// Register
app.post('/api/register', (req, res) => {
    const { username, email, password } = req.body;

    if (!username || !email || !password) {
        return res.status(400).json({ error: 'All fields required' });
    }

    const users = readUsers();
    
    if (users.find(u => u.email === email)) {
        return res.status(400).json({ error: 'Email already exists' });
    }

    const newUser = {
        id: Date.now(),
        username,
        email,
        password, // In production, hash this!
        createdAt: new Date().toISOString()
    };

    users.push(newUser);
    writeUsers(users);

    res.json({ 
        success: true, 
        user: { 
            id: newUser.id, 
            username: newUser.username, 
            email: newUser.email 
        } 
    });
});

// Login
app.post('/api/login', (req, res) => {
    const { email, password } = req.body;
    const users = readUsers();
    
    const user = users.find(u => u.email === email && u.password === password);
    
    if (!user) {
        return res.status(401).json({ error: 'Invalid credentials' });
    }

    res.json({ 
        success: true, 
        user: { 
            id: user.id, 
            username: user.username, 
            email: user.email 
        } 
    });
});

// Get all users
app.get('/api/users', (req, res) => {
    const users = readUsers();
    const onlineUsers = Array.from(connectedUsers.values());
    
    res.json(users.map(u => ({
        id: u.id,
        username: u.username,
        email: u.email,
        status: onlineUsers.some(ou => ou.userId === u.id) ? 'online' : 'offline'
    })));
});

// Search users by username
app.get('/api/users/search/:query', (req, res) => {
    const { query } = req.params;
    const users = readUsers();
    const onlineUsers = Array.from(connectedUsers.values());
    
    const results = users.filter(u => 
        u.username.toLowerCase().includes(query.toLowerCase())
    ).map(u => ({
        id: u.id,
        username: u.username,
        email: u.email,
        status: onlineUsers.some(ou => ou.userId === u.id) ? 'online' : 'offline'
    }));
    
    res.json(results);
});

// Get user profile
app.get('/api/profile/:userId', (req, res) => {
    const { userId } = req.params;
    const users = readUsers();
    const onlineUsers = Array.from(connectedUsers.values());
    
    const user = users.find(u => u.id === parseInt(userId));
    
    if (!user) {
        return res.status(404).json({ error: 'User not found' });
    }
    
    res.json({
        id: user.id,
        username: user.username,
        email: user.email,
        createdAt: user.createdAt,
        status: onlineUsers.some(ou => ou.userId === user.id) ? 'online' : 'offline'
    });
});

// Get chats for user
app.get('/api/chats/:userId', (req, res) => {
    const { userId } = req.params;
    const messages = readMessages();
    const chats = readChats();
    
    const userChats = chats.filter(c => c.participants.includes(parseInt(userId)));
    
    const chatsWithMessages = userChats.map(chat => {
        const chatMessages = messages.filter(m => m.chatId === chat.id);
        return {
            ...chat,
            lastMessage: chatMessages[chatMessages.length - 1] || null,
            messageCount: chatMessages.length
        };
    });

    res.json(chatsWithMessages);
});

// Get messages for chat
app.get('/api/messages/:chatId', (req, res) => {
    const { chatId } = req.params;
    const messages = readMessages();
    
    const chatMessages = messages.filter(m => m.chatId === parseInt(chatId));
    res.json(chatMessages);
});

// Create or get chat
app.post('/api/chats', (req, res) => {
    const { userId1, userId2 } = req.body;
    const chats = readChats();
    const users = readUsers();
    
    // Find existing chat
    let chat = chats.find(c => 
        (c.participants.includes(userId1) && c.participants.includes(userId2))
    );

    if (!chat) {
        // Create new chat
        const user1 = users.find(u => u.id === userId1);
        const user2 = users.find(u => u.id === userId2);
        
        chat = {
            id: Date.now(),
            participants: [userId1, userId2],
            participantNames: [user1?.username, user2?.username],
            createdAt: new Date().toISOString()
        };
        
        chats.push(chat);
        writeChats(chats);
    }

    res.json(chat);
});

// WebSocket handling
wss.on('connection', (ws) => {
    let userId = null;

    ws.on('message', (data) => {
        try {
            const message = JSON.parse(data);

            switch (message.type) {
                case 'connect':
                    userId = message.userId;
                    connectedUsers.set(ws, {
                        userId,
                        username: message.username,
                        ws
                    });
                    broadcastUserStatus();
                    break;

                case 'message':
                    handleMessage(message);
                    break;

                case 'typing':
                    broadcastTyping(message);
                    break;
            }
        } catch (err) {
            console.error('Error:', err);
        }
    });

    ws.on('close', () => {
        if (userId) {
            connectedUsers.delete(ws);
            broadcastUserStatus();
        }
    });
});

function handleMessage(messageData) {
    const { chatId, senderId, text } = messageData;
    
    const message = {
        id: Date.now(),
        chatId,
        senderId,
        text,
        timestamp: new Date().toISOString()
    };

    // Save to file
    const messages = readMessages();
    messages.push(message);
    writeMessages(messages);

    // Broadcast to all connected users
    const messageJson = JSON.stringify({
        type: 'message',
        data: message
    });

    wss.clients.forEach(client => {
        if (client.readyState === WebSocket.OPEN) {
            client.send(messageJson);
        }
    });
}

function broadcastTyping(typingData) {
    const json = JSON.stringify({
        type: 'typing',
        data: typingData
    });

    wss.clients.forEach(client => {
        if (client.readyState === WebSocket.OPEN) {
            client.send(json);
        }
    });
}

function broadcastUserStatus() {
    const onlineUsers = Array.from(connectedUsers.values()).map(u => ({
        userId: u.userId,
        username: u.username
    }));

    const json = JSON.stringify({
        type: 'userStatus',
        data: onlineUsers
    });

    wss.clients.forEach(client => {
        if (client.readyState === WebSocket.OPEN) {
            client.send(json);
        }
    });
}

// Start server
const PORT = process.env.PORT || 3000;
server.listen(PORT, () => {
    console.log(`🚀 OVKGRAMM сервер запущен на http://localhost:${PORT}`);
    console.log(`📱 Открой http://localhost:${PORT} в браузере`);
});
