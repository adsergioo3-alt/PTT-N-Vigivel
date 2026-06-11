// npm install ws express cors
const WebSocket = require('ws');
const http = require('http');
const express = require('express');
const path = require('path');

const app = express();
app.use(express.static(path.join(__dirname, 'public')));
const server = http.createServer(app);
const wss = new WebSocket.Server({ server });

// roomName -> Map(ws -> {name, peerId})
const rooms = new Map();
let lastLocationUpdate = null;

function getStatus() {
    let clients = 0;
    const roomDetails = [];
    const userLocations = [];
    for (const [roomName, room] of rooms.entries()) {
        clients += room.size;
        roomDetails.push({ room: roomName, users: room.size });
        for (const userData of room.values()) {
            if (userData && typeof userData.lat === 'number' && typeof userData.lng === 'number') {
                userLocations.push({
                    name: userData.name,
                    room: roomName,
                    lat: userData.lat,
                    lng: userData.lng,
                });
            }
        }
    }
    return {
        status: 'online',
        timestamp: new Date().toISOString(),
        rooms: rooms.size,
        clients,
        roomDetails,
        userLocations,
        lastLocationUpdate,
    };
}

app.get('/', (req, res) => {
    res.sendFile(path.join(__dirname, 'public', 'index.html'));
});

app.get('/map', (req, res) => {
    res.sendFile(path.join(__dirname, 'public', 'map.html'));
});

app.get('/client', (req, res) => {
    res.sendFile(path.join(__dirname, 'public', 'client.html'));
});

// Rota leve que retorna somente as localizações (útil para páginas que atualizam só os pontos)
app.get('/locations', (req, res) => {
    const status = getStatus();
    res.json({ userLocations: status.userLocations || [], lastLocationUpdate: status.lastLocationUpdate || null });
});

app.get('/status', (req, res) => {
    res.json(getStatus());
});

app.post('/clear-rooms', (req, res) => {
    clearAllRooms();
    res.json({ status: 'rooms_cleared' });
});

wss.on('connection', (ws) => {
    console.log('Dispositivo conectado');

    ws.on('message', (message) => {
        try {
            // CONVERSÃO VITAL: Transforma o Buffer recebido em String de texto
            const msgText = message.toString();

            // Agora sim tentamos o JSON.parse
            const data = JSON.parse(msgText);
            console.log('[WS] mensagem recebida:', data.type || 'sem tipo', msgText);

            // inscrição de viewers de mapa (páginas /map)
            if (data.type === 'map_subscribe') {
                ws.isMapViewer = true;
                // envia estado inicial
                const status = getStatus();
                try {
                    ws.send(JSON.stringify({ type: 'locations', userLocations: status.userLocations || [], lastLocationUpdate: status.lastLocationUpdate || null }));
                } catch (e) {}
                return;
            }

            if (data.type === 'register') {
                const { room, name, peerId } = data;
                ws.room = room;
                ws.userData = { name, peerId, isTalking: false };

                if (!rooms.has(room)) rooms.set(room, new Map());
                const roomMap = rooms.get(room);

                // Prevenção de logins duplicados: se já existir um cliente
                // com o mesmo `peerId` (preferido) ou `name`, encerramos a
                // conexão antiga e substituímos pela nova.
                let existingClient = null;
                for (const [client, udata] of roomMap.entries()) {
                    if (!udata) continue;
                    if ((peerId && udata.peerId === peerId) || (!peerId && udata.name === name)) {
                        existingClient = client;
                        break;
                    }
                }

                if (existingClient) {
                    try {
                        existingClient.send(JSON.stringify({ type: 'duplicate_login', reason: 'replaced_by_new_connection' }));
                        existingClient.close(4000, 'replaced_by_new_connection');
                    } catch (e) {
                        // ignora erros ao fechar a conexão antiga
                    }
                    roomMap.delete(existingClient);
                }

                roomMap.set(ws, ws.userData);

                console.log(`[Registro] ${name} entrou na sala ${room}`);
                broadcastPresence(room);
            }

            // Repasse de áudio
            if (data.type === 'audio') {
                if (ws.room) {
                    // Repassa exatamente a mesma string para os outros
                    broadcastToRoom(ws.room, msgText, ws);
                }
            }

            // Repasse de imagens
            if (data.type === 'image') {
                if (ws.room) {
                    console.log(`[Imagem] Recebida de: ${data.name} na sala ${ws.room}`);
                    // Repassa exatamente a mesma string para os outros usuários na sala
                    broadcastToRoom(ws.room, msgText, ws);
                }
            }

            // Chat textual (novo tipo `chat` esperado pelo app Android)
            if (data.type === 'chat') {
                if (ws.room) {
                    console.log(`[Chat] ${data.name}: ${data.message} na sala ${ws.room}`);
                    try {
                        const out = JSON.stringify({ type: 'chat', name: data.name, message: data.message });
                        broadcastToRoom(ws.room, out, ws);
                    } catch (e) { /* ignore */ }
                }
            }

            if (data.type === 'message') {
                if (ws.room) {
                    console.log(`[Mensagem] ${data.name}: ${data.text} na sala ${ws.room}`);
                    broadcastToRoom(ws.room, msgText, ws);
                }
            }

            if (data.type === 'location_update') {
                if (ws.userData) {
                    const lat = Number(data.lat);
                    const lng = Number(data.lng);
                    if (Number.isFinite(lat) && Number.isFinite(lng)) {
                        ws.userData.lat = lat;
                        ws.userData.lng = lng;
                        const logMessage = `[Localização] ${ws.userData.name} está em: ${lat}, ${lng} na sala ${ws.room || 'sem sala'}`;
                        console.log(logMessage);
                        lastLocationUpdate = {
                            timestamp: new Date().toISOString(),
                            name: ws.userData.name,
                            room: ws.room,
                            lat,
                            lng,
                            message: logMessage,
                        };
                        // envia atualização em tempo real para viewers de mapa
                        try {
                            const payload = JSON.stringify({ type: 'locations', userLocations: getStatus().userLocations, lastLocationUpdate });
                            broadcastToViewers(payload);
                        } catch (e) { /* ignore */ }
                    } else {
                        console.warn('[Localização] coordenadas inválidas recebidas:', data.lat, data.lng);
                    }
                } else {
                    console.warn('[Localização] recebido sem ws.userData', data);
                }
            }

            if (data.type === 'talking_state') {
                if (ws.room && ws.userData) {
                    ws.userData.isTalking = !!data.isTalking;
                    // Notifica só os outros membros da sala sobre quem está falando
                    broadcastTalkingState(ws.room, ws);
                }
            }
        } catch (e) { 
            console.error('Erro ao processar mensagem:', e.message); 
        }
    });

    ws.on('close', () => {
        if (ws.room && rooms.has(ws.room)) {
            rooms.get(ws.room).delete(ws);
            broadcastPresence(ws.room);
        }
    });
});

function broadcastPresence(roomName) {
    const room = rooms.get(roomName);
    if (!room) return;
    const users = Array.from(room.values());
    const msg = JSON.stringify({ type: 'presence', users });
    room.forEach((_, client) => {
        if (client.readyState === WebSocket.OPEN) client.send(msg);
    });
}

function broadcastTalkingState(roomName, senderWs) {
    const room = rooms.get(roomName);
    if (!room) return;
    const sender = room.get(senderWs) || senderWs.userData || {};
    const msg = JSON.stringify({ type: 'user_talking', name: sender.name, isTalking: sender.isTalking });
    room.forEach((userData, client) => {
        if (client !== senderWs && client.readyState === WebSocket.OPEN) client.send(msg);
    });
}

function broadcastToViewers(msgText) {
    wss.clients.forEach(client => {
        try {
            if (client.isMapViewer && client.readyState === WebSocket.OPEN) client.send(msgText);
        } catch (e) {}
    });
}

function broadcastToRoom(roomName, msgText, senderWs) {
    const room = rooms.get(roomName);
    if (!room) return;
    room.forEach((userData, client) => {
        if (client !== senderWs && client.readyState === WebSocket.OPEN) client.send(msgText);
    });
}

function clearAllRooms() {
    for (const [roomName, room] of rooms.entries()) {
        room.forEach((userData, client) => {
            if (client.readyState === WebSocket.OPEN) {
                client.send(JSON.stringify({ type: 'room_cleared', room: roomName }));
            }
            client.room = undefined;
            client.userData = undefined;
        });
        rooms.delete(roomName);
    }
}

const PORT = process.env.PORT || 4000;
server.listen(PORT, () => console.log(`Servidor PTT rodando na porta ${PORT}`));
