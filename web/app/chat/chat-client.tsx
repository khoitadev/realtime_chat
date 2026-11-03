'use client';
import { useEffect, useRef, useState } from 'react';
import { io, Socket } from 'socket.io-client';

interface Room {
  id: string;
  name: string;
}

interface Message {
  id: string;
  roomId: string;
  senderId: string;
  content: string;
  createdAt: string;
}

export function ChatClient({ initialRooms }: { initialRooms: Room[] }) {
  const [rooms, setRooms] = useState<Room[]>(initialRooms);
  const [activeRoomId, setActiveRoomId] = useState<string | null>(null);
  const [messages, setMessages] = useState<Message[]>([]);
  const [newRoomName, setNewRoomName] = useState('');
  const [joinRoomId, setJoinRoomId] = useState('');
  const [draft, setDraft] = useState('');
  const [onlineUserIds, setOnlineUserIds] = useState<string[]>([]);
  const socketRef = useRef<Socket | null>(null);
  const activeRoomIdRef = useRef<string | null>(null);

  useEffect(() => {
    activeRoomIdRef.current = activeRoomId;
  }, [activeRoomId]);

  useEffect(() => {
    // Unlike the previous "fetch ticket then create socket" shape, io(...) is
    // called synchronously here and the ticket is fetched fresh on every
    // connection attempt (initial connect AND reconnect) via the auth
    // callback below. There's no async gap before the socket exists, so a
    // fast unmount is handled entirely by calling socket.disconnect() in the
    // cleanup below, which is safe to call regardless of connection state.
    const socket = io(process.env.NEXT_PUBLIC_NEST_URL ?? 'http://localhost:3000', {
      transports: ['websocket'], // no sticky sessions needed behind a load balancer
      auth: (cb) => {
        fetch('/api/ws-ticket')
          .then((res) => res.json())
          .then(({ ticket }) => cb({ ticket }))
          .catch(() => cb({}));
      },
    });

    socket.on('message:new', (message: Message) => {
      if (message.roomId === activeRoomIdRef.current) {
        setMessages((prev) => [...prev, message]);
      }
    });

    socket.on('presence:update', ({ userId, online }: { userId: string; online: boolean }) => {
      setOnlineUserIds((prev) => {
        const without = prev.filter((id) => id !== userId);
        return online ? [...without, userId] : without;
      });
    });

    socket.on('connect', () => {
      const roomId = activeRoomIdRef.current;
      if (roomId) {
        socket.emit('room:subscribe', { roomId });
        void loadPresence(roomId); // deltas may have been missed while disconnected
      }
    });

    socketRef.current = socket;

    return () => {
      socket.disconnect();
    };
  }, []);

  async function loadPresence(roomId: string) {
    const res = await fetch(`/api/rooms/${roomId}/presence`);
    if (!res.ok) return;
    const { onlineUserIds: ids } = await res.json();
    if (roomId === activeRoomIdRef.current) setOnlineUserIds(ids);
  }

  async function selectRoom(roomId: string) {
    setActiveRoomId(roomId);
    activeRoomIdRef.current = roomId;
    socketRef.current?.emit('room:subscribe', { roomId });
    void loadPresence(roomId);

    const res = await fetch(`/api/rooms/${roomId}/messages`);
    const { items } = await res.json();
    setMessages(items);
  }

  async function createRoom() {
    const res = await fetch('/api/rooms', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ name: newRoomName }),
    });
    if (!res.ok) {
      console.error('Failed to create room', res.status);
      return;
    }
    const room: Room = await res.json();
    setRooms((prev) => [...prev, room]);
    setNewRoomName('');
  }

  async function joinRoom() {
    const joinRes = await fetch(`/api/rooms/${joinRoomId}/join`, { method: 'POST' });
    if (!joinRes.ok) {
      console.error('Failed to join room', joinRes.status);
      return;
    }
    const res = await fetch('/api/rooms');
    if (!res.ok) {
      console.error('Failed to list rooms', res.status);
      return;
    }
    setRooms(await res.json());
    setJoinRoomId('');
  }

  function sendMessage() {
    if (!activeRoomId || !draft.trim()) return;
    socketRef.current?.emit('message:send', { roomId: activeRoomId, content: draft }, () => {
      setDraft('');
    });
  }

  return (
    <div style={{ display: 'flex', gap: '2rem' }}>
      <aside>
        <h2>Rooms</h2>
        <ul>
          {rooms.map((room) => (
            <li key={room.id}>
              <button onClick={() => selectRoom(room.id)}>{room.name}</button>
            </li>
          ))}
        </ul>
        <input
          value={newRoomName}
          onChange={(e) => setNewRoomName(e.target.value)}
          placeholder="New room name"
        />
        <button onClick={createRoom}>Create room</button>
        <input
          value={joinRoomId}
          onChange={(e) => setJoinRoomId(e.target.value)}
          placeholder="Room id to join"
        />
        <button onClick={joinRoom}>Join room</button>
      </aside>
      <main>
        <ul>
          {messages.map((m) => (
            <li key={m.id}>{m.content}</li>
          ))}
        </ul>
        {activeRoomId && <p>Online now: {onlineUserIds.length}</p>}
        {activeRoomId && (
          <div>
            <input value={draft} onChange={(e) => setDraft(e.target.value)} />
            <button onClick={sendMessage}>Send</button>
          </div>
        )}
      </main>
    </div>
  );
}
