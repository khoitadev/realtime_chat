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
  const socketRef = useRef<Socket | null>(null);
  const activeRoomIdRef = useRef<string | null>(null);

  useEffect(() => {
    activeRoomIdRef.current = activeRoomId;
  }, [activeRoomId]);

  useEffect(() => {
    let cancelled = false;
    let socket: Socket;

    (async () => {
      const res = await fetch('/api/ws-ticket');
      const { ticket } = await res.json();
      if (cancelled) return;
      socket = io(process.env.NEXT_PUBLIC_NEST_URL ?? 'http://localhost:3000', {
        auth: { ticket },
      });
      socket.on('message:new', (message: Message) => {
        if (message.roomId === activeRoomIdRef.current) {
          setMessages((prev) => [...prev, message]);
        }
      });
      socketRef.current = socket;
    })();

    return () => {
      cancelled = true;
      socket?.disconnect();
    };
  }, []);

  async function selectRoom(roomId: string) {
    setActiveRoomId(roomId);
    socketRef.current?.emit('room:subscribe', { roomId });

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
    const room: Room = await res.json();
    setRooms((prev) => [...prev, room]);
    setNewRoomName('');
  }

  async function joinRoom() {
    await fetch(`/api/rooms/${joinRoomId}/join`, { method: 'POST' });
    const res = await fetch('/api/rooms');
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
