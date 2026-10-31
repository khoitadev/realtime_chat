import { cookies } from 'next/headers';
import { redirect } from 'next/navigation';
import { NEST_URL } from '../../lib/nest';
import { ChatClient } from './chat-client';

interface Room {
  id: string;
  name: string;
}

export default async function ChatPage() {
  const accessToken = cookies().get('access_token')?.value;
  if (!accessToken) {
    redirect('/login');
  }

  const res = await fetch(`${NEST_URL}/rooms`, {
    headers: { Authorization: `Bearer ${accessToken}` },
    cache: 'no-store',
  });

  if (!res.ok) {
    redirect('/login');
  }

  const rooms: Room[] = await res.json();

  return <ChatClient initialRooms={rooms} />;
}
