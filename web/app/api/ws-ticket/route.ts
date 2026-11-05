import { NextResponse } from 'next/server';
import { nestFetch } from '../../../lib/nest-fetch';

export async function GET() {
  const nestResponse = await nestFetch('/ws-ticket', { method: 'POST' });
  if (!nestResponse.ok) {
    return NextResponse.json({ error: 'Not authenticated' }, { status: 401 });
  }
  return NextResponse.json(await nestResponse.json());
}
