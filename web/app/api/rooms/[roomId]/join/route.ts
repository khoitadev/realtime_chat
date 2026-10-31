import { NextResponse } from 'next/server';
import { nestFetch } from '../../../../../lib/nest-fetch';

export async function POST(_request: Request, { params }: { params: { roomId: string } }) {
  const nestResponse = await nestFetch(`/rooms/${params.roomId}/join`, { method: 'POST' });
  return NextResponse.json(await nestResponse.json(), { status: nestResponse.status });
}
