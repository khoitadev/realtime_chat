import { NextResponse } from 'next/server';
import { nestFetch } from '../../../../../lib/nest-fetch';

export async function GET(_request: Request, { params }: { params: { roomId: string } }) {
  const nestResponse = await nestFetch(`/rooms/${params.roomId}/presence`);
  return NextResponse.json(await nestResponse.json(), { status: nestResponse.status });
}
