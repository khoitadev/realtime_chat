import { NextRequest, NextResponse } from 'next/server';
import { nestFetch } from '../../../../../lib/nest-fetch';

export async function GET(request: NextRequest, { params }: { params: { roomId: string } }) {
  const search = request.nextUrl.search; // forwards ?cursor&limit as-is
  const nestResponse = await nestFetch(`/rooms/${params.roomId}/messages${search}`);
  return NextResponse.json(await nestResponse.json(), { status: nestResponse.status });
}
