import { NextRequest, NextResponse } from 'next/server';
import { nestFetch } from '../../../lib/nest-fetch';

export async function GET() {
  const nestResponse = await nestFetch('/rooms');
  return NextResponse.json(await nestResponse.json(), { status: nestResponse.status });
}

export async function POST(request: NextRequest) {
  const body = await request.json();
  const nestResponse = await nestFetch('/rooms', {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify(body),
  });
  return NextResponse.json(await nestResponse.json(), { status: nestResponse.status });
}
