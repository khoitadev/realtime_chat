import { NextRequest, NextResponse } from 'next/server';
import { NEST_URL } from '../../../lib/nest';

export async function POST(request: NextRequest) {
  const body = await request.json();
  const nestResponse = await fetch(`${NEST_URL}/auth/login`, {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify(body),
  });

  if (!nestResponse.ok) {
    const error = await nestResponse.json();
    return NextResponse.json(error, { status: nestResponse.status });
  }

  const { accessToken, refreshToken } = await nestResponse.json();
  const response = NextResponse.json({ success: true });
  response.cookies.set('access_token', accessToken, { httpOnly: true, path: '/' });
  response.cookies.set('refresh_token', refreshToken, { httpOnly: true, path: '/' });
  return response;
}
