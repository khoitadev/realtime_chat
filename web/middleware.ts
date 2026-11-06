import { NextRequest, NextResponse } from 'next/server';
import { GATEWAY_URL } from './lib/gateway';

// Protects routes that render via a Server Component reading cookies()
// directly (e.g. app/chat/page.tsx). Server Components cannot write cookies
// during render, so they cannot refresh an expired access token themselves
// — a reload past the 15-minute access-token lifetime would otherwise bounce
// the user to /login even though their refresh_token is still valid.
// Middleware runs before that render and CAN write cookies, so it refreshes
// here and forwards the new token into the same request.
export const config = {
  matcher: ['/chat/:path*'],
};

function isExpired(accessToken: string | undefined): boolean {
  if (!accessToken) return true;
  const payload = accessToken.split('.')[1];
  if (!payload) return true;
  try {
    const json = atob(payload.replace(/-/g, '+').replace(/_/g, '/'));
    const { exp } = JSON.parse(json) as { exp?: number };
    return typeof exp !== 'number' || exp * 1000 <= Date.now();
  } catch {
    return true;
  }
}

export async function middleware(request: NextRequest) {
  const accessToken = request.cookies.get('access_token')?.value;
  const refreshToken = request.cookies.get('refresh_token')?.value;

  if (!isExpired(accessToken)) {
    return NextResponse.next();
  }

  // No access token and nothing to refresh with — let the page's own check
  // redirect to /login as before.
  if (!refreshToken) {
    return NextResponse.next();
  }

  const refreshRes = await fetch(`${GATEWAY_URL}/auth/refresh`, {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({ refreshToken }),
  });

  // Refresh token itself invalid/expired/revoked — let the page redirect.
  if (!refreshRes.ok) {
    return NextResponse.next();
  }

  const tokens = await refreshRes.json();

  // Forward the refreshed token into THIS request so the Server Component
  // render that follows sees it via cookies().get(...).
  request.cookies.set('access_token', tokens.accessToken);
  request.cookies.set('refresh_token', tokens.refreshToken);

  const response = NextResponse.next({ request });

  // Persist it back to the browser for future requests.
  response.cookies.set('access_token', tokens.accessToken, { httpOnly: true, path: '/' });
  response.cookies.set('refresh_token', tokens.refreshToken, { httpOnly: true, path: '/' });

  return response;
}
