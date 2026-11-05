import { cookies } from 'next/headers';
import { NEST_URL } from './nest';
import { AUTH_URL } from './auth';

// nestFetch is only ever called from inside Next.js Route Handlers
// (web/app/api/.../route.ts), where cookies() from next/headers is writable:
// cookieStore.set(...) here is merged into that handler's returned
// NextResponse. So when a refresh rotates the token pair, we persist the new
// pair back onto the cookie store immediately — otherwise the next request
// would present the now-revoked refresh token, which the backend's reuse
// detection treats as theft and revokes every session for the user.
export async function nestFetch(path: string, init: RequestInit = {}): Promise<Response> {
  const cookieStore = cookies();
  let accessToken = cookieStore.get('access_token')?.value;
  const refreshToken = cookieStore.get('refresh_token')?.value;

  const doFetch = (token?: string) =>
    fetch(`${NEST_URL}${path}`, {
      ...init,
      headers: { ...init.headers, Authorization: `Bearer ${token ?? ''}` },
    });

  let response = await doFetch(accessToken);

  if (response.status === 401 && refreshToken) {
    const refreshRes = await fetch(`${AUTH_URL}/auth/refresh`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ refreshToken }),
    });
    if (refreshRes.ok) {
      const tokens = await refreshRes.json();
      accessToken = tokens.accessToken;
      cookieStore.set('access_token', tokens.accessToken, { httpOnly: true, path: '/' });
      cookieStore.set('refresh_token', tokens.refreshToken, { httpOnly: true, path: '/' });
      response = await doFetch(accessToken);
    }
  }

  return response;
}
