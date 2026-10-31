import { cookies } from 'next/headers';
import { NEST_URL } from './nest';

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
    const refreshRes = await fetch(`${NEST_URL}/auth/refresh`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ refreshToken }),
    });
    if (refreshRes.ok) {
      const tokens = await refreshRes.json();
      accessToken = tokens.accessToken;
      response = await doFetch(accessToken);
    }
  }

  return response;
}
