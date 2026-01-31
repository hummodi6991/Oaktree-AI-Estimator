/**
 * API base URL resolution:
 * - In prod, we usually want same-origin, so VITE_API_BASE_URL should be "".
 * - If it's missing (undefined), we still fall back to "" to avoid "undefined/v1/...".
 */
const raw = (import.meta.env.VITE_API_BASE_URL ?? "").trim();

// remove trailing slashes to avoid "//v1/search"
export const API_BASE_URL = raw.replace(/\/+$/, "");

export function makeApiUrl(base: string, path: string): string {
  const trimmedBase = base.trim().replace(/\/+$/, "");
  const normalizedPath = path.startsWith("/") ? path : `/${path}`;
  if (!trimmedBase) return normalizedPath;
  return `${trimmedBase}${normalizedPath}`;
}

export function apiUrl(path: string): string {
  return makeApiUrl(API_BASE_URL, path);
}
