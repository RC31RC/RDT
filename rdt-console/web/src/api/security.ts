import http from "@/api/http";
import type { Principal } from "./management";
export type Role = Principal["role"];
export interface LocalUser {
  username: string;
  displayName: string;
  role: Role;
  enabled: boolean;
}
export const securityApi = {
  users: () => http.get<LocalUser[]>("/users"),
  create: (data: { username: string; displayName: string; role: Role; password: string }) =>
    http.post<LocalUser>("/users", data),
  update: (
    username: string,
    data: {
      displayName: string;
      role: Role;
      enabled: boolean;
      password: string | null;
    }
  ) => http.put<LocalUser>(`/users/${encodeURIComponent(username)}`, data),
  remove: (username: string) => http.delete(`/users/${encodeURIComponent(username)}`),
  password: (data: { currentPassword: string; newPassword: string }) => http.post("/auth/password", data)
};
export function apiError(error: unknown, fallback = "操作失败，请检查输入后重试") {
  return (error as { response?: { data?: { message?: string } } }).response?.data?.message || fallback;
}
