import http from "@/api/http";

export type Topology = "SINGLE" | "CLUSTER" | "SENTINEL";
export interface ProfileInput {
  topology: Topology;
  addresses: string[];
  database: number;
  masterName: string;
  username: string;
  password: string | null;
  sentinelUsername: string;
  sentinelPassword: string | null;
}
export interface ProfileView extends Omit<ProfileInput, "password" | "sentinelPassword"> {
  passwordSet: boolean;
  sentinelPasswordSet: boolean;
  clientActive: boolean;
}
export interface ConnectionSettings {
  revision: number;
  runtime: ProfileView | null;
  cache: ProfileView | null;
  managedNamespaces: string[];
}
export const connectionsApi = {
  get: () => http.get<ConnectionSettings>("/connections"),
  save: (value: {
    revision: number;
    runtime: ProfileInput | null;
    cache: ProfileInput | null;
    managedNamespaces: string[];
  }) => http.put<ConnectionSettings>("/connections", value),
  test: (role: "RUNTIME" | "CACHE", value: ProfileInput) =>
    http.post<{ reachable: boolean }>(`/connections/test/${role}`, value)
};
