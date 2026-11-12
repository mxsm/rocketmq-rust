import { act, cleanup, render, screen } from "@testing-library/react";
import { User } from "oidc-client-ts";
import type * as OidcModule from "oidc-client-ts";
import { StrictMode } from "react";

import { AuthProvider, useAuth } from "./AuthContext";

const oidc = vi.hoisted(() => {
  const loaded = new Set<(user: User) => void>();
  const unloaded = new Set<() => void>();
  const expired = new Set<() => void>();
  return {
    loaded,
    unloaded,
    expired,
    manager: {
      getUser: vi.fn<() => Promise<User | null>>(),
      signinRedirectCallback: vi.fn<() => Promise<User>>(),
      events: {
        addUserLoaded: (callback: (user: User) => void) => loaded.add(callback),
        removeUserLoaded: (callback: (user: User) => void) => loaded.delete(callback),
        addUserUnloaded: (callback: () => void) => unloaded.add(callback),
        removeUserUnloaded: (callback: () => void) => unloaded.delete(callback),
        addAccessTokenExpired: (callback: () => void) => expired.add(callback),
        removeAccessTokenExpired: (callback: () => void) => expired.delete(callback),
      },
    },
  };
});

vi.mock("oidc-client-ts", async (importOriginal) => ({
  ...await importOriginal<typeof OidcModule>(),
  UserManager: class {
    constructor() {
      return oidc.manager;
    }
  },
  WebStorageStateStore: class {},
}));

function user(subject: string, token = "synthetic-token"): User {
  const issued = Math.floor(Date.now() / 1000);
  return new User({
    access_token: token,
    token_type: "Bearer",
    expires_at: issued + 3600,
    scope: "openid rocketmq:read",
    profile: {
      sub: subject,
      iss: "https://identity.example.test",
      aud: "sre-ui",
      iat: issued,
      exp: issued + 3600,
      name: subject,
      rocketmq_tenant: "synthetic-tenant",
      rocketmq_clusters: ["synthetic-cluster-a"],
      roles: ["rocketmq:read"],
    },
  });
}

function deferred<T>() {
  let resolve!: (value: T) => void;
  let reject!: (reason: Error) => void;
  const promise = new Promise<T>((done, fail) => {
    resolve = done;
    reject = fail;
  });
  return { promise, resolve, reject };
}

function Probe() {
  const auth = useAuth();
  return (
    <>
      <span data-testid="status">{auth.status}</span>
      <output data-testid="context">{JSON.stringify(auth.requestContext)}</output>
      <span data-testid="error">{auth.error}</span>
    </>
  );
}

describe("OIDC session lifecycle", () => {
  beforeEach(() => {
    vi.stubEnv("VITE_SRE_AUTH_MODE", "oidc");
    window.__ROCKETMQ_SRE_CONFIG__ = {
      oidc: { authority: "https://identity.example.test", clientId: "sre-ui" },
    };
    window.history.replaceState(null, "", "/");
    oidc.manager.getUser.mockReset().mockResolvedValue(null);
    oidc.manager.signinRedirectCallback.mockReset();
  });

  afterEach(() => {
    cleanup();
    delete window.__ROCKETMQ_SRE_CONFIG__;
    vi.unstubAllEnvs();
  });

  it("uses loaded events for token renewal and changed principal claims", async () => {
    oidc.manager.getUser.mockResolvedValue(user("first"));
    render(
      <AuthProvider>
        <Probe />
      </AuthProvider>,
    );
    await screen.findByText("authenticated");

    act(() => oidc.loaded.forEach((callback) => callback(user("first", "renewed"))));
    expect(screen.getByTestId("context")).toHaveTextContent('"token":"renewed"');
    const next = user("second");
    next.profile.rocketmq_tenant = "next-tenant";
    next.profile.rocketmq_clusters = ["synthetic-cluster-b"];
    next.profile.roles = ["reader"];
    act(() => oidc.loaded.forEach((callback) => callback(next)));
    expect(screen.getByTestId("context")).toHaveTextContent('"subject":"second"');
    expect(screen.getByTestId("context")).toHaveTextContent("next-tenant");
    expect(screen.getByTestId("context")).not.toHaveTextContent("synthetic-cluster-a");
  });

  it.each(["resolve", "reject"] as const)(
    "does not let an initial getUser %s overwrite a newer lifecycle event",
    async (outcome) => {
      const initial = deferred<User | null>();
      oidc.manager.getUser.mockReturnValue(initial.promise);
      render(
        <AuthProvider>
          <Probe />
        </AuthProvider>,
      );
      act(() => oidc.loaded.forEach((callback) => callback(user("newer"))));
      await act(async () => {
        if (outcome === "resolve") initial.resolve(user("old-storage"));
        else initial.reject(new Error("old read failed"));
        await initial.promise.catch(() => undefined);
      });
      expect(screen.getByTestId("status")).toHaveTextContent("authenticated");
      expect(screen.getByTestId("context")).toHaveTextContent("newer");
      expect(screen.getByTestId("error")).toBeEmptyDOMElement();
    },
  );

  it.each(["unloaded", "expired"] as const)(
    "clears the current session on %s and rejects a late initial user",
    async (event) => {
      const initial = deferred<User | null>();
      oidc.manager.getUser.mockReturnValue(initial.promise);
      render(
        <AuthProvider>
          <Probe />
        </AuthProvider>,
      );
      act(() => oidc.loaded.forEach((callback) => callback(user("first"))));
      act(() => oidc[event].forEach((callback) => callback()));
      expect(screen.getByTestId("status")).toHaveTextContent("anonymous");
      expect(screen.getByTestId("context")).toBeEmptyDOMElement();
      await act(async () => initial.resolve(user("old-storage")));
      expect(screen.getByTestId("status")).toHaveTextContent("anonymous");
    },
  );

  it("removes an existing session when a loaded user lacks authorization claims", async () => {
    oidc.manager.getUser.mockResolvedValue(user("first"));
    render(
      <AuthProvider>
        <Probe />
      </AuthProvider>,
    );
    await screen.findByText("authenticated");
    const invalid = user("missing-clusters");
    invalid.profile.rocketmq_clusters = [];
    act(() => oidc.loaded.forEach((callback) => callback(invalid)));
    expect(screen.getByTestId("status")).toHaveTextContent("error");
    expect(screen.getByTestId("context")).toBeEmptyDOMElement();
  });

  it("owns one subscription per event and removes all listeners on unmount", async () => {
    const view = render(
      <StrictMode>
        <AuthProvider>
          <Probe />
        </AuthProvider>
      </StrictMode>,
    );
    await screen.findByText("anonymous");
    expect([oidc.loaded.size, oidc.unloaded.size, oidc.expired.size]).toEqual([1, 1, 1]);
    view.unmount();
    expect([oidc.loaded.size, oidc.unloaded.size, oidc.expired.size]).toEqual([0, 0, 0]);
  });
});
