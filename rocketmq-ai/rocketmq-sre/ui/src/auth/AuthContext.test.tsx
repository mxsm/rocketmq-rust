import { act, cleanup, render, screen, waitFor } from "@testing-library/react";
import { User } from "oidc-client-ts";
import type * as OidcModule from "oidc-client-ts";
import { Activity, StrictMode } from "react";

import { AuthProvider, useAuth } from "./AuthContext";

const oidc = vi.hoisted(() => {
  const loaded = new Set<(user: User) => void>();
  const unloaded = new Set<() => void>();
  const expired = new Set<() => void>();
  return {
    loaded,
    unloaded,
    expired,
    constructed: vi.fn<(settings: OidcModule.UserManagerSettings) => void>(),
    manager: {
      getUser: vi.fn<() => Promise<User | null>>(),
      signinRedirectCallback: vi.fn<() => Promise<User>>(),
      startSilentRenew: vi.fn<() => void>(),
      stopSilentRenew: vi.fn<() => void>(),
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
    constructor(settings: OidcModule.UserManagerSettings) {
      oidc.constructed(settings);
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
    oidc.constructed.mockClear();
    oidc.manager.startSilentRenew.mockReset();
    oidc.manager.stopSilentRenew.mockReset();
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

  it.each(["invalid_scope", "access_denied"])(
    "validates an OIDC %s response through the SDK and never restores a stale stored identity",
    async (code) => {
      window.history.replaceState(null, "", `/oidc/callback?error=${code}&error_description=PRIVATE_DESCRIPTION_CANARY&state=PRIVATE_STATE_CANARY`);
      oidc.manager.getUser.mockResolvedValue(user("stale-stored-subject", "PRIVATE_STORED_TOKEN_CANARY"));
      // Both a provider rejection and an unrecognized state are SDK failures;
      // their external descriptions must never become UI error content.
      oidc.manager.signinRedirectCallback.mockRejectedValue(new Error("PRIVATE_SDK_ERROR_CANARY"));
      render(<AuthProvider><Probe /></AuthProvider>);

      await screen.findByText("error");
      expect(oidc.manager.signinRedirectCallback).toHaveBeenCalledTimes(1);
      expect(oidc.manager.getUser).not.toHaveBeenCalled();
      expect(screen.getByTestId("context")).toBeEmptyDOMElement();
      expect(screen.getByTestId("error")).toHaveTextContent("OIDC 登录回调校验失败，请重新登录。");
      expect(document.body.textContent).not.toContain("PRIVATE_");
      expect(document.body.textContent).not.toContain(code);
      expect(window.location.pathname).toBe("/oidc/callback");
      expect(window.location.search).toBe("");
    },
  );

  it("keeps a validated successful callback and clears its response parameters", async () => {
    window.history.replaceState(null, "", "/oidc/callback?code=PRIVATE_CODE_CANARY&state=PRIVATE_STATE_CANARY");
    oidc.manager.signinRedirectCallback.mockResolvedValue(user("validated-callback-user"));
    render(<AuthProvider><Probe /></AuthProvider>);

    await screen.findByText("authenticated");
    expect(oidc.manager.signinRedirectCallback).toHaveBeenCalledTimes(1);
    expect(oidc.manager.getUser).not.toHaveBeenCalled();
    expect(screen.getByTestId("context")).toHaveTextContent("validated-callback-user");
    expect(screen.getByTestId("error")).toBeEmptyDOMElement();
    expect(window.location.search).toBe("");
    expect(document.body.textContent).not.toContain("PRIVATE_");
  });

  it.each(["resolve", "reject"] as const)(
    "exchanges a single-use OIDC callback only once across StrictMode replay when it later %s settles",
    async (outcome) => {
      window.history.replaceState(null, "", "/oidc/callback?code=PRIVATE_CODE_CANARY&state=PRIVATE_STATE_CANARY");
      const callback = deferred<User>();
      oidc.manager.signinRedirectCallback.mockReturnValue(callback.promise);
      render(<StrictMode><AuthProvider><Probe /></AuthProvider></StrictMode>);
      await act(async () => {
        await Promise.resolve();
      });
      expect(oidc.manager.signinRedirectCallback).toHaveBeenCalledTimes(1);
      expect(oidc.manager.signinRedirectCallback).toHaveBeenCalledWith(window.location.href);
      expect(oidc.manager.getUser).not.toHaveBeenCalled();
      await act(async () => {
        if (outcome === "resolve") callback.resolve(user("single-exchange-user"));
        else callback.reject(new Error("PRIVATE_SDK_CALLBACK_ERROR_CANARY"));
        await callback.promise.catch(() => undefined);
      });
      expect(screen.getByTestId("status")).toHaveTextContent(outcome === "resolve" ? "authenticated" : "error");
      expect(window.location.search).toBe("");
      expect(document.body.textContent).not.toContain("PRIVATE_");
    },
  );

  it.each(["resolve", "reject"] as const)(
    "keeps a replacement provider's identity and URL when an unmounted callback later %s settles",
    async (outcome) => {
      window.history.replaceState(null, "", "/oidc/callback?code=PRIVATE_OLD_CODE&state=PRIVATE_OLD_STATE");
      const callback = deferred<User>();
      oidc.manager.signinRedirectCallback.mockReturnValue(callback.promise);
      const old = render(<AuthProvider><Probe /></AuthProvider>);
      await act(async () => { await Promise.resolve(); });
      old.unmount();
      const selected = "/plans/synthetic?cluster=synthetic-cluster-a&resource=broker%2Fbroker-a";
      window.history.replaceState(null, "", selected);
      oidc.manager.getUser.mockResolvedValue(user("replacement-principal"));
      render(<AuthProvider><Probe /></AuthProvider>);
      await screen.findByText("authenticated");
      await act(async () => {
        if (outcome === "resolve") callback.resolve(user("retired-principal", "PRIVATE_OLD_TOKEN"));
        else callback.reject(new Error("PRIVATE_OLD_ERROR"));
        await callback.promise.catch(() => undefined);
      });

      expect(screen.getByTestId("context")).toHaveTextContent("replacement-principal");
      expect(screen.getByTestId("error")).toBeEmptyDOMElement();
      expect(window.location.pathname + window.location.search).toBe(selected);
      expect(document.body.textContent).not.toContain("PRIVATE_");
      expect(oidc.manager.signinRedirectCallback).toHaveBeenCalledTimes(1);
    },
  );

  it.each(["resolve", "reject"] as const)(
    "revalidates a reinserted callback URL through the SDK after the original exchange %s settles",
    async (outcome) => {
      const callbackUrl = "/oidc/callback?code=PRIVATE_REINSERTED_CODE&state=PRIVATE_REINSERTED_STATE";
      window.history.replaceState(null, "", callbackUrl);
      const callback = deferred<User>();
      oidc.manager.signinRedirectCallback.mockReturnValueOnce(callback.promise)
        .mockRejectedValue(new Error("PRIVATE_ALREADY_CONSUMED_STATE"));
      const content = (visible: boolean) => (
        <Activity mode={visible ? "visible" : "hidden"}>
          <AuthProvider><Probe /></AuthProvider>
        </Activity>
      );
      const view = render(content(true));
      await act(async () => {
        if (outcome === "resolve") callback.resolve(user("first-exchange-principal"));
        else callback.reject(new Error("PRIVATE_ORIGINAL_REJECTION"));
        await callback.promise.catch(() => undefined);
      });
      await screen.findByText(outcome === "resolve" ? "authenticated" : "error");
      view.rerender(content(false));
      window.history.replaceState(null, "", callbackUrl);
      view.rerender(content(true));

      await waitFor(() => expect(oidc.manager.signinRedirectCallback).toHaveBeenCalledTimes(2));
      await screen.findByText("error");
      expect(oidc.constructed).toHaveBeenCalledTimes(1);
      expect(screen.getByTestId("context")).toBeEmptyDOMElement();
      expect(screen.getByTestId("error")).toHaveTextContent("OIDC 登录回调校验失败，请重新登录。");
      expect(document.body.textContent).not.toContain("PRIVATE_");
    },
  );

  it("does not let a late rejected callback replace a newer lifecycle identity", async () => {
    window.history.replaceState(null, "", "/oidc/callback?error=access_denied&state=PRIVATE_STATE_CANARY");
    const callback = deferred<User>();
    oidc.manager.signinRedirectCallback.mockReturnValue(callback.promise);
    render(<AuthProvider><Probe /></AuthProvider>);
    act(() => oidc.loaded.forEach((loaded) => loaded(user("newer-callback-identity"))));
    await act(async () => {
      callback.reject(new Error("PRIVATE_ERROR_DESCRIPTION_CANARY"));
      await callback.promise.catch(() => undefined);
    });

    expect(screen.getByTestId("status")).toHaveTextContent("authenticated");
    expect(screen.getByTestId("context")).toHaveTextContent("newer-callback-identity");
    expect(screen.getByTestId("error")).toBeEmptyDOMElement();
    expect(window.location.search).toBe("");
    expect(document.body.textContent).not.toContain("PRIVATE_");
  });

  it.each(["resolve", "reject"] as const)(
    "keeps a newer deep-link query when a previous callback later %s settles",
    async (outcome) => {
      window.history.replaceState(null, "", "/oidc/callback?code=PRIVATE_CODE_CANARY&state=PRIVATE_STATE_CANARY");
      const callback = deferred<User>();
      oidc.manager.signinRedirectCallback.mockReturnValue(callback.promise);
      render(<AuthProvider><Probe /></AuthProvider>);
      const selected = "/investigations/synthetic?run_id=00000000-0000-4000-8000-000000000004&cluster=synthetic-cluster-a";
      window.history.replaceState(null, "", selected);
      await act(async () => {
        if (outcome === "resolve") callback.resolve(user("validated-user"));
        else callback.reject(new Error("PRIVATE_CALLBACK_ERROR_CANARY"));
        await callback.promise.catch(() => undefined);
      });

      expect(window.location.pathname + window.location.search).toBe(selected);
      expect(screen.getByTestId("status")).toHaveTextContent(outcome === "resolve" ? "authenticated" : "error");
      expect(document.body.textContent).not.toContain("PRIVATE_");
    },
  );

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
    expect(oidc.constructed).toHaveBeenCalledTimes(1);
    expect(oidc.constructed.mock.calls[0][0].automaticSilentRenew).toBe(false);
    expect(oidc.manager.startSilentRenew).toHaveBeenCalled();
    expect([oidc.loaded.size, oidc.unloaded.size, oidc.expired.size]).toEqual([1, 1, 1]);
    view.unmount();
    expect(oidc.manager.stopSilentRenew).toHaveBeenCalled();
    expect(oidc.manager.startSilentRenew.mock.calls.length).toBe(oidc.manager.stopSilentRenew.mock.calls.length);
    expect([oidc.loaded.size, oidc.unloaded.size, oidc.expired.size]).toEqual([0, 0, 0]);
  });
});
