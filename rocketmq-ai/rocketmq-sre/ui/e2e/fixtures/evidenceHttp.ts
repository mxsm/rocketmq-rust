import type { Page, Request, Route } from "@playwright/test";
import { runList } from "./investigationRunData";

import {
  CLUSTER_A,
  CLUSTER_B,
  EVIDENCE_ONE,
  EVIDENCE_TWO,
  INVESTIGATION,
  clusters,
  coverage,
  evidence,
  investigation,
  oidcUser,
  type SyntheticUser,
} from "./evidenceData";

export interface HttpCall {
  url: URL;
  method: string;
  headers: Record<string, string>;
}

interface FetchObservation {
  url: string;
  aborted: boolean;
}

interface FixtureWindow extends Window {
  __sreHttpSignals: FetchObservation[];
  __sreOidcFixture: {
    loaded: (user: SyntheticUser) => Promise<void>;
    unloaded: () => Promise<void>;
  };
}

function deferred<T>() {
  let resolve!: (value: T) => void;
  const promise = new Promise<T>((done) => {
    resolve = done;
  });
  return { promise, resolve };
}

export interface ResponseFixture {
  status?: number;
  contentType?: string;
  body: unknown;
  networkError?: "connectionreset";
}

/** Routes shape HTTP responses; the application still uses its real fetch client. */
export class EvidenceHttpFixture {
  readonly calls: HttpCall[] = [];
  readonly unexpected: string[] = [];
  private readonly failed = new Set<Request>();
  private readonly streamRelease = deferred<void>();
  private readonly sessions = new Map([[oidcUser().access_token, oidcUser()]]);
  private readonly gates: Array<{
    matches: (call: HttpCall) => boolean;
    entered: ReturnType<typeof deferred<HttpCall>>;
    release: ReturnType<typeof deferred<void>>;
    completed: ReturnType<typeof deferred<void>>;
    claimed: boolean;
  }> = [];

  constructor(
    private readonly page: Page,
    private readonly baseURL: string,
    private readonly extension?: (
      call: HttpCall,
      body: string | null,
    ) => ResponseFixture | undefined,
  ) {}

  async install() {
    const authority = new URL("/oidc", this.baseURL).href;
    await this.page.addInitScript(
      ({ authority, initialUser }) => {
        sessionStorage.setItem(
          `oidc.user:${authority}:sre-http-regression`,
          JSON.stringify(initialUser),
        );
        localStorage.setItem(
          "rocketmq-sre-operator-preferences.v1",
          JSON.stringify({ locale: "en-US", timeZone: "UTC", theme: "dark" }),
        );
        const target = window as unknown as FixtureWindow;
        target.__sreHttpSignals = [];
        const original = window.fetch.bind(window);
        window.fetch = (input, init) => {
          const url = new URL(
            input instanceof Request ? input.url : String(input),
            location.href,
          );
          const signal =
            init?.signal ??
            (input instanceof Request ? input.signal : undefined);
          if (url.pathname.startsWith("/v1/")) {
            const observation = {
              url: `${url.pathname}${url.search}`,
              aborted: signal?.aborted ?? false,
            };
            target.__sreHttpSignals.push(observation);
            signal?.addEventListener(
              "abort",
              () => {
                observation.aborted = true;
              },
              { once: true },
            );
          }
          return original(input, init);
        };
      },
      { authority, initialUser: oidcUser() },
    );

    // Only this E2E project wraps the application's actual manager instance.
    // These are synthetic standard lifecycle events, not IdP/signature validation.
    await this.page.route(
      "**/node_modules/.vite/deps/oidc-client-ts.js*",
      async (route) => {
        const original = new URL(route.request().url());
        if (original.searchParams.has("sre_fixture_original"))
          return route.continue();
        original.searchParams.set("sre_fixture_original", "1");
        await route.fulfill({
          contentType: "text/javascript",
          body: `
          export * from ${JSON.stringify(original.href)};
          import { UserManager as Base, User } from ${JSON.stringify(original.href)};
          export class UserManager extends Base {
            constructor(settings) {
              super(settings);
              const addLoaded = this.events.addUserLoaded.bind(this.events);
              this.events.addUserLoaded = (callback) => {
                // Effect subscription selects the mounted manager under StrictMode.
                window.__sreOidcFixture = {
                  loaded: async (data) => {
                    const user = User.fromStorageString(JSON.stringify(data));
                    await this.storeUser(user);
                    await this.events.load(user);
                  },
                  unloaded: async () => { await this.removeUser(); },
                };
                return addLoaded(callback);
              };
            }
          }
        `,
        });
      },
    );
    this.page.on("requestfailed", (request) => this.failed.add(request));
    await this.page.route(
      (url) =>
        url.pathname.startsWith("/v1/") ||
        url.pathname.startsWith("/oidc/") ||
        ["/healthz", "/readyz"].includes(url.pathname),
      (route) => this.respond(route),
    );
  }

  holdNext(matches: (call: HttpCall) => boolean) {
    const gate = {
      matches,
      entered: deferred<HttpCall>(),
      release: deferred<void>(),
      completed: deferred<void>(),
      claimed: false,
    };
    this.gates.push(gate);
    return {
      entered: gate.entered.promise,
      release: async () => {
        gate.release.resolve();
        await gate.completed.promise;
      },
    };
  }

  async publish(user: SyntheticUser) {
    this.sessions.set(user.access_token, user);
    await this.page.evaluate(
      (next) =>
        (window as unknown as FixtureWindow).__sreOidcFixture.loaded(next),
      user,
    );
  }

  async unload() {
    await this.page.evaluate(() =>
      (window as unknown as FixtureWindow).__sreOidcFixture.unloaded(),
    );
  }

  async aborted(fragment: string) {
    return this.page.evaluate(
      (value) =>
        (window as unknown as FixtureWindow).__sreHttpSignals
          .filter((item) => item.url.includes(value))
          .at(-1)?.aborted ?? false,
      fragment,
    );
  }

  async dispose() {
    for (const gate of this.gates) gate.release.resolve();
    this.streamRelease.resolve();
    if (!this.page.isClosed()) {
      try {
        await this.page.unrouteAll({ behavior: "wait" });
      } catch (error) {
        if (!this.page.isClosed()) throw error;
      }
    }
  }

  private async respond(route: Route) {
    const request = route.request();
    const call = {
      url: new URL(request.url()),
      method: request.method(),
      headers: request.headers(),
    };
    this.calls.push(call);
    const response = this.response(call, request.postData());
    if (call.url.pathname === "/v1/events/stream") {
      await this.streamRelease.promise;
    }
    const gate = this.gates.find((item) => !item.claimed && item.matches(call));
    if (gate) {
      gate.claimed = true;
      gate.entered.resolve(call);
      await gate.release.promise;
    }
    try {
      if (response.networkError) {
        await route.abort(response.networkError);
      } else {
        await route.fulfill({
          status: response.status ?? 200,
          contentType: response.contentType ?? "application/json",
          body: response.contentType
            ? String(response.body)
            : JSON.stringify(response.body),
        });
      }
    } catch (error) {
      if (!this.page.isClosed() && !this.failed.has(request)) throw error;
    } finally {
      gate?.completed.resolve();
    }
  }

  private response(call: HttpCall, body: string | null): ResponseFixture {
    const { pathname, searchParams } = call.url;
    const subject = call.headers["x-rocketmq-subject"] ?? "missing-identity";
    if (pathname === "/oidc/.well-known/openid-configuration") {
      return {
        body: {
          issuer: new URL("/oidc", this.baseURL).href,
          authorization_endpoint: new URL("/oidc/authorize", this.baseURL).href,
          token_endpoint: new URL("/oidc/token", this.baseURL).href,
        },
      };
    }
    if (pathname === "/healthz") return { body: { status: "healthy" } };
    if (pathname === "/readyz") return { body: { status: "ready" } };
    const session = this.sessions.get(
      call.headers.authorization?.replace(/^Bearer /, ""),
    );
    if (
      !session ||
      call.headers["x-rocketmq-tenant"] !== session.profile.rocketmq_tenant ||
      call.headers["x-rocketmq-subject"] !== session.profile.sub ||
      call.headers["x-rocketmq-clusters"] !==
        session.profile.rocketmq_clusters.join(",")
    ) {
      this.unexpected.push(`Mismatched synthetic HTTP identity: ${pathname}`);
      return { status: 401, body: { code: "invalid_fixture_identity" } };
    }
    const extended = this.extension?.(call, body);
    if (extended) return extended;
    if (call.method !== "GET") {
      this.unexpected.push(`${call.method} ${pathname}`);
      return { status: 405, body: { code: "read_only_fixture" } };
    }
    if (pathname === "/v1/clusters") return { body: clusters() };
    if (pathname === "/v1/assets") {
      return {
        body: {
          items: [],
          partial: false,
          warnings: [],
          observed_at: "2026-11-11T10:05:00.000Z",
        },
      };
    }
    if (pathname === `/v1/investigations/${INVESTIGATION}`) {
      return { body: investigation(subject) };
    }
    if (pathname === `/v1/investigations/${INVESTIGATION}/runs`)
      return { body: runList() };
    if (pathname === "/v1/capabilities/coverage") {
      const clusterId = searchParams.get("cluster_id");
      if (
        !clusterId ||
        !session.profile.rocketmq_clusters.includes(clusterId) ||
        (searchParams.has("revision_id") &&
          (searchParams.get("investigation_id") !== INVESTIGATION ||
            clusterId !== CLUSTER_A))
      ) {
        this.unexpected.push(`Unbound coverage ${call.url.search}`);
        return { status: 400, body: { code: "invalid_fixture_scope" } };
      }
      const matrix = coverage(searchParams.get("revision_id"));
      if (clusterId === CLUSTER_B && matrix.quality) {
        matrix.quality.cluster_id = CLUSTER_B;
        matrix.quality.packs = [];
        matrix.quality.scenarios = [];
      }
      return { body: matrix };
    }
    if (pathname === "/v1/events/stream") {
      return {
        contentType: "text/event-stream",
        body: ": synthetic read-only fixture\n\n",
      };
    }
    for (const id of [EVIDENCE_ONE, EVIDENCE_TWO]) {
      if (pathname === `/v1/evidence/${id}`)
        return { body: evidence(id, subject) };
      if (pathname === `/v1/evidence/${id}/content`) {
        return {
          body: {
            synthetic_result: `${subject}:revision-${id === EVIDENCE_ONE ? "one" : "two"}`,
            production_verified: false,
          },
        };
      }
    }
    this.unexpected.push(`${call.method} ${pathname}`);
    return { status: 404, body: { code: "fixture_not_found" } };
  }
}
