import { resolveOidcSettings } from "./oidcConfig";

const origin = "https://sre.example.com";
const build = {
  authority: "https://old-id.example.com",
  clientId: "old-ui",
};

describe("OIDC configuration", () => {
  it("uses runtime settings for the same build in a different deployment", () => {
    const settings = resolveOidcSettings(
      {
        authority: "https://id.example.com",
        clientId: "sre-ui",
        redirectUri: `${origin}/custom-callback`,
        postLogoutRedirectUri: `${origin}/signed-out`,
        scope: "openid profile rocketmq:read",
      },
      build,
      origin,
    );
    expect(settings).toMatchObject({
      authority: "https://id.example.com",
      client_id: "sre-ui",
      redirect_uri: `${origin}/custom-callback`,
      post_logout_redirect_uri: `${origin}/signed-out`,
      scope: "openid profile rocketmq:read",
      response_type: "code",
    });
  });

  it("preserves build configuration when no runtime override is supplied", () => {
    expect(resolveOidcSettings(undefined, build, origin)).toMatchObject({
      authority: build.authority,
      client_id: build.clientId,
      redirect_uri: `${origin}/auth/callback`,
      post_logout_redirect_uri: origin,
    });
  });

  it.each([{}, { authority: "https://id.example.com" }, { clientId: "ui" }])(
    "fails closed when deployment settings are incomplete: %j",
    (runtime) => {
      expect(resolveOidcSettings(runtime, {}, origin)).toBeUndefined();
    },
  );

  it("does not restore a build-time identity after an explicit empty override", () => {
    expect(
      resolveOidcSettings({ authority: "", clientId: "" }, build, origin),
    ).toBeUndefined();
  });

  it("uses deployment defaults when optional Docker build arguments are empty", () => {
    const settings = resolveOidcSettings(
      build,
      { redirectUri: "", postLogoutRedirectUri: "", scope: "" },
      origin,
    );
    expect(settings?.redirect_uri).toBe(`${origin}/auth/callback`);
    expect(settings?.post_logout_redirect_uri).toBe(origin);
    expect(settings?.scope?.split(" ")).toContain("openid");
  });
});
