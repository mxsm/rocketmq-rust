import { readFileSync } from "node:fs";
import { resolve } from "node:path";

describe("full-screen layout contract", () => {
  const tokens = readFileSync(
    resolve(process.cwd(), "src/styles/tokens.css"),
    "utf8",
  );
  const app = readFileSync(
    resolve(process.cwd(), "src/styles/app.css"),
    "utf8",
  );
  const autonomy = readFileSync(
    resolve(
      process.cwd(),
      "src/styles/autonomy-operations.css",
    ),
    "utf8",
  );

  it("does not constrain the root or application shell", () => {
    expect(tokens).toMatch(/html,\s*body,\s*#root\s*\{[^}]*width:\s*100%/s);
    expect(tokens).toMatch(/#root\s*\{[^}]*min-width:\s*0/s);
    expect(app).toMatch(/\.app-shell\s*\{[^}]*width:\s*100%/s);
    expect(app).toMatch(/\.app-shell\s*\{[^}]*max-width:\s*none/s);
    expect(app).toMatch(/\.workspace\s*\{[^}]*height:\s*100dvh/s);
    expect(app).toMatch(/\.main-content\s*\{[^}]*overflow:\s*auto/s);
  });

  it("limits only the authentication card, not its full-screen shell", () => {
    expect(app).toMatch(/\.auth-shell\s*\{[^}]*min-height:\s*100dvh/s);
    expect(app).toMatch(/\.auth-shell\s*\{[^}]*width:\s*100%/s);
    expect(app).toMatch(/\.auth-card\s*\{[^}]*max-width:\s*440px/s);
  });

  it("keeps autonomy operations full-width and desktop-dense", () => {
    expect(autonomy).toMatch(
      /\.autonomy-operations-page\s*\{[^}]*width:\s*100%/s,
    );
    expect(autonomy).toMatch(
      /\.autonomy-operations-page\s*\{[^}]*max-width:\s*none/s,
    );
    expect(autonomy).toMatch(
      /\.autonomy-metric-grid\s*\{[^}]*repeat\(4,\s*minmax\(0,\s*1fr\)\)/s,
    );
    expect(autonomy).toMatch(
      /\.autonomy-candidate-grid\s*\{[^}]*repeat\(2,\s*minmax\(0,\s*1fr\)\)/s,
    );
  });
});
