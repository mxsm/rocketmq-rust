import { render, screen } from "@testing-library/react";

import { AuthProvider } from "@/auth/AuthContext";
import { SreDataProvider, useSreData } from "./SreDataContext";
import { DEMO_TENANT_ID } from "./phase1Demo";

function Roster() {
  const { clusters, loading } = useSreData();
  return (
    <div>
      {loading
        ? "loading"
        : clusters.map((cluster) => (
            <span key={cluster.id}>
              {cluster.external_cluster_key} · {cluster.tenant_id}
            </span>
          ))}
    </div>
  );
}

it("keeps the default DEV identity and demo cluster roster in the same tenant", async () => {
  window.sessionStorage.clear();
  window.history.replaceState(null, "", "/?demo=1");
  render(
    <AuthProvider>
      <SreDataProvider>
        <Roster />
      </SreDataProvider>
    </AuthProvider>,
  );
  expect(
    await screen.findByText(`rmq-prod-cn · ${DEMO_TENANT_ID}`),
  ).toBeVisible();
  expect(screen.getByText(`rmq-staging · ${DEMO_TENANT_ID}`)).toBeVisible();
  expect(screen.getByText(`rmq-dr · ${DEMO_TENANT_ID}`)).toBeVisible();
});
