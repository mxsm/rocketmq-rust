import { useLayoutEffect, useMemo } from "react";

import { createSupervisedSreApi, type SupervisedSreApi } from "@/api/supervisedClient";
import { useAuth } from "@/auth/AuthContext";
import { createApiRelay } from "@/data/apiRelay";

/** Renew credentials within one principal without remounting action forms. */
export function useSupervisedApi() {
  const auth = useAuth();
  const identityKey = JSON.stringify([
    auth.status,
    auth.session?.tenantId,
    auth.session?.subject,
    [...(auth.session?.roles ?? [])].sort(),
    [...(auth.session?.clusterIds ?? [])].sort(),
  ]);
  const requestApi = useMemo(
    () => auth.requestContext
      ? createSupervisedSreApi(auth.requestContext)
      : undefined,
    [auth.requestContext],
  );
  const relay = useMemo(
    () => ({ identityKey, ...createApiRelay<SupervisedSreApi>() }),
    // Each principal owns a relay; an old callback cannot acquire a new identity.
    [identityKey],
  );
  useLayoutEffect(() => {
    relay.replace(requestApi);
    return () => relay.replace(undefined);
  }, [relay, requestApi]);
  return { auth, identityKey, api: requestApi ? relay.api : undefined };
}
