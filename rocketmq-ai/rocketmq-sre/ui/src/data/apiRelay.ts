import type { SreApi } from "@/api/client";

/** Preserve page request identities across token renewal within one principal. */
export function createApiRelay() {
  let current: SreApi | undefined;
  const methods = new Map<PropertyKey, (...args: unknown[]) => unknown>();
  const api = new Proxy({} as SreApi, {
    get(_target, property) {
      if (!current || typeof Reflect.get(current, property) !== "function")
        return undefined;
      let method = methods.get(property);
      if (!method) {
        method = (...args: unknown[]) => {
          if (!current) throw new Error("SRE session is unavailable");
          return Reflect.apply(Reflect.get(current, property), current, args);
        };
        methods.set(property, method);
      }
      return method;
    },
  });
  return {
    api,
    replace: (next: SreApi | undefined) => {
      current = next;
    },
  };
}
