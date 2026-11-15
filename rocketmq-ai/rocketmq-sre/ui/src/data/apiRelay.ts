import type { SreApi } from "@/api/client";

/** Preserve page request identities across token renewal within one principal. */
export function createApiRelay<T extends object = SreApi>() {
  let current: T | undefined;
  const methods = new Map<PropertyKey, (...args: unknown[]) => unknown>();
  const api = new Proxy({} as T, {
    get(_target, property) {
      if (!current || typeof Reflect.get(current, property) !== "function")
        return undefined;
      let method = methods.get(property);
      if (!method) {
        method = (...args: unknown[]) => {
          if (!current) throw new Error("SRE session is unavailable");
          const target: unknown = Reflect.get(current, property);
          if (typeof target !== "function") throw new Error("SRE session is unavailable");
          return Reflect.apply(target, current, args);
        };
        methods.set(property, method);
      }
      return method;
    },
  });
  return {
    api,
    replace: (next: T | undefined) => {
      current = next;
    },
  };
}
