import {
  createContext,
  forwardRef,
  type AnchorHTMLAttributes,
  type MouseEvent,
  type ReactNode,
  useCallback,
  useContext,
  useEffect,
  useMemo,
  useState,
} from "react";

export type To =
  | string
  | {
      pathname?: string;
      search?: string;
      hash?: string;
    };

export interface NavigateOptions {
  replace?: boolean;
}

export interface Location {
  pathname: string;
  search: string;
  hash: string;
  state: unknown;
}

export interface RouteObject {
  path?: string;
  index?: boolean;
  element: ReactNode;
  children?: RouteObject[];
}

export interface BrowserRouter {
  routes: RouteObject[];
}

type NavigateFunction = (
  target: To,
  options?: NavigateOptions,
) => void;

interface NavigationContextValue {
  location: Location;
  navigate: NavigateFunction;
}

interface RouteMatch {
  element: ReactNode;
  params: Readonly<Record<string, string>>;
}

const NavigationContext = createContext<
  NavigationContextValue | undefined
>(undefined);
const OutletContext = createContext<ReactNode>(null);
const ParamsContext = createContext<Readonly<Record<string, string>>>({});
const NAVIGATION_EVENT = "rocketmq-sre:navigate";

export function createBrowserRouter(routes: RouteObject[]): BrowserRouter {
  if (routes.length === 0) {
    throw new Error("browser router requires at least one route");
  }
  return { routes };
}

export function RouterProvider({ router }: { router: BrowserRouter }) {
  const [location, setLocation] = useState(readBrowserLocation);

  useEffect(() => {
    const update = () => setLocation(readBrowserLocation());
    window.addEventListener("popstate", update);
    window.addEventListener(NAVIGATION_EVENT, update);
    return () => {
      window.removeEventListener("popstate", update);
      window.removeEventListener(NAVIGATION_EVENT, update);
    };
  }, []);

  const navigate = useCallback<NavigateFunction>(
    (target, options) => {
      const href = resolveInternalHref(target, location);
      if (options?.replace) {
        window.history.replaceState(null, "", href);
      } else {
        window.history.pushState(null, "", href);
      }
      window.dispatchEvent(new Event(NAVIGATION_EVENT));
    },
    [location],
  );
  const value = useMemo(
    () => ({ location, navigate }),
    [location, navigate],
  );
  const { layout, match } = matchBrowserRoutes(
    router.routes,
    location.pathname,
  );

  return (
    <NavigationContext.Provider value={value}>
      <ParamsContext.Provider value={match.params}>
        <OutletContext.Provider value={match.element}>
          {layout}
        </OutletContext.Provider>
      </ParamsContext.Provider>
    </NavigationContext.Provider>
  );
}

export function MemoryRouter({
  children,
  initialEntries = ["/"],
}: {
  children: ReactNode;
  initialEntries?: string[];
}) {
  const initial = initialEntries[0] ?? "/";
  const [location, setLocation] = useState(() =>
    locationFromUrl(new URL(initial, "http://router.local")),
  );
  const navigate = useCallback<NavigateFunction>((target) => {
    setLocation((current) =>
      locationFromUrl(
        new URL(
          resolveInternalHref(target, current),
          "http://router.local",
        ),
      ),
    );
  }, []);
  const value = useMemo(
    () => ({ location, navigate }),
    [location, navigate],
  );
  return (
    <NavigationContext.Provider value={value}>
      <ParamsContext.Provider value={{}}>
        {children}
      </ParamsContext.Provider>
    </NavigationContext.Provider>
  );
}

export function Outlet() {
  return useContext(OutletContext);
}

export function Navigate({
  to,
  replace = false,
}: {
  to: To;
  replace?: boolean;
}) {
  const navigate = useNavigate();
  useEffect(() => {
    navigate(to, { replace });
  }, [navigate, replace, to]);
  return null;
}

export function useLocation(): Location {
  return useNavigationContext().location;
}

export function useNavigate(): NavigateFunction {
  return useNavigationContext().navigate;
}

export function useParams<
  Params extends Record<string, string | undefined> = Record<
    string,
    string | undefined
  >,
>(): Readonly<Params> {
  return useContext(ParamsContext) as Readonly<Params>;
}

type SearchParamsInput =
  | URLSearchParams
  | string
  | Record<string, string | readonly string[]>
  | readonly (readonly [string, string])[];

export function useSearchParams(): [
  URLSearchParams,
  (next: SearchParamsInput, options?: NavigateOptions) => void,
] {
  const location = useLocation();
  const navigate = useNavigate();
  const params = useMemo(
    () => new URLSearchParams(location.search),
    [location.search],
  );
  const setParams = useCallback(
    (next: SearchParamsInput, options?: NavigateOptions) => {
      const encoded = createSearchParams(next).toString();
      navigate(
        {
          pathname: location.pathname,
          search: encoded ? `?${encoded}` : "",
          hash: location.hash,
        },
        options,
      );
    },
    [location.hash, location.pathname, navigate],
  );
  return [params, setParams];
}

export interface LinkProps
  extends Omit<AnchorHTMLAttributes<HTMLAnchorElement>, "href"> {
  to: To;
  replace?: boolean;
}

export const Link = forwardRef<HTMLAnchorElement, LinkProps>(
  function Link({ to, replace = false, onClick, target, ...props }, ref) {
    const { location, navigate } = useNavigationContext();
    const href = resolveInternalHref(to, location);
    const follow = (event: MouseEvent<HTMLAnchorElement>) => {
      onClick?.(event);
      if (
        event.defaultPrevented ||
        event.button !== 0 ||
        event.metaKey ||
        event.altKey ||
        event.ctrlKey ||
        event.shiftKey ||
        (target && target !== "_self") ||
        props.download
      ) {
        return;
      }
      event.preventDefault();
      navigate(to, { replace });
    };
    return (
      <a
        {...props}
        href={href}
        onClick={follow}
        ref={ref}
        target={target}
      />
    );
  },
);

export interface NavLinkRenderProps {
  isActive: boolean;
}

export interface NavLinkProps
  extends Omit<LinkProps, "className"> {
  className?:
    | string
    | ((props: NavLinkRenderProps) => string | undefined);
  end?: boolean;
}

export const NavLink = forwardRef<HTMLAnchorElement, NavLinkProps>(
  function NavLink(
    { className, end = false, to, "aria-current": ariaCurrent, ...props },
    ref,
  ) {
    const location = useLocation();
    const target = pathnameFor(to, location);
    const isActive =
      location.pathname === target ||
      (!end &&
        target !== "/" &&
        location.pathname.startsWith(`${target}/`));
    const resolvedClassName =
      typeof className === "function"
        ? className({ isActive })
        : className;
    return (
      <Link
        {...props}
        aria-current={ariaCurrent ?? (isActive ? "page" : undefined)}
        className={resolvedClassName}
        ref={ref}
        to={to}
      />
    );
  },
);

function useNavigationContext(): NavigationContextValue {
  const context = useContext(NavigationContext);
  if (!context) {
    throw new Error("router hook must be used inside RouterProvider or MemoryRouter");
  }
  return context;
}

function matchBrowserRoutes(
  routes: RouteObject[],
  pathname: string,
): { layout: ReactNode; match: RouteMatch } {
  for (const route of routes) {
    if (!route.children) {
      const direct = matchRoute(route, pathname);
      if (direct) {
        return { layout: direct.element, match: direct };
      }
      continue;
    }
    const child = matchRouteList(route.children, pathname);
    if (child) {
      return { layout: route.element, match: child };
    }
  }
  throw new Error(`no route or wildcard matched ${pathname}`);
}

function matchRouteList(
  routes: RouteObject[],
  pathname: string,
): RouteMatch | undefined {
  let wildcard: RouteObject | undefined;
  for (const route of routes) {
    if (route.path === "*") {
      wildcard = route;
      continue;
    }
    const match = matchRoute(route, pathname);
    if (match) {
      return match;
    }
  }
  return wildcard
    ? { element: wildcard.element, params: {} }
    : undefined;
}

function matchRoute(
  route: RouteObject,
  pathname: string,
): RouteMatch | undefined {
  const normalizedPathname = normalizePathname(pathname);
  if (route.index) {
    return normalizedPathname === "/"
      ? { element: route.element, params: {} }
      : undefined;
  }
  if (!route.path) {
    return undefined;
  }
  const pattern = normalizePathname(route.path);
  const patternSegments = splitPath(pattern);
  const pathnameSegments = splitPath(normalizedPathname);
  if (patternSegments.length !== pathnameSegments.length) {
    return undefined;
  }
  const params: Record<string, string> = {};
  for (let index = 0; index < patternSegments.length; index += 1) {
    const expected = patternSegments[index];
    const actual = pathnameSegments[index];
    if (expected.startsWith(":")) {
      const name = expected.slice(1);
      if (!name || !actual) {
        return undefined;
      }
      try {
        params[name] = decodeURIComponent(actual);
      } catch {
        return undefined;
      }
    } else if (expected !== actual) {
      return undefined;
    }
  }
  return { element: route.element, params };
}

function readBrowserLocation(): Location {
  return {
    pathname: normalizePathname(window.location.pathname),
    search: window.location.search,
    hash: window.location.hash,
    state: window.history.state,
  };
}

function locationFromUrl(url: URL): Location {
  return {
    pathname: normalizePathname(url.pathname),
    search: url.search,
    hash: url.hash,
    state: null,
  };
}

function pathnameFor(target: To, location: Location): string {
  const href = resolveInternalHref(target, location);
  return normalizePathname(
    new URL(href, "http://router.local").pathname,
  );
}

function resolveInternalHref(target: To, location: Location): string {
  const raw = toRawHref(target, location);
  if (
    raw.startsWith("//") ||
    /^[a-zA-Z][a-zA-Z\d+.-]*:/.test(raw)
  ) {
    throw new Error("router navigation target must be an internal path");
  }
  const origin =
    typeof window === "undefined"
      ? "http://router.local"
      : window.location.origin;
  const base = new URL(
    `${location.pathname}${location.search}${location.hash}`,
    origin,
  );
  const resolved = new URL(raw, base);
  if (
    resolved.origin !== origin ||
    !["http:", "https:"].includes(resolved.protocol)
  ) {
    throw new Error("router navigation target crosses the application origin");
  }
  return `${normalizePathname(resolved.pathname)}${resolved.search}${resolved.hash}`;
}

function toRawHref(target: To, location: Location): string {
  if (typeof target === "string") {
    return target || "/";
  }
  const pathname = target.pathname ?? location.pathname;
  const search = normalizeSearch(target.search ?? "");
  const hash = normalizeHash(target.hash ?? "");
  return `${pathname}${search}${hash}`;
}

function normalizeSearch(search: string): string {
  if (!search) {
    return "";
  }
  return search.startsWith("?") ? search : `?${search}`;
}

function normalizeHash(hash: string): string {
  if (!hash) {
    return "";
  }
  return hash.startsWith("#") ? hash : `#${hash}`;
}

function normalizePathname(pathname: string): string {
  const withLeadingSlash = pathname.startsWith("/")
    ? pathname
    : `/${pathname}`;
  if (withLeadingSlash.length === 1) {
    return "/";
  }
  return withLeadingSlash.replace(/\/+$/, "");
}

function splitPath(pathname: string): string[] {
  return pathname === "/"
    ? []
    : pathname.slice(1).split("/");
}

function createSearchParams(input: SearchParamsInput): URLSearchParams {
  if (
    typeof input === "string" ||
    input instanceof URLSearchParams ||
    Array.isArray(input)
  ) {
    return new URLSearchParams(
      input as string | URLSearchParams | string[][],
    );
  }
  const params = new URLSearchParams();
  for (const [key, value] of Object.entries(input)) {
    if (typeof value === "string") {
      params.set(key, value);
    } else {
      value.forEach((item) => params.append(key, item));
    }
  }
  return params;
}
