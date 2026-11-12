import {
  createContext,
  type PropsWithChildren,
  useContext,
  useEffect,
  useMemo,
  useState,
} from "react";

export type OperatorLocale = "zh-CN" | "en-US";
export type OperatorTheme = "dark" | "light";
export type OperatorTimeZone = "Asia/Shanghai" | "Asia/Singapore" | "UTC";

interface OperatorPreferencesValue {
  locale: OperatorLocale;
  timeZone: OperatorTimeZone;
  theme: OperatorTheme;
  setLocale: (locale: OperatorLocale) => void;
  setTimeZone: (timeZone: OperatorTimeZone) => void;
  setTheme: (theme: OperatorTheme) => void;
  formatDateTime: (value: Date | string) => string;
}

const STORAGE_KEY = "rocketmq-sre-operator-preferences.v1";
const DEFAULT_PREFERENCES = {
  locale: "zh-CN" as OperatorLocale,
  timeZone: "Asia/Shanghai" as OperatorTimeZone,
  theme: "dark" as OperatorTheme,
};

const OperatorPreferencesContext = createContext<
  OperatorPreferencesValue | undefined
>(undefined);

export function OperatorPreferencesProvider({ children }: PropsWithChildren) {
  const initial = useMemo(readPreferences, []);
  const [locale, setLocaleState] = useState<OperatorLocale>(initial.locale);
  const [timeZone, setTimeZoneState] = useState<OperatorTimeZone>(
    initial.timeZone,
  );
  const [theme, setThemeState] = useState<OperatorTheme>(initial.theme);
  useEffect(() => {
    document.documentElement.dataset.theme = theme;
    return () => {
      delete document.documentElement.dataset.theme;
    };
  }, [theme]);
  const value = useMemo<OperatorPreferencesValue>(
    () => ({
      locale,
      timeZone,
      theme,
      setLocale: (next) => {
        setLocaleState(next);
        writePreferences(next, timeZone, theme);
      },
      setTimeZone: (next) => {
        setTimeZoneState(next);
        writePreferences(locale, next, theme);
      },
      setTheme: (next) => {
        setThemeState(next);
        writePreferences(locale, timeZone, next);
      },
      formatDateTime: (input) =>
        new Date(input).toLocaleString(locale, {
          hour12: false,
          timeZone,
        }),
    }),
    [locale, theme, timeZone],
  );

  return (
    <OperatorPreferencesContext.Provider value={value}>
      {children}
    </OperatorPreferencesContext.Provider>
  );
}

export function useOperatorPreferences() {
  const value = useContext(OperatorPreferencesContext);
  if (!value) {
    throw new Error(
      "useOperatorPreferences must be used within OperatorPreferencesProvider",
    );
  }
  return value;
}

function readPreferences() {
  try {
    const stored = JSON.parse(
      window.localStorage.getItem(STORAGE_KEY) ?? "{}",
    ) as {
      locale?: string;
      timeZone?: string;
      theme?: string;
    };
    return {
      locale: isLocale(stored.locale)
        ? stored.locale
        : DEFAULT_PREFERENCES.locale,
      timeZone: isTimeZone(stored.timeZone)
        ? stored.timeZone
        : DEFAULT_PREFERENCES.timeZone,
      theme: stored.theme === "light" ? ("light" as const) : ("dark" as const),
    };
  } catch {
    return DEFAULT_PREFERENCES;
  }
}

function writePreferences(
  locale: OperatorLocale,
  timeZone: OperatorTimeZone,
  theme: OperatorTheme,
) {
  try {
    window.localStorage.setItem(
      STORAGE_KEY,
      JSON.stringify({ locale, timeZone, theme }),
    );
  } catch {
    // Restricted browser storage must not prevent in-session preferences.
  }
}

function isLocale(value: string | undefined): value is OperatorLocale {
  return value === "zh-CN" || value === "en-US";
}

function isTimeZone(value: string | undefined): value is OperatorTimeZone {
  return (
    value === "Asia/Shanghai" || value === "Asia/Singapore" || value === "UTC"
  );
}
