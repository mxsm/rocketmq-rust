export interface ApiResponse<T> {
  success: boolean;
  code: string;
  message: string;
  data?: T;
  details?: Record<string, unknown>;
}

export interface PageState<T> {
  loading: boolean;
  error: string | null;
  data: T | null;
}

export const initialPageState = <T>(): PageState<T> => ({
  loading: true,
  error: null,
  data: null
});
