export interface ApiResponse<T> {
  success: boolean;
  code: string;
  message: string;
  data?: T;
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
