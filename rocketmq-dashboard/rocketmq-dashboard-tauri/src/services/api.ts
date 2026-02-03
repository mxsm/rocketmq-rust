// Service wrapper for API calls
// You can use invoke() here if using Tauri, or fetch() for web

export const api = {
  get: async (endpoint: string) => {
    // Mock implementation
    console.log(`GET ${endpoint}`);
    return {};
  },
  post: async (endpoint: string, data: any) => {
    // Mock implementation
    console.log(`POST ${endpoint}`, data);
    return {};
  }
};
