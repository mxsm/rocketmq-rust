let available: boolean | null = null;

function probe() {
  if (typeof window === 'undefined' || typeof window.WebGLRenderingContext === 'undefined') return false;
  try {
    const canvas = document.createElement('canvas');
    const context = canvas.getContext('webgl2') ?? canvas.getContext('webgl');
    if (!context) return false;
    context.getExtension('WEBGL_lose_context')?.loseContext();
    return true;
  } catch {
    return false;
  }
}

/** Whether this browser can create a WebGL context. Probed once; the probe context is released at once. */
export function supportsWebGL(): boolean {
  available ??= probe();
  return available;
}
