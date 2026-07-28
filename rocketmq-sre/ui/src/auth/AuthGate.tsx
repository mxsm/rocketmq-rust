import { KeyRound, LoaderCircle, ShieldX } from "lucide-react";
import type { PropsWithChildren } from "react";

import { Button } from "@/components/ui/button";

import { useAuth } from "@/auth/AuthContext";

export function AuthGate({ children }: PropsWithChildren) {
  const { status, mode, error, signIn } = useAuth();

  if (status === "authenticated") {
    return children;
  }

  return (
    <main className="auth-shell">
      <section className="auth-card">
        {status === "loading" ? (
          <>
            <LoaderCircle className="spin" size={28} />
            <h1>正在校验 SRE 会话</h1>
            <p>加载租户与集群只读范围。</p>
          </>
        ) : status === "anonymous" ? (
          <>
            <KeyRound size={28} />
            <h1>需要 OIDC 登录</h1>
            <p>登录后仅展示 token claim 允许访问的 RocketMQ 集群。</p>
            <Button onClick={() => void signIn()}>使用企业身份登录</Button>
          </>
        ) : (
          <>
            <ShieldX size={28} />
            <h1>无法建立安全会话</h1>
            <p>{error ?? "认证配置不可用。"}</p>
            {mode === "oidc" && (
              <Button onClick={() => void signIn()} variant="outline">
                重新登录
              </Button>
            )}
          </>
        )}
        <code>read_only · cluster scoped</code>
      </section>
    </main>
  );
}
