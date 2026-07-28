import React from "react";
import ReactDOM from "react-dom/client";

import { App } from "./App";
import { AuthGate } from "@/auth/AuthGate";
import { AuthProvider } from "@/auth/AuthContext";
import { SreDataProvider } from "./data/SreDataContext";
import "./styles/tokens.css";
import "./styles/app.css";
import "./styles/change-management.css";
import "./styles/model-governance.css";
import "./styles/release-management.css";

ReactDOM.createRoot(document.getElementById("root")!).render(
  <React.StrictMode>
    <AuthProvider>
      <AuthGate>
        <SreDataProvider>
          <App />
        </SreDataProvider>
      </AuthGate>
    </AuthProvider>
  </React.StrictMode>,
);
