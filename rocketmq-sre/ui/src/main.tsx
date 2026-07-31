import React from "react";
import ReactDOM from "react-dom/client";

import { App } from "./App";
import { AuthGate } from "@/auth/AuthGate";
import { AuthProvider } from "@/auth/AuthContext";
import { SreDataProvider } from "./data/SreDataContext";
import { OperatorPreferencesProvider } from "./preferences/OperatorPreferences";
import "./styles/tokens.css";
import "./styles/app.css";
import "./styles/autonomy-operations.css";
import "./styles/change-management.css";
import "./styles/enterprise-operations.css";
import "./styles/model-governance.css";
import "./styles/release-management.css";

ReactDOM.createRoot(document.getElementById("root")!).render(
  <React.StrictMode>
    <OperatorPreferencesProvider>
      <AuthProvider>
        <AuthGate>
          <SreDataProvider>
            <App />
          </SreDataProvider>
        </AuthGate>
      </AuthProvider>
    </OperatorPreferencesProvider>
  </React.StrictMode>,
);
