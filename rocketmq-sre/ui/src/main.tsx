import React from "react";
import ReactDOM from "react-dom/client";

import { App } from "./App";
import { SreDataProvider } from "./data/SreDataContext";
import "./styles/tokens.css";
import "./styles/app.css";

ReactDOM.createRoot(document.getElementById("root")!).render(
  <React.StrictMode>
    <SreDataProvider>
      <App />
    </SreDataProvider>
  </React.StrictMode>,
);
