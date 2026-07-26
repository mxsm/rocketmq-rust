import { Navigate, RouterProvider, createBrowserRouter } from "react-router-dom";

import { AppLayout } from "./layouts/AppLayout";
import { ClusterDetailPage } from "./pages/ClusterDetailPage";
import { ClustersPage } from "./pages/ClustersPage";
import { CoverageMatrixPage } from "./pages/CoverageMatrixPage";
import { EvidenceWorkbenchPage } from "./pages/EvidenceWorkbenchPage";
import { OverviewPage } from "./pages/OverviewPage";
import { SystemStatusPage } from "./pages/SystemStatusPage";

const router = createBrowserRouter([
  {
    element: <AppLayout />,
    children: [
      { index: true, element: <OverviewPage /> },
      { path: "clusters", element: <ClustersPage /> },
      { path: "clusters/:clusterId", element: <ClusterDetailPage /> },
      { path: "evidence", element: <EvidenceWorkbenchPage /> },
      { path: "coverage", element: <CoverageMatrixPage /> },
      { path: "system", element: <SystemStatusPage /> },
      { path: "*", element: <Navigate replace to="/" /> },
    ],
  },
]);

export function App() {
  return (
    <RouterProvider
      future={{ v7_startTransition: true }}
      router={router}
    />
  );
}
