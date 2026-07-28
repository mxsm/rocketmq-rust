import { Suspense, lazy } from "react";
import { Navigate, RouterProvider, createBrowserRouter } from "react-router-dom";

import { AppLayout } from "./layouts/AppLayout";
import { ClusterDetailPage } from "./pages/ClusterDetailPage";
import { ClustersPage } from "./pages/ClustersPage";
import { CoverageMatrixPage } from "./pages/CoverageMatrixPage";
import { OverviewPage } from "./pages/OverviewPage";
import { SystemStatusPage } from "./pages/SystemStatusPage";

const AssetsPage = lazy(() =>
  import("./pages/AssetsTopologyPages").then((module) => ({
    default: module.AssetsPage,
  })),
);
const OnboardingPage = lazy(() =>
  import("./pages/AssetsTopologyPages").then((module) => ({
    default: module.OnboardingPage,
  })),
);
const TopologyPage = lazy(() =>
  import("./pages/AssetsTopologyPages").then((module) => ({
    default: module.TopologyPage,
  })),
);
const EvidenceExplorerPage = lazy(() =>
  import("./pages/InsightsPages").then((module) => ({
    default: module.EvidenceExplorerPage,
  })),
);
const KnowledgePage = lazy(() =>
  import("./pages/InsightsPages").then((module) => ({
    default: module.KnowledgePage,
  })),
);
const MessageJourneyPage = lazy(() =>
  import("./pages/InsightsPages").then((module) => ({
    default: module.MessageJourneyPage,
  })),
);
const ModelsPage = lazy(() =>
  import("./pages/InsightsPages").then((module) => ({
    default: module.ModelsPage,
  })),
);
const AskSrePage = lazy(() =>
  import("./pages/WorkflowPages").then((module) => ({
    default: module.AskSrePage,
  })),
);
const ConversationDetailPage = lazy(() =>
  import("./pages/WorkflowPages").then((module) => ({
    default: module.ConversationDetailPage,
  })),
);
const IncidentDetailPage = lazy(() =>
  import("./pages/WorkflowPages").then((module) => ({
    default: module.IncidentDetailPage,
  })),
);
const IncidentsPage = lazy(() =>
  import("./pages/WorkflowPages").then((module) => ({
    default: module.IncidentsPage,
  })),
);
const InspectionDetailPage = lazy(() =>
  import("./pages/WorkflowPages").then((module) => ({
    default: module.InspectionDetailPage,
  })),
);
const InspectionsPage = lazy(() =>
  import("./pages/WorkflowPages").then((module) => ({
    default: module.InspectionsPage,
  })),
);
const InvestigationDetailPage = lazy(() =>
  import("./pages/WorkflowPages").then((module) => ({
    default: module.InvestigationDetailPage,
  })),
);
const ForecastPage = lazy(() =>
  import("./pages/ForecastPage").then((module) => ({
    default: module.ForecastPage,
  })),
);
const IncidentPostmortemPage = lazy(() =>
  import("./pages/PostmortemPages").then((module) => ({
    default: module.IncidentPostmortemPage,
  })),
);
const PostmortemDetailPage = lazy(() =>
  import("./pages/PostmortemPages").then((module) => ({
    default: module.PostmortemDetailPage,
  })),
);
const ActionItemsPage = lazy(() =>
  import("./pages/PostmortemPages").then((module) => ({
    default: module.ActionItemsPage,
  })),
);
const OperationsPage = lazy(() =>
  import("./pages/OperationsPage").then((module) => ({
    default: module.OperationsPage,
  })),
);
const ChangeCenterPage = lazy(() =>
  import("./pages/SupervisedExecutionPages").then((module) => ({
    default: module.ChangeCenterPage,
  })),
);
const PlanPage = lazy(() =>
  import("./pages/SupervisedExecutionPages").then((module) => ({
    default: module.PlanPage,
  })),
);
const ExecutionPage = lazy(() =>
  import("./pages/SupervisedExecutionPages").then((module) => ({
    default: module.ExecutionPage,
  })),
);
const AuditPageView = lazy(() =>
  import("./pages/SupervisedExecutionPages").then((module) => ({
    default: module.AuditPageView,
  })),
);
const QuarantinePageView = lazy(() =>
  import("./pages/SupervisedExecutionPages").then((module) => ({
    default: module.QuarantinePageView,
  })),
);

function RouteLoading() {
  return (
    <div className="page">
      <div className="state-panel" role="status">
        正在加载 SRE 工作区…
      </div>
    </div>
  );
}

const router = createBrowserRouter([
  {
    element: (
      <Suspense fallback={<RouteLoading />}>
        <AppLayout />
      </Suspense>
    ),
    children: [
      { index: true, element: <OverviewPage /> },
      { path: "clusters", element: <ClustersPage /> },
      { path: "clusters/onboard", element: <OnboardingPage /> },
      { path: "clusters/:clusterId", element: <ClusterDetailPage /> },
      { path: "assets", element: <AssetsPage /> },
      { path: "topology", element: <TopologyPage /> },
      { path: "ask", element: <AskSrePage /> },
      {
        path: "conversations/:conversationId",
        element: <ConversationDetailPage />,
      },
      {
        path: "investigations/:investigationId",
        element: <InvestigationDetailPage />,
      },
      { path: "incidents", element: <IncidentsPage /> },
      { path: "incidents/:incidentId", element: <IncidentDetailPage /> },
      {
        path: "incidents/:incidentId/postmortem",
        element: <IncidentPostmortemPage />,
      },
      {
        path: "postmortems/:postmortemId",
        element: <PostmortemDetailPage />,
      },
      { path: "action-items", element: <ActionItemsPage /> },
      { path: "operations", element: <OperationsPage /> },
      { path: "changes", element: <ChangeCenterPage /> },
      { path: "changes/plans/:planId", element: <PlanPage /> },
      {
        path: "changes/executions/:executionId",
        element: <ExecutionPage />,
      },
      {
        path: "changes/audit/:correlationId",
        element: <AuditPageView />,
      },
      { path: "changes/quarantines", element: <QuarantinePageView /> },
      { path: "inspections", element: <InspectionsPage /> },
      { path: "forecasts", element: <ForecastPage /> },
      {
        path: "inspections/:inspectionId",
        element: <InspectionDetailPage />,
      },
      { path: "evidence", element: <EvidenceExplorerPage /> },
      { path: "journeys", element: <MessageJourneyPage /> },
      { path: "coverage", element: <CoverageMatrixPage /> },
      { path: "knowledge", element: <KnowledgePage /> },
      { path: "models", element: <ModelsPage /> },
      { path: "system", element: <SystemStatusPage /> },
      { path: "*", element: <Navigate replace to="/" /> },
    ],
  },
]);

export function App() {
  return <RouterProvider router={router} />;
}
