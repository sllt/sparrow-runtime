import { lazy, Suspense } from "react";
import { BrowserRouter, Navigate, Route, Routes } from "react-router-dom";
import { AuthProvider, useAuth } from "./auth/AuthContext";
import { LoginPage } from "./features/login/LoginPage";
import { Shell } from "./components/Shell";
import { Skeleton } from "./components/ui";

const Dashboard = lazy(() => import("./features/dashboard/DashboardPage"));
const PipelineList = lazy(() => import("./features/pipelines/PipelineListPage"));
const PipelineDetail = lazy(() => import("./features/pipelines/PipelineDetailPage"));
const Audit = lazy(() => import("./features/audit/AuditPage"));
const Instance = lazy(() => import("./features/instance/InstancePage"));

function Routed() {
  const { me } = useAuth();
  if (!me) return <LoginPage />;
  return (
    <Shell>
      <Suspense fallback={<Skeleton rows={6} />}>
        <Routes>
          <Route path="/" element={<Dashboard />} />
          <Route path="/pipelines" element={<PipelineList />} />
          <Route path="/pipelines/:name" element={<PipelineDetail />} />
          <Route path="/audit" element={<Audit />} />
          <Route path="/instance" element={<Instance />} />
          <Route path="*" element={<Navigate to="/" replace />} />
        </Routes>
      </Suspense>
    </Shell>
  );
}

export function App() {
  return (
    <AuthProvider>
      <BrowserRouter basename="/ui">
        <Routed />
      </BrowserRouter>
    </AuthProvider>
  );
}
