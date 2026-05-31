import { AppProvider, useAppStore } from '../stores/app.store';
import { MainLayout } from './layout/MainLayout';
import { Login } from '../pages/Login';
import { AppRouter } from './router/AppRouter';
import { useSessionBootstrap } from '../features/auth/hooks/useSessionBootstrap';

const AppContent = () => {
  const { isBootstrappingAuth, isLoggedIn, mustChangePassword } = useAppStore();
  useSessionBootstrap();

  if (isBootstrappingAuth) {
    return (
      <div className="auth-boot">
        <div className="auth-boot-panel">
          <div className="auth-boot-spinner" />
          <p>Restoring session</p>
        </div>
      </div>
    );
  }

  if (!isLoggedIn || mustChangePassword) {
    return <Login />;
  }

  return (
    <MainLayout>
       <AppRouter />
    </MainLayout>
  );
};

export default function App() {
  return (
    <AppProvider>
      <AppContent />
    </AppProvider>
  );
}
