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
      <div className="min-h-screen bg-[#09090B] text-white flex items-center justify-center">
        <div className="text-center space-y-3">
          <div className="w-10 h-10 mx-auto rounded-full border-2 border-white/20 border-t-white animate-spin" />
          <p className="text-sm text-white/70 tracking-[0.2em] uppercase">Restoring Session</p>
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
