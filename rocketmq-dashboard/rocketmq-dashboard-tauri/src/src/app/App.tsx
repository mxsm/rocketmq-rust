import { AppProvider, useAppStore } from '../stores/app.store';
import { MainLayout } from './layout/MainLayout';
import { Login } from '../pages/Login';
import { AppRouter } from './router/AppRouter';

const AppContent = () => {
  const { isLoggedIn } = useAppStore();

  if (!isLoggedIn) {
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
