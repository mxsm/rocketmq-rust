import { AuthLayout } from '../app/layout/AuthLayout';
import { ChangePasswordDialog, LoginForm } from '../features/auth';
import { LoginHero } from './LoginHero';

export function Login() {
    return (
        <>
            <AuthLayout>
                <LoginHero />
                <LoginForm />
            </AuthLayout>
            <ChangePasswordDialog />
        </>
    );
}
