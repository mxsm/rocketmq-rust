import rocketImage from 'rocketmq-rust:asset/rocketmq-rust.png';
import { AuthLayout } from '../app/layout/AuthLayout';
import { ChangePasswordDialog, LoginForm } from '../features/auth';

export function Login() {
    return <><AuthLayout>
        <section className="ops-auth-brand" aria-label="RocketMQ-Rust Dashboard">
            <img src={rocketImage} alt="" width={64} height={64} />
            <h2>RocketMQ-Rust<br />Dashboard</h2>
            <p>Inspect broker health, follow messages and manage your local RocketMQ environments.</p>
        </section>
        <LoginForm />
    </AuthLayout><ChangePasswordDialog /></>;
}
