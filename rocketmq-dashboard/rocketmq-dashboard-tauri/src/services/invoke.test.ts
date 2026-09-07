import { describe, expect, it } from 'vitest';
import { DashboardClientError, dashboardErrorMessage } from './invoke';

describe('dashboardErrorMessage', () => {
    const fallback = 'The operation could not be completed.';

    it('does not expose ordinary Error messages', () => {
        expect(dashboardErrorMessage(new Error('secret filesystem detail'), fallback)).toBe(fallback);
    });

    it('does not expose message properties from unknown objects', () => {
        expect(dashboardErrorMessage({ message: 'secret transport detail' }, fallback)).toBe(fallback);
    });

    it('keeps messages from structured dashboard errors', () => {
        const error = new DashboardClientError({
            code: 'dashboard.invalid_argument',
            message: 'The request is invalid.',
            category: 'validation',
            retryable: false,
        });

        expect(dashboardErrorMessage(error, fallback)).toBe('The request is invalid.');
    });
});
