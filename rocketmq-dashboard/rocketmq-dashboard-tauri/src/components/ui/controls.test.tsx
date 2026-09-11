import React from 'react';
import { renderToStaticMarkup as render } from 'react-dom/server';
import { describe, expect, it } from 'vitest';
import { Button } from './button';
import { Button as LegacyButton } from './LegacyButton';
import { Input } from './LegacyInput';
import { Toggle } from './LegacyToggle';
import { Pagination } from '../Pagination';
import { CursorPagination } from '../layout/CursorPagination';
import { PageState } from '../layout/PageState';

describe('shared control semantics', () => {
    it('makes ordinary actions safe inside forms while preserving explicit submit buttons', () => {
        expect(render(<Button>Inspect</Button>)).toContain('type="button"');
        expect(render(<LegacyButton>Inspect</LegacyButton>)).toContain('type="button"');
        expect(render(<LegacyButton type="submit" disabled aria-label="Save changes">Save</LegacyButton>))
            .toMatch(/disabled="".*aria-label="Save changes".*type="submit"/);
    });

    it('preserves link semantics for slotted navigation', () => {
        const html = render(<Button asChild><a href="#details">Details</a></Button>);
        expect(html).toContain('href="#details"');
        expect(html).not.toContain('type="button"');
    });

    it('associates visible labels, existing hints, and validation errors with the input', () => {
        const html = render(<Input id="broker-name" label="Broker" error="Name is required" aria-describedby="broker-hint" />);
        expect(html).toContain('for="broker-name"');
        expect(html).toContain('id="broker-name"');
        expect(html).toContain('aria-invalid="true"');
        expect(html).toContain('aria-describedby="broker-hint broker-name-error"');
        expect(html).toContain('id="broker-name-error"');
        expect(html).toContain('role="alert"');
    });

    it('generates a label association when no input id is supplied', () => {
        const html = render(<Input label="Topic" />);
        const id = html.match(/<input[^>]* id="([^"]+)"/)?.[1];
        expect(id).toBeTruthy();
        expect(html).toContain(`for="${id}"`);
        expect(html).not.toContain('aria-invalid');
    });

    it('honors native invalid state and hint without inventing an error', () => {
        const html = render(<Input aria-invalid="true" aria-describedby="server-error" />);
        expect(html).toContain('aria-invalid="true"');
        expect(html).toContain('aria-describedby="server-error"');
        expect(html).not.toContain('role="alert"');
    });

    it('exposes a labeled disabled switch to assistive technology', () => {
        const html = render(<Toggle id="tls" label="TLS" checked disabled onChange={() => {}} />);
        expect(html).toContain('for="tls"');
        expect(html).toContain('aria-checked="true"');
        expect(html).toContain('disabled=""');
        expect(html).toContain('role="switch"');
        expect(html).toContain('type="button"');
    });

    it('supports an external accessible switch name', () => {
        expect(render(<Toggle aria-label="VIP channel" checked={false} onChange={() => {}} />))
            .toContain('aria-label="VIP channel"');
    });
});

describe('pagination boundaries', () => {
    it.each([0, -1, Number.NaN])('disables both directions for an empty or invalid total %s', totalPages => {
        const html = render(<Pagination currentPage={1} totalPages={totalPages} onPageChange={() => {}} />);
        expect(html.match(/disabled=""/g)).toHaveLength(2);
        expect(html).not.toContain('aria-current="page"');
    });

    it('does not offer navigation from a stale page after a result count shrinks', () => {
        const html = render(<Pagination currentPage={8} totalPages={2} onPageChange={() => {}} />);
        expect(html.match(/disabled=""/g)).toHaveLength(2);
    });

    it('keeps the final page window bounded', () => {
        const html = render(<Pagination currentPage={20} totalPages={20} onPageChange={() => {}} />);
        expect(html).toContain('aria-label="Page 16"');
        expect(html).toContain('aria-label="Page 20"');
        expect(html).not.toContain('aria-label="Page 21"');
        expect(html.match(/disabled=""/g)).toHaveLength(1);
        expect(html.match(/aria-current="page"/g)).toHaveLength(1);
    });

    it('locks cursor controls during a request without presenting a total', () => {
        const html = render(<CursorPagination page={3} hasPrevious hasNext pending onPrevious={() => {}} onNext={() => {}} />);
        expect(html.match(/disabled=""/g)).toHaveLength(2);
        expect(html).toContain('Page 3');
        expect(html).not.toMatch(/of \d|total/i);
    });

    it('shows the terminal cursor boundary independently of the page number', () => {
        const html = render(<CursorPagination page={3} hasPrevious hasNext={false} onPrevious={() => {}} onNext={() => {}} />);
        expect(html.match(/disabled=""/g)).toHaveLength(1);
    });
});

describe('query feedback', () => {
    it('announces failures with their specific reason and a retry action', () => {
        const html = render(<PageState kind="error" title="Broker unavailable" description="Connection timed out."
            action={<Button>Retry</Button>} />);
        expect(html).toContain('role="alert"');
        expect(html).toContain('Connection timed out.');
        expect(html).toContain('Retry');
        expect(html).not.toContain('0 results');
    });

    it('retains the partial-data reason without escalating it to a failure alert', () => {
        const html = render(<PageState kind="partial" title="Partial data" description="One Broker did not respond." />);
        expect(html).toContain('role="status"');
        expect(html).toContain('One Broker did not respond.');
        expect(html).not.toContain('role="alert"');
    });
});
