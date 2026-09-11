import React from 'react';
import { ChevronLeft, ChevronRight } from 'lucide-react';
import { Button } from './ui/button';

interface PaginationProps {
    currentPage: number;
    totalPages: number;
    onPageChange: (page: number) => void;
    className?: string;
    disabled?: boolean;
}

export const Pagination = ({
    currentPage, totalPages, onPageChange, className = '', disabled = false,
}: PaginationProps) => {
    const total = Number.isFinite(totalPages) ? Math.max(0, Math.floor(totalPages)) : 0;
    const validPage = Number.isInteger(currentPage) && currentPage >= 1 && currentPage <= total;
    const start = Math.max(1, Math.min(currentPage - 2, total - 4));
    const pages = validPage ? Array.from({ length: Math.min(5, total) }, (_, index) => start + index) : [];
    const selectPage = (page: number) => {
        if (!disabled && validPage && page >= 1 && page <= total && page !== currentPage) onPageChange(page);
    };
    return (
        <nav aria-label="Pagination" className={`ops-pagination ${className}`}>
            <Button variant="outline" size="icon" onClick={() => selectPage(currentPage - 1)}
                disabled={disabled || !validPage || currentPage <= 1} aria-label="Previous page">
                <ChevronLeft aria-hidden="true" />
            </Button>
            {pages.map(page => (
                <Button key={page} variant={page === currentPage ? 'default' : 'ghost'} size="icon"
                    disabled={disabled} aria-label={`Page ${page}`}
                    aria-current={page === currentPage ? 'page' : undefined}
                    onClick={() => selectPage(page)}>{page}</Button>
            ))}
            <Button variant="outline" size="icon" onClick={() => selectPage(currentPage + 1)}
                disabled={disabled || !validPage || currentPage >= total} aria-label="Next page">
                <ChevronRight aria-hidden="true" />
            </Button>
        </nav>
    );
};
