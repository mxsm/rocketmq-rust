import React from 'react';
import { ChevronLeft, ChevronRight } from 'lucide-react';
import { Button } from '../ui/button';

interface CursorPaginationProps {
    page: number;
    hasPrevious: boolean;
    hasNext: boolean;
    pending?: boolean;
    onPrevious: () => void;
    onNext: () => void;
}

export function CursorPagination({
    page, hasPrevious, hasNext, pending = false, onPrevious, onNext,
}: CursorPaginationProps) {
    return (
        <nav className="ops-cursor-pagination" aria-label="Result pages">
            <Button variant="outline" disabled={pending || !hasPrevious} onClick={onPrevious}>
                <ChevronLeft aria-hidden="true" />Previous
            </Button>
            <span aria-current="page">Page {page}</span>
            <Button variant="outline" disabled={pending || !hasNext} onClick={onNext}>
                Next<ChevronRight aria-hidden="true" />
            </Button>
        </nav>
    );
}
