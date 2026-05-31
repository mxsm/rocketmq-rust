import React from 'react';
import { ChevronLeft, ChevronRight } from 'lucide-react';

interface PaginationProps {
  currentPage: number;
  totalPages: number;
  onPageChange: (page: number) => void;
  className?: string;
}

export const Pagination: React.FC<PaginationProps> = ({ 
  currentPage, 
  totalPages, 
  onPageChange,
  className = ""
}) => {
  // Generate page numbers to display
  const getPageNumbers = () => {
    const pages = [];
    // Simple logic for now: show all pages if total is small, otherwise ... (user image shows 1, 2, 3, 4, 5)
    // We'll stick to a max of 5 pages for simplicity or implement a sliding window if needed.
    // Given the user image shows 1-5, let's assume we show a range around current page.
    
    // For this implementation, I'll show up to 5 pages.
    const maxVisible = 5;
    let start = Math.max(1, currentPage - Math.floor(maxVisible / 2));
    let end = Math.min(totalPages, start + maxVisible - 1);
    
    if (end - start + 1 < maxVisible) {
      start = Math.max(1, end - maxVisible + 1);
    }

    for (let i = start; i <= end; i++) {
      pages.push(i);
    }
    return pages;
  };

  return (
    <div className={`ops-pagination ${className}`}>
      <button
        onClick={() => onPageChange(currentPage - 1)}
        disabled={currentPage === 1}
        className="ops-page-button"
        aria-label="Previous page"
      >
        <ChevronLeft className="w-5 h-5" />
      </button>

      {getPageNumbers().map((page) => (
        <button
          key={page}
          onClick={() => onPageChange(page)}
          className={`ops-page-button ${currentPage === page ? 'is-active' : ''}`}
          aria-current={currentPage === page ? 'page' : undefined}
        >
          {page}
        </button>
      ))}

      <button
        onClick={() => onPageChange(currentPage + 1)}
        disabled={currentPage === totalPages}
        className="ops-page-button"
        aria-label="Next page"
      >
        <ChevronRight className="w-5 h-5" />
      </button>
    </div>
  );
};
