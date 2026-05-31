import React from 'react';

interface CardProps {
  children: React.ReactNode;
  className?: string;
  title?: string;
  description?: string;
  headerAction?: React.ReactNode;
}

export const Card = ({ children, className = "", title, description, headerAction }: CardProps) => (
  <div className={`ops-card ${className}`}>
    {(title || description) && (
      <div className="ops-card-header">
        <div>
          {title && <h3 className="ops-card-title">{title}</h3>}
          {description && <p className="ops-card-description">{description}</p>}
        </div>
        {headerAction}
      </div>
    )}
    {children}
  </div>
);
