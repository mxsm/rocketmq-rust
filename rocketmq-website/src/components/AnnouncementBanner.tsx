/**
 * Announcement Banner - Similar to Docusaurus homepage announcement
 */

import React from 'react';
import Link from '@docusaurus/Link';
import {useColorMode} from '@docusaurus/theme-common';
import {translate} from '@docusaurus/Translate';

export default function AnnouncementBanner(): React.JSX.Element {
    const {colorMode} = useColorMode();
    const isDark = colorMode === 'dark';

    return (
        <div style={{
            background: isDark ? 'linear-gradient(90deg, #1e1b4b 0%, #1e293b 100%)' : 'linear-gradient(90deg, #f8f9fa 0%, #e9ecef 100%)',
            borderBottom: isDark ? '2px solid #dc2626' : '2px solid #dc2626',
            padding: '16px 24px',
            textAlign: 'center',
        }}>
            <div style={{
                maxWidth: '1200px',
                margin: '0 auto',
                display: 'flex',
                alignItems: 'center',
                justifyContent: 'center',
                gap: '12px',
                flexWrap: 'wrap',
            }}>
                <span style={{fontSize: '24px'}}>🎉</span>
                <span style={{
                    fontSize: '20px',
                    fontWeight: 700,
                    background: 'linear-gradient(135deg, #a855f7 0%, #06b6d4 50%, #f59e0b 100%)',
                    WebkitBackgroundClip: 'text',
                    WebkitTextFillColor: 'transparent',
                    backgroundClip: 'text',
                    letterSpacing: '-0.5px',
                }}>
          {translate({
              id: 'homepage.announcement.title',
              message: 'RocketMQ-Rust v0.7.0 is out!',
          })}
        </span>
                <span style={{fontSize: '24px'}}>🚀</span>
                <Link
                    to="/docs/release-notes"
                    style={{
                        marginLeft: '8px',
                        padding: '6px 16px',
                        background: isDark ? 'rgba(168, 85, 247, 0.2)' : 'rgba(168, 85, 247, 0.1)',
                        border: `1px solid ${isDark ? '#a855f7' : '#9333ea'}`,
                        borderRadius: '20px',
                        color: isDark ? '#c084fc' : '#7c3aed',
                        textDecoration: 'none',
                        fontSize: '14px',
                        fontWeight: 600,
                        transition: 'all 0.2s ease',
                    }}
                    onMouseEnter={(e) => {
                        e.currentTarget.style.background = isDark ? 'rgba(168, 85, 247, 0.3)' : 'rgba(168, 85, 247, 0.2)';
                        e.currentTarget.style.transform = 'translateY(-1px)';
                    }}
                    onMouseLeave={(e) => {
                        e.currentTarget.style.background = isDark ? 'rgba(168, 85, 247, 0.2)' : 'rgba(168, 85, 247, 0.1)';
                        e.currentTarget.style.transform = 'translateY(0)';
                    }}
                >
                    {translate({
                        id: 'homepage.announcement.link',
                        message: 'See what\'s new →',
                    })}
                </Link>
            </div>
        </div>
    );
}
