import React, { type ReactNode } from 'react'
import { Icon } from '@iconify/react'

import type { PublicMetrics } from '../api'
import type { Translations } from '../i18n'
import RollingNumber from './RollingNumber'
import { Button } from './ui/button'

export interface PublicHomeHeroCardProps {
  publicStrings: Translations['public']
  loading: boolean
  metrics: PublicMetrics | null
  availableKeys: number | null
  totalKeys: number | null
  error: string | null
  showLinuxDoLogin: boolean
  showRegistrationPausedNotice?: boolean
  showTokenAccessButton: boolean
  showAdminAction: boolean
  adminActionLabel: string
  topControls?: ReactNode
  linuxDoHref?: string
  onLinuxDoLogin?: () => void
  onTokenAccessClick?: () => void
  onAdminActionClick?: () => void
}

const heroSecondaryButtonClassName =
  'h-auto rounded-full border-foreground/20 bg-card/95 px-4 py-[0.72rem] text-foreground no-underline shadow-[0_10px_20px_-18px_hsl(var(--foreground)/0.5)] hover:-translate-y-[1px] hover:border-primary/50 hover:bg-card hover:text-foreground'

function PublicHomeHeroCard({
  publicStrings,
  loading,
  metrics,
  availableKeys,
  totalKeys,
  error,
  showLinuxDoLogin,
  showRegistrationPausedNotice = false,
  showTokenAccessButton,
  showAdminAction,
  adminActionLabel,
  topControls,
  linuxDoHref = '/auth/linuxdo',
  onLinuxDoLogin,
  onTokenAccessClick,
  onAdminActionClick,
}: PublicHomeHeroCardProps): JSX.Element {
  const shouldShowActions = showLinuxDoLogin || showTokenAccessButton || showAdminAction
  const linuxDoContent = (
    <>
      <img src="/linuxdo-logo.svg" alt={publicStrings.linuxDoLogin.logoAlt} width={20} height={20} />
      <span>{publicStrings.linuxDoLogin.button}</span>
    </>
  )

  return (
    <section className="surface public-home-hero">
      <div className="language-switcher-row">{topControls}</div>
      <h1 className="hero-title">{publicStrings.heroTitle}</h1>
      <p className="public-home-description">{publicStrings.heroDescription}</p>
      {error && <div className="surface error-banner" role="status">{error}</div>}
      {showRegistrationPausedNotice && (
        <div
          className="mt-4 rounded-3xl border border-amber-300/80 bg-amber-50/90 px-5 py-4 text-left text-amber-950 shadow-[0_12px_30px_-24px_rgba(180,83,9,0.9)]"
          role="status"
          aria-live="polite"
        >
          <div className="mb-1 inline-flex items-center rounded-full bg-amber-200/80 px-3 py-1 text-xs font-semibold uppercase tracking-[0.18em] text-amber-900">
            {publicStrings.registrationPaused.badge}
          </div>
          <div className="text-base font-semibold">{publicStrings.registrationPausedNotice.title}</div>
          <p className="mb-0 mt-1 text-sm leading-6 text-amber-900/80">
            {publicStrings.registrationPausedNotice.description}
          </p>
        </div>
      )}
      <div className="metrics-grid hero-metrics">
        <div className="metric-card">
          <h3>{publicStrings.metrics.monthly.title}</h3>
          <div className="metric-value">
            <RollingNumber value={loading ? null : metrics?.monthlySuccess ?? 0} />
          </div>
          <div className="metric-subtitle">{publicStrings.metrics.monthly.subtitle}</div>
        </div>
        <div className="metric-card">
          <h3>{publicStrings.metrics.daily.title}</h3>
          <div className="metric-value">
            <RollingNumber value={loading ? null : metrics?.dailySuccess ?? 0} />
          </div>
          <div className="metric-subtitle">{publicStrings.metrics.daily.subtitle}</div>
        </div>
        <div className="metric-card">
          <h3>{publicStrings.metrics.pool.title}</h3>
          <div className="metric-value">
            {loading ? '—' : availableKeys != null && totalKeys != null ? `${availableKeys}/${totalKeys}` : '—'}
          </div>
          <div className="metric-subtitle">{publicStrings.metrics.pool.subtitle}</div>
        </div>
      </div>
      {shouldShowActions && (
        <div className="public-home-actions">
          {showLinuxDoLogin && (
            onLinuxDoLogin
              ? (
                  <Button
                    type="button"
                    variant="outline"
                    className={`linuxdo-login-button ${heroSecondaryButtonClassName}`}
                    aria-label={publicStrings.linuxDoLogin.button}
                    onClick={onLinuxDoLogin}
                  >
                    {linuxDoContent}
                  </Button>
                )
              : (
                  <Button asChild variant="outline" className={`linuxdo-login-button ${heroSecondaryButtonClassName}`}>
                    <a href={linuxDoHref} aria-label={publicStrings.linuxDoLogin.button}>
                      {linuxDoContent}
                    </a>
                  </Button>
                )
          )}
          {showTokenAccessButton && (
            <Button
              type="button"
              variant="outline"
              className={`token-access-button ${heroSecondaryButtonClassName}`}
              onClick={onTokenAccessClick}
            >
              <Icon icon="mdi:key-outline" aria-hidden="true" className="token-access-icon" />
              <span>{publicStrings.tokenAccess.button}</span>
            </Button>
          )}
          {showAdminAction && (
            <Button
              type="button"
              className="public-home-admin-button h-auto rounded-full px-4 py-[0.72rem]"
              onClick={onAdminActionClick}
            >
              {adminActionLabel}
            </Button>
          )}
        </div>
      )}
    </section>
  )
}

export default PublicHomeHeroCard
