import type { ApiKeyStats, AuthToken, JobLogView, RequestLog } from '../api'
import type { AdminModuleId } from './routes'

export interface DashboardMetricCard {
  id: string
  label: string
  value: string
  subtitle: string
}

export interface DashboardOverviewStrings {
  title: string
  description: string
  loading: string
  trendsTitle: string
  trendsDescription: string
  requestTrend: string
  errorTrend: string
  riskTitle: string
  riskDescription: string
  riskEmpty: string
  actionsTitle: string
  actionsDescription: string
  recentRequests: string
  recentJobs: string
  openModule: string
  openToken: string
  openKey: string
  disabledTokenRisk: string
  exhaustedKeyRisk: string
  failedJobRisk: string
  tokenCoverageTruncated: string
  tokenCoverageError: string
}

interface DashboardOverviewProps {
  strings: DashboardOverviewStrings
  overviewReady: boolean
  metrics: DashboardMetricCard[]
  trend: {
    request: number[]
    error: number[]
  }
  tokenCoverage: 'ok' | 'truncated' | 'error'
  tokens: AuthToken[]
  keys: ApiKeyStats[]
  logs: RequestLog[]
  jobs: JobLogView[]
  onOpenModule: (module: AdminModuleId) => void
  onOpenToken: (id: string) => void
  onOpenKey: (id: string) => void
}

function toPoints(values: number[]): string {
  if (values.length === 0) return ''
  const max = Math.max(...values, 1)
  const min = Math.min(...values, 0)
  const width = 220
  const height = 64
  return values
    .map((value, index) => {
      const x = values.length === 1 ? 0 : (index / (values.length - 1)) * width
      const normalized = (value - min) / Math.max(max - min, 1)
      const y = height - normalized * height
      return `${x.toFixed(2)},${y.toFixed(2)}`
    })
    .join(' ')
}

function Sparkline({ values }: { values: number[] }): JSX.Element {
  const points = toPoints(values)
  return (
    <svg viewBox="0 0 220 64" className="dashboard-sparkline" aria-hidden="true" preserveAspectRatio="none">
      <polyline points={points} fill="none" stroke="currentColor" strokeWidth="2.5" strokeLinecap="round" />
    </svg>
  )
}

export default function DashboardOverview({
  strings,
  overviewReady,
  metrics,
  trend,
  tokenCoverage,
  tokens,
  keys,
  logs,
  jobs,
  onOpenModule,
  onOpenToken,
  onOpenKey,
}: DashboardOverviewProps): JSX.Element {
  const disabledTokens = tokens.filter((item) => !item.enabled).slice(0, 5)
  const exhaustedKeys = keys.filter((item) => item.status === 'exhausted').slice(0, 5)
  const failingJobs = jobs
    .filter((item) => {
      const normalized = item.status.trim().toLowerCase()
      return normalized === 'error' || normalized === 'failed'
    })
    .slice(0, 5)

  const riskItems: Array<{ id: string; label: string; action?: () => void }> = []
  if (tokenCoverage === 'truncated') {
    riskItems.push({
      id: 'token-coverage-truncated',
      label: strings.tokenCoverageTruncated,
      action: () => onOpenModule('tokens'),
    })
  }
  if (tokenCoverage === 'error') {
    riskItems.push({
      id: 'token-coverage-error',
      label: strings.tokenCoverageError,
      action: () => onOpenModule('tokens'),
    })
  }
  for (const token of disabledTokens) {
    riskItems.push({
      id: `token-${token.id}`,
      label: strings.disabledTokenRisk.replace('{id}', token.id),
      action: () => onOpenToken(token.id),
    })
  }
  for (const key of exhaustedKeys) {
    riskItems.push({
      id: `key-${key.id}`,
      label: strings.exhaustedKeyRisk.replace('{id}', key.id),
      action: () => onOpenKey(key.id),
    })
  }
  for (const job of failingJobs) {
    riskItems.push({
      id: `job-${job.id}`,
      label: strings.failedJobRisk.replace('{id}', String(job.id)).replace('{status}', job.status),
      action: () => onOpenModule('jobs'),
    })
  }

  return (
    <>
      <section className="surface panel dashboard-hero-panel">
        <div className="panel-header">
          <div>
            <h2>{strings.title}</h2>
            <p className="panel-description">{strings.description}</p>
          </div>
          <button type="button" className="btn btn-outline" onClick={() => onOpenModule('tokens')}>
            {strings.openModule}
          </button>
        </div>
      </section>

      <section className="surface quick-stats-grid">
        {metrics.length === 0 ? (
          <div className="empty-state alert">{strings.loading}</div>
        ) : (
          metrics.map((metric) => (
            <div key={metric.id} className="metric-card quick-stats-card">
              <h3>{metric.label}</h3>
              <div className="metric-value">{metric.value}</div>
              <div className="metric-subtitle">{metric.subtitle}</div>
            </div>
          ))
        )}
      </section>

      <section className="surface panel dashboard-trend-panel">
        <div className="panel-header">
          <div>
            <h2>{strings.trendsTitle}</h2>
            <p className="panel-description">{strings.trendsDescription}</p>
          </div>
        </div>
        <div className="dashboard-trend-grid">
          <article className="dashboard-trend-card">
            <header>{strings.requestTrend}</header>
            <Sparkline values={trend.request} />
          </article>
          <article className="dashboard-trend-card">
            <header>{strings.errorTrend}</header>
            <Sparkline values={trend.error} />
          </article>
        </div>
      </section>

      <section className="surface panel">
        <div className="panel-header">
          <div>
            <h2>{strings.riskTitle}</h2>
            <p className="panel-description">{strings.riskDescription}</p>
          </div>
        </div>
        {!overviewReady ? (
          <div className="empty-state alert">{strings.loading}</div>
        ) : riskItems.length === 0 ? (
          <div className="empty-state alert">{strings.riskEmpty}</div>
        ) : (
          <ul className="dashboard-risk-list">
            {riskItems.map((item) => (
              <li key={item.id}>
                <span>{item.label}</span>
                {item.action && (
                  <button type="button" className="btn btn-ghost btn-sm" onClick={item.action}>
                    {strings.openModule}
                  </button>
                )}
              </li>
            ))}
          </ul>
        )}
      </section>

      <section className="surface panel">
        <div className="panel-header">
          <div>
            <h2>{strings.actionsTitle}</h2>
            <p className="panel-description">{strings.actionsDescription}</p>
          </div>
        </div>
        <div className="dashboard-actions-grid">
          <article className="dashboard-actions-card">
            <h3>{strings.recentRequests}</h3>
            <ul>
              {logs.slice(0, 5).map((log) => (
                <li key={log.id}>
                  <code>{log.key_id}</code>
                  <span>{log.result_status}</span>
                </li>
              ))}
            </ul>
          </article>
          <article className="dashboard-actions-card">
            <h3>{strings.recentJobs}</h3>
            <ul>
              {jobs.slice(0, 5).map((job) => (
                <li key={job.id}>
                  <span>#{job.id}</span>
                  <span>{job.status}</span>
                </li>
              ))}
            </ul>
          </article>
        </div>
      </section>
    </>
  )
}
