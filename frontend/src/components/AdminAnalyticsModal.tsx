import { useEffect, useMemo, useState } from "react";

import {
  getAdminUsageFeedback,
  getAdminUsageFeedbackInbox,
  getAdminUsageFunnel,
  getAdminUsageInsights,
  getAdminUsageSummary,
  getAdminUsageUser,
  getAdminUsageUsers,
  type AdminUsageFunnelResponse,
  type AdminUsageInsights,
  type AdminUsageFeedbackResponse,
  type AdminUsageFeedbackInbox,
  type FeedbackItem,
  type AdminUsageSummary,
  type AdminUsageUser,
  type AdminUsageUserDetail,
} from "../api";

type AdminAnalyticsModalProps = {
  isOpen: boolean;
  onClose: () => void;
};

const SINCE_OPTIONS = [
  { label: "1d", value: "1d", days: 1 },
  { label: "7d", value: "7d", days: 7 },
  { label: "30d", value: "30d", days: 30 },
  { label: "90d", value: "90d", days: 90 },
  { label: "All", value: "all", days: null },
] as const;

type SinceKey = (typeof SINCE_OPTIONS)[number]["value"];

function getSinceDate(value: SinceKey): string | undefined {
  const option = SINCE_OPTIONS.find((entry) => entry.value === value);
  if (!option || option.days == null) return undefined;
  const date = new Date();
  date.setDate(date.getDate() - option.days);
  return date.toISOString().slice(0, 10);
}

function formatNumber(value?: number | null): string {
  if (value == null || Number.isNaN(value)) return "—";
  return value.toLocaleString();
}

function formatPercent(value?: number | null): string {
  if (value == null || Number.isNaN(value)) return "—";
  return `${(value * 100).toFixed(1)}%`;
}

function formatMinutes(value?: number | null): string {
  if (value == null || Number.isNaN(value)) return "—";
  return `${value.toFixed(1)} min`;
}

function formatDateTime(value?: string | null): string {
  if (!value) return "—";
  const date = new Date(value);
  if (Number.isNaN(date.getTime())) return value;
  return date.toLocaleString();
}

function formatEvidenceValue(key: string, value: number | string | null | undefined): string {
  if (value == null) return "—";
  if (typeof value === "number") {
    if (key.includes("rate") || key.endsWith("_pct")) {
      return formatPercent(value);
    }
    return formatNumber(value);
  }
  return String(value);
}

const EVIDENCE_LABELS: Record<string, string> = {
  users_with_estimates: "Users w/ estimates",
  override_users: "Users overriding",
  override_rate: "Override rate",
  avg_delta_pct: "Avg delta",
  estimate_count: "Estimates",
  pdf_exports: "PDF exports",
  conversion_rate: "PDF conversion",
  repeated_error_users: "Users w/ 3+ 5xx",
  estimate_failures: "Estimate failures",
  top_5xx_path_count: "Endpoints w/ 5xx",
  total_estimate_results: "Estimate results",
  suhail_overlay_count: "Suhail fallbacks",
  suhail_overlay_pct: "Suhail fallback rate",
  feedback_up_count: "Thumbs up",
  feedback_down_count: "Thumbs down",
  feedback_down_rate: "Thumbs-down rate",
};

const EVIDENCE_ORDER = [
  "users_with_estimates",
  "override_users",
  "override_rate",
  "avg_delta_pct",
  "estimate_count",
  "pdf_exports",
  "conversion_rate",
  "feedback_up_count",
  "feedback_down_count",
  "feedback_down_rate",
  "repeated_error_users",
  "estimate_failures",
  "top_5xx_path_count",
  "total_estimate_results",
  "suhail_overlay_count",
  "suhail_overlay_pct",
];

function getEvidenceLines(item: FeedbackItem) {
  const evidence = item.evidence ?? {};
  return EVIDENCE_ORDER.filter((key) => key in evidence)
    .map((key) => ({
      key,
      label: EVIDENCE_LABELS[key] ?? key,
      value: formatEvidenceValue(key, evidence[key]),
    }))
    .slice(0, 3);
}

export default function AdminAnalyticsModal({ isOpen, onClose }: AdminAnalyticsModalProps) {
  const [sinceKey, setSinceKey] = useState<SinceKey>("30d");
  const [summary, setSummary] = useState<AdminUsageSummary | null>(null);
  const [funnel, setFunnel] = useState<AdminUsageFunnelResponse | null>(null);
  const [insights, setInsights] = useState<AdminUsageInsights | null>(null);
  const [feedback, setFeedback] = useState<AdminUsageFeedbackResponse | null>(null);
  const [feedbackInbox, setFeedbackInbox] = useState<AdminUsageFeedbackInbox | null>(null);
  const [users, setUsers] = useState<AdminUsageUser[]>([]);
  const [selectedUserId, setSelectedUserId] = useState<string | null>(null);
  const [userDetail, setUserDetail] = useState<AdminUsageUserDetail | null>(null);
  const [summaryError, setSummaryError] = useState<string | null>(null);
  const [funnelError, setFunnelError] = useState<string | null>(null);
  const [insightsError, setInsightsError] = useState<string | null>(null);
  const [feedbackError, setFeedbackError] = useState<string | null>(null);
  const [feedbackInboxError, setFeedbackInboxError] = useState<string | null>(null);
  const [usersError, setUsersError] = useState<string | null>(null);
  const [detailError, setDetailError] = useState<string | null>(null);
  const [isSummaryLoading, setIsSummaryLoading] = useState(false);
  const [isFunnelLoading, setIsFunnelLoading] = useState(false);
  const [isInsightsLoading, setIsInsightsLoading] = useState(false);
  const [isFeedbackLoading, setIsFeedbackLoading] = useState(false);
  const [isFeedbackInboxLoading, setIsFeedbackInboxLoading] = useState(false);
  const [isUsersLoading, setIsUsersLoading] = useState(false);
  const [isDetailLoading, setIsDetailLoading] = useState(false);

  const since = useMemo(() => getSinceDate(sinceKey), [sinceKey]);

  useEffect(() => {
    if (!isOpen) {
      setSummary(null);
      setFunnel(null);
      setInsights(null);
      setFeedback(null);
      setFeedbackInbox(null);
      setUsers([]);
      setSelectedUserId(null);
      setUserDetail(null);
      setSummaryError(null);
      setFunnelError(null);
      setInsightsError(null);
      setFeedbackError(null);
      setFeedbackInboxError(null);
      setUsersError(null);
      setDetailError(null);
      setIsSummaryLoading(false);
      setIsFunnelLoading(false);
      setIsInsightsLoading(false);
      setIsFeedbackLoading(false);
      setIsFeedbackInboxLoading(false);
      setIsUsersLoading(false);
      setIsDetailLoading(false);
      return;
    }
    setSelectedUserId(null);
    setUserDetail(null);
  }, [isOpen, sinceKey]);

  useEffect(() => {
    if (!isOpen) return;
    let isActive = true;

    const load = async () => {
      setIsSummaryLoading(true);
      setIsUsersLoading(true);
      setSummaryError(null);
      setUsersError(null);
      try {
        const [summaryResponse, usersResponse] = await Promise.all([
          getAdminUsageSummary(since),
          getAdminUsageUsers(since),
        ]);
        if (!isActive) return;
        setSummary(summaryResponse);
        setUsers(usersResponse.items ?? []);
      } catch (error) {
        if (!isActive) return;
        setSummary(null);
        setUsers([]);
        const message = error instanceof Error ? error.message : "Admin endpoints unavailable";
        setSummaryError(message);
        setUsersError(message);
      } finally {
        if (!isActive) return;
        setIsSummaryLoading(false);
        setIsUsersLoading(false);
      }
    };

    void load();

    return () => {
      isActive = false;
    };
  }, [isOpen, since]);

  useEffect(() => {
    if (!isOpen) return;
    let isActive = true;

    const loadFeedback = async () => {
      setIsFeedbackLoading(true);
      setFeedbackError(null);
      setIsFeedbackInboxLoading(true);
      setFeedbackInboxError(null);
      try {
        const [response, inboxResponse] = await Promise.all([
          getAdminUsageFeedback(since),
          getAdminUsageFeedbackInbox(since),
        ]);
        if (!isActive) return;
        setFeedback(response);
        setFeedbackInbox(inboxResponse);
      } catch (error) {
        if (!isActive) return;
        setFeedback(null);
        setFeedbackInbox(null);
        const message = error instanceof Error ? error.message : "Feedback unavailable";
        setFeedbackError(message);
        setFeedbackInboxError(message);
      } finally {
        if (!isActive) return;
        setIsFeedbackLoading(false);
        setIsFeedbackInboxLoading(false);
      }
    };

    void loadFeedback();

    return () => {
      isActive = false;
    };
  }, [isOpen, since]);

  useEffect(() => {
    if (!isOpen) return;
    let isActive = true;

    const loadFunnel = async () => {
      setIsFunnelLoading(true);
      setFunnelError(null);
      try {
        const response = await getAdminUsageFunnel(since);
        if (!isActive) return;
        setFunnel(response);
      } catch (error) {
        if (!isActive) return;
        setFunnel(null);
        const message = error instanceof Error ? error.message : "Funnel unavailable";
        setFunnelError(message);
      } finally {
        if (!isActive) return;
        setIsFunnelLoading(false);
      }
    };

    void loadFunnel();

    return () => {
      isActive = false;
    };
  }, [isOpen, since]);

  useEffect(() => {
    if (!isOpen) return;
    let isActive = true;

    const loadInsights = async () => {
      setIsInsightsLoading(true);
      setInsightsError(null);
      try {
        const response = await getAdminUsageInsights(since);
        if (!isActive) return;
        setInsights(response);
      } catch (error) {
        if (!isActive) return;
        setInsights(null);
        const message = error instanceof Error ? error.message : "Insights unavailable";
        setInsightsError(message);
      } finally {
        if (!isActive) return;
        setIsInsightsLoading(false);
      }
    };

    void loadInsights();

    return () => {
      isActive = false;
    };
  }, [isOpen, since]);

  useEffect(() => {
    if (!isOpen || !selectedUserId) return;
    let isActive = true;

    const loadDetail = async () => {
      setIsDetailLoading(true);
      setDetailError(null);
      try {
        const response = await getAdminUsageUser(selectedUserId, since);
        if (!isActive) return;
        setUserDetail(response);
      } catch (error) {
        if (!isActive) return;
        setUserDetail(null);
        const message = error instanceof Error ? error.message : "Unable to load user detail";
        setDetailError(message);
      } finally {
        if (!isActive) return;
        setIsDetailLoading(false);
      }
    };

    void loadDetail();

    return () => {
      isActive = false;
    };
  }, [isOpen, selectedUserId, since]);

  if (!isOpen) return null;

  const totals = summary?.totals;
  const funnelTotals = funnel?.totals;
  const funnelEvents = funnel?.totals?.events;
  const funnelConversion = funnel?.conversion;
  const funnelTimeToValue = funnel?.time_to_value;
  const funnelSamples = funnel?.per_user_samples ?? [];
  const metrics = userDetail?.metrics;
  const topPaths = userDetail?.top_paths ?? [];
  const daily = userDetail?.daily ?? [];
  const highlights = insights?.highlights ?? [];
  const feedbackItems = feedback?.items ?? [];
  const feedbackTotals = feedbackInbox?.totals;
  const feedbackReasons = feedbackInbox?.top_reasons ?? [];
  const feedbackUsers = feedbackInbox?.by_user ?? [];

  return (
    <div className="admin-analytics-overlay" role="presentation">
      <div className="admin-analytics-modal" role="dialog" aria-modal="true" aria-labelledby="admin-analytics-title">
        <header className="admin-analytics-header">
          <h2 id="admin-analytics-title" className="admin-analytics-title">Admin analytics</h2>
          <div className="admin-analytics-controls">
            <label className="admin-analytics-filter" htmlFor="admin-analytics-since">
              Since
              <select
                id="admin-analytics-since"
                className="admin-analytics-select"
                value={sinceKey}
                onChange={(event) => setSinceKey(event.target.value as SinceKey)}
              >
                {SINCE_OPTIONS.map((option) => (
                  <option key={option.value} value={option.value}>
                    {option.label}
                  </option>
                ))}
              </select>
            </label>
            <button type="button" className="tertiary-button" onClick={onClose}>
              Close
            </button>
          </div>
        </header>
        <div className="admin-analytics-body">
          <section className="admin-analytics-section">
            <h3 className="section-heading">Funnel &amp; time-to-value</h3>
            {funnelError && <p className="admin-analytics-error">Funnel metrics unavailable.</p>}
            {isFunnelLoading && <p className="admin-analytics-loading">Loading funnel metrics...</p>}
            {!isFunnelLoading && !funnelError && (
              <>
                <div className="admin-analytics-split">
                  <div className="admin-analytics-panel">
                    <h4 className="admin-analytics-subtitle">Activation + value funnel</h4>
                    <dl className="metrics-grid">
                      <div>
                        <dt>Unique users</dt>
                        <dd className="numeric-value">{formatNumber(funnelTotals?.unique_users)}</dd>
                      </div>
                      <div>
                        <dt>Parcel selected</dt>
                        <dd className="numeric-value">
                          {formatNumber(funnelTotals?.parcel_selected_users)}
                        </dd>
                      </div>
                      <div>
                        <dt>Estimate started</dt>
                        <dd className="numeric-value">
                          {formatNumber(funnelTotals?.estimate_started_users)}
                        </dd>
                      </div>
                      <div>
                        <dt>Estimate completed</dt>
                        <dd className="numeric-value">
                          {formatNumber(funnelTotals?.estimate_completed_users)}
                        </dd>
                      </div>
                      <div>
                        <dt>PDF opened</dt>
                        <dd className="numeric-value">{formatNumber(funnelTotals?.pdf_opened_users)}</dd>
                      </div>
                      <div>
                        <dt>Feedback vote</dt>
                        <dd className="numeric-value">{formatNumber(funnelTotals?.feedback_users)}</dd>
                      </div>
                    </dl>
                    <div className="table-wrapper">
                      <table className="data-table">
                        <thead>
                          <tr>
                            <th>event</th>
                            <th className="numeric-cell">events</th>
                          </tr>
                        </thead>
                        <tbody>
                          <tr>
                            <td>parcel_selected</td>
                            <td className="numeric-cell">{formatNumber(funnelEvents?.parcel_selected)}</td>
                          </tr>
                          <tr>
                            <td>estimate_started</td>
                            <td className="numeric-cell">{formatNumber(funnelEvents?.estimate_started)}</td>
                          </tr>
                          <tr>
                            <td>estimate_completed</td>
                            <td className="numeric-cell">{formatNumber(funnelEvents?.estimate_completed)}</td>
                          </tr>
                          <tr>
                            <td>pdf_opened</td>
                            <td className="numeric-cell">{formatNumber(funnelEvents?.pdf_opened)}</td>
                          </tr>
                          <tr>
                            <td>feedback_votes</td>
                            <td className="numeric-cell">{formatNumber(funnelEvents?.feedback_votes)}</td>
                          </tr>
                        </tbody>
                      </table>
                    </div>
                    <dl className="metrics-grid">
                      <div>
                        <dt>Parcel → started</dt>
                        <dd className="numeric-value">
                          {formatPercent(funnelConversion?.parcel_to_estimate_started)}
                        </dd>
                      </div>
                      <div>
                        <dt>Started → completed</dt>
                        <dd className="numeric-value">
                          {formatPercent(funnelConversion?.estimate_started_to_completed)}
                        </dd>
                      </div>
                      <div>
                        <dt>Completed → PDF</dt>
                        <dd className="numeric-value">{formatPercent(funnelConversion?.completed_to_pdf)}</dd>
                      </div>
                      <div>
                        <dt>PDF → feedback</dt>
                        <dd className="numeric-value">{formatPercent(funnelConversion?.pdf_to_feedback)}</dd>
                      </div>
                    </dl>
                  </div>
                  <div className="admin-analytics-panel">
                    <h4 className="admin-analytics-subtitle">Time-to-value</h4>
                    <dl className="metrics-grid">
                      <div>
                        <dt>Median parcel → estimate</dt>
                        <dd className="numeric-value">
                          {formatMinutes(funnelTimeToValue?.median_minutes_parcel_to_first_estimate)}
                        </dd>
                      </div>
                      <div>
                        <dt>P80 parcel → estimate</dt>
                        <dd className="numeric-value">
                          {formatMinutes(funnelTimeToValue?.p80_minutes_parcel_to_first_estimate)}
                        </dd>
                      </div>
                      <div>
                        <dt>Median estimate → PDF</dt>
                        <dd className="numeric-value">
                          {formatMinutes(funnelTimeToValue?.median_minutes_estimate_to_pdf)}
                        </dd>
                      </div>
                      <div>
                        <dt>P80 estimate → PDF</dt>
                        <dd className="numeric-value">
                          {formatMinutes(funnelTimeToValue?.p80_minutes_estimate_to_pdf)}
                        </dd>
                      </div>
                    </dl>
                    {funnelSamples.length > 0 && (
                      <div className="table-wrapper">
                        <table className="data-table">
                          <thead>
                            <tr>
                              <th>user_id</th>
                              <th className="numeric-cell">parcel → estimate</th>
                              <th className="numeric-cell">estimate → PDF</th>
                            </tr>
                          </thead>
                          <tbody>
                            {funnelSamples.map((row) => (
                              <tr key={row.user_id}>
                                <td>{row.user_id}</td>
                                <td className="numeric-cell">{formatMinutes(row.parcel_to_estimate_min)}</td>
                                <td className="numeric-cell">{formatMinutes(row.estimate_to_pdf_min)}</td>
                              </tr>
                            ))}
                          </tbody>
                        </table>
                      </div>
                    )}
                  </div>
                </div>
              </>
            )}
          </section>
          <section className="admin-analytics-section">
            <h3 className="section-heading">Feedback inbox</h3>
            {feedbackInboxError && <p className="admin-analytics-error">Feedback inbox unavailable.</p>}
            {isFeedbackInboxLoading && <p className="admin-analytics-loading">Loading feedback inbox...</p>}
            {!isFeedbackInboxLoading && !feedbackInboxError && (
              <div className="admin-analytics-split">
                <div className="admin-analytics-panel">
                  <h4 className="admin-analytics-subtitle">Totals</h4>
                  <dl className="metrics-grid">
                    <div>
                      <dt>Thumbs up</dt>
                      <dd className="numeric-value">{formatNumber(feedbackTotals?.count_up)}</dd>
                    </div>
                    <div>
                      <dt>Thumbs down</dt>
                      <dd className="numeric-value">{formatNumber(feedbackTotals?.count_down)}</dd>
                    </div>
                    <div>
                      <dt>Down rate</dt>
                      <dd className="numeric-value">{formatPercent(feedbackTotals?.down_rate)}</dd>
                    </div>
                  </dl>
                </div>
                <div className="admin-analytics-panel">
                  <h4 className="admin-analytics-subtitle">Top reasons</h4>
                  {feedbackReasons.length === 0 ? (
                    <p className="admin-analytics-muted">No downvotes recorded.</p>
                  ) : (
                    <div className="table-wrapper">
                      <table className="data-table">
                        <thead>
                          <tr>
                            <th>reason</th>
                            <th className="numeric-cell">count</th>
                          </tr>
                        </thead>
                        <tbody>
                          {feedbackReasons.map((row) => (
                            <tr key={row.reason}>
                              <td>{row.reason}</td>
                              <td className="numeric-cell">{formatNumber(row.count)}</td>
                            </tr>
                          ))}
                        </tbody>
                      </table>
                    </div>
                  )}
                </div>
              </div>
            )}
            {!isFeedbackInboxLoading && !feedbackInboxError && feedbackUsers.length > 0 && (
              <div className="admin-analytics-panel">
                <h4 className="admin-analytics-subtitle">Feedback by user</h4>
                <div className="table-wrapper">
                  <table className="data-table">
                    <thead>
                      <tr>
                        <th>user_id</th>
                        <th className="numeric-cell">thumbs up</th>
                        <th className="numeric-cell">thumbs down</th>
                        <th className="numeric-cell">down rate</th>
                      </tr>
                    </thead>
                    <tbody>
                      {feedbackUsers.map((row) => (
                        <tr key={row.user_id}>
                          <td>{row.user_id}</td>
                          <td className="numeric-cell">{formatNumber(row.count_up)}</td>
                          <td className="numeric-cell">{formatNumber(row.count_down)}</td>
                          <td className="numeric-cell">{formatPercent(row.down_rate)}</td>
                        </tr>
                      ))}
                    </tbody>
                  </table>
                </div>
              </div>
            )}
          </section>
          <section className="admin-analytics-section">
            <h3 className="section-heading">Product feedback</h3>
            {feedbackError && <p className="admin-analytics-error">Feedback unavailable.</p>}
            {isFeedbackLoading && <p className="admin-analytics-loading">Loading feedback...</p>}
            {!isFeedbackLoading && !feedbackError && feedbackItems.length === 0 && (
              <p className="admin-analytics-muted">No feedback available for the selected window.</p>
            )}
            {!isFeedbackLoading && !feedbackError && feedbackItems.length > 0 && (
              <div className="admin-feedback-list">
                {feedbackItems.map((item) => {
                  const evidenceLines = getEvidenceLines(item);
                  return (
                    <article key={item.id} className="admin-feedback-card">
                      <div className="admin-feedback-header">
                        <h4 className="admin-analytics-subtitle">{item.title}</h4>
                        <span className={`severity-badge severity-${item.severity}`}>
                          {item.severity}
                        </span>
                      </div>
                      <p className="admin-analytics-muted">{item.summary}</p>
                      {evidenceLines.length > 0 && (
                        <ul className="admin-feedback-evidence">
                          {evidenceLines.map((line) => (
                            <li key={`${item.id}-${line.key}`}>
                              <span className="admin-feedback-label">{line.label}:</span>{" "}
                              <span>{line.value}</span>
                            </li>
                          ))}
                        </ul>
                      )}
                      {item.recommended_actions && item.recommended_actions.length > 0 && (
                        <div>
                          <h5 className="admin-feedback-actions-title">Recommended actions</h5>
                          <ul className="admin-feedback-actions">
                            {item.recommended_actions.map((action) => (
                              <li key={action}>{action}</li>
                            ))}
                          </ul>
                        </div>
                      )}
                    </article>
                  );
                })}
              </div>
            )}
          </section>
          <section className="admin-analytics-section">
            <h3 className="section-heading">Insights</h3>
            {insightsError && <p className="admin-analytics-error">Insights unavailable.</p>}
            {isInsightsLoading && <p className="admin-analytics-loading">Loading insights...</p>}
            {!isInsightsLoading && !insightsError && highlights.length === 0 && (
              <p className="admin-analytics-muted">No insights available for the selected window.</p>
            )}
            {!isInsightsLoading && !insightsError && highlights.length > 0 && (
              <ul className="admin-analytics-insights">
                {highlights.map((item, index) => (
                  <li key={`${item.title}-${index}`} className="admin-analytics-insight">
                    <h4 className="admin-analytics-subtitle">{item.title}</h4>
                    <p className="admin-analytics-muted">{item.detail || "No detail available yet."}</p>
                  </li>
                ))}
              </ul>
            )}
          </section>
          <section className="admin-analytics-section">
            <h3 className="section-heading">Key performance indicators</h3>
            {summaryError && <p className="admin-analytics-error">Admin endpoints unavailable.</p>}
            {isSummaryLoading && <p className="admin-analytics-loading">Loading KPI summary...</p>}
            {!isSummaryLoading && !summaryError && (
              <dl className="stat-grid">
                <div className="stat">
                  <dt>Active users</dt>
                  <dd className="numeric-value">{formatNumber(totals?.active_users)}</dd>
                </div>
                <div className="stat">
                  <dt>Requests</dt>
                  <dd className="numeric-value">{formatNumber(totals?.requests)}</dd>
                </div>
                <div className="stat">
                  <dt>Estimates</dt>
                  <dd className="numeric-value">{formatNumber(totals?.estimates)}</dd>
                </div>
                <div className="stat">
                  <dt>PDF exports</dt>
                  <dd className="numeric-value">{formatNumber(totals?.pdf_exports)}</dd>
                </div>
                <div className="stat highlight">
                  <dt>Error rate</dt>
                  <dd className="numeric-value">{formatPercent(totals?.error_rate)}</dd>
                </div>
              </dl>
            )}
          </section>

          <section className="admin-analytics-section">
            <h3 className="section-heading">User usage</h3>
            <div className="admin-analytics-split">
              <div className="admin-analytics-panel">
                <h4 className="admin-analytics-subtitle">Users</h4>
                {usersError && <p className="admin-analytics-error">Admin endpoints unavailable.</p>}
                {isUsersLoading && <p className="admin-analytics-loading">Loading users...</p>}
                {!isUsersLoading && !usersError && users.length === 0 && (
                  <p className="admin-analytics-muted">No usage data for the selected window.</p>
                )}
                {!isUsersLoading && !usersError && users.length > 0 && (
                  <div className="table-wrapper">
                    <table className="data-table">
                      <thead>
                        <tr>
                          <th>user_id</th>
                          <th className="numeric-cell">requests</th>
                          <th className="numeric-cell">estimates</th>
                          <th className="numeric-cell">pdf_exports</th>
                          <th>last_seen</th>
                          <th className="numeric-cell">error_rate</th>
                        </tr>
                      </thead>
                      <tbody>
                        {users.map((user) => (
                          <tr
                            key={user.user_id}
                            className={`admin-analytics-user-row${selectedUserId === user.user_id ? " is-selected" : ""}`}
                            onClick={() => setSelectedUserId(user.user_id)}
                          >
                            <td>{user.user_id}</td>
                            <td className="numeric-cell">{formatNumber(user.requests)}</td>
                            <td className="numeric-cell">{formatNumber(user.estimates)}</td>
                            <td className="numeric-cell">{formatNumber(user.pdf_exports)}</td>
                            <td>{formatDateTime(user.last_seen)}</td>
                            <td className="numeric-cell">{formatPercent(user.error_rate)}</td>
                          </tr>
                        ))}
                      </tbody>
                    </table>
                  </div>
                )}
              </div>
              <div className="admin-analytics-panel">
                <h4 className="admin-analytics-subtitle">User detail</h4>
                {!selectedUserId && <p className="admin-analytics-muted">Select a user to see detail.</p>}
                {isDetailLoading && <p className="admin-analytics-loading">Loading user detail...</p>}
                {detailError && <p className="admin-analytics-error">Unable to load user detail.</p>}
                {userDetail && !isDetailLoading && !detailError && (
                  <div className="admin-analytics-section">
                    <div>
                      <h5 className="admin-analytics-subtitle">Metrics summary</h5>
                      <dl className="metrics-grid">
                        <div>
                          <dt>Requests</dt>
                          <dd className="numeric-value">{formatNumber(metrics?.requests)}</dd>
                        </div>
                        <div>
                          <dt>Estimates</dt>
                          <dd className="numeric-value">{formatNumber(metrics?.estimates)}</dd>
                        </div>
                        <div>
                          <dt>PDF exports</dt>
                          <dd className="numeric-value">{formatNumber(metrics?.pdf_exports)}</dd>
                        </div>
                        <div>
                          <dt>Error rate</dt>
                          <dd className="numeric-value">{formatPercent(metrics?.error_rate)}</dd>
                        </div>
                      </dl>
                    </div>
                    <div>
                      <h5 className="admin-analytics-subtitle">Top paths</h5>
                      {topPaths.length === 0 ? (
                        <p className="admin-analytics-muted">No path activity recorded.</p>
                      ) : (
                        <div className="table-wrapper">
                          <table className="data-table">
                            <thead>
                              <tr>
                                <th>path</th>
                                <th className="numeric-cell">count</th>
                              </tr>
                            </thead>
                            <tbody>
                              {topPaths.map((row) => (
                                <tr key={row.path}>
                                  <td>{row.path}</td>
                                  <td className="numeric-cell">{formatNumber(row.count)}</td>
                                </tr>
                              ))}
                            </tbody>
                          </table>
                        </div>
                      )}
                    </div>
                    <div>
                      <h5 className="admin-analytics-subtitle">Daily timeline</h5>
                      {daily.length === 0 ? (
                        <p className="admin-analytics-muted">No daily activity recorded.</p>
                      ) : (
                        <div className="table-wrapper">
                          <table className="data-table">
                            <thead>
                              <tr>
                                <th>date</th>
                                <th className="numeric-cell">requests</th>
                                <th className="numeric-cell">estimates</th>
                                <th className="numeric-cell">pdf_exports</th>
                                <th className="numeric-cell">errors</th>
                              </tr>
                            </thead>
                            <tbody>
                              {daily.map((row) => (
                                <tr key={row.date}>
                                  <td>{row.date}</td>
                                  <td className="numeric-cell">{formatNumber(row.requests)}</td>
                                  <td className="numeric-cell">{formatNumber(row.estimates)}</td>
                                  <td className="numeric-cell">{formatNumber(row.pdf_exports)}</td>
                                  <td className="numeric-cell">{formatNumber(row.errors)}</td>
                                </tr>
                              ))}
                            </tbody>
                          </table>
                        </div>
                      )}
                    </div>
                  </div>
                )}
              </div>
            </div>
          </section>
        </div>
      </div>
    </div>
  );
}
