import { useEffect, useMemo, useState } from "react";

import {
  getAdminUsageInsights,
  getAdminUsageSummary,
  getAdminUsageUser,
  getAdminUsageUsers,
  type AdminUsageInsights,
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

function formatDateTime(value?: string | null): string {
  if (!value) return "—";
  const date = new Date(value);
  if (Number.isNaN(date.getTime())) return value;
  return date.toLocaleString();
}

export default function AdminAnalyticsModal({ isOpen, onClose }: AdminAnalyticsModalProps) {
  const [sinceKey, setSinceKey] = useState<SinceKey>("30d");
  const [summary, setSummary] = useState<AdminUsageSummary | null>(null);
  const [insights, setInsights] = useState<AdminUsageInsights | null>(null);
  const [users, setUsers] = useState<AdminUsageUser[]>([]);
  const [selectedUserId, setSelectedUserId] = useState<string | null>(null);
  const [userDetail, setUserDetail] = useState<AdminUsageUserDetail | null>(null);
  const [summaryError, setSummaryError] = useState<string | null>(null);
  const [insightsError, setInsightsError] = useState<string | null>(null);
  const [usersError, setUsersError] = useState<string | null>(null);
  const [detailError, setDetailError] = useState<string | null>(null);
  const [isSummaryLoading, setIsSummaryLoading] = useState(false);
  const [isInsightsLoading, setIsInsightsLoading] = useState(false);
  const [isUsersLoading, setIsUsersLoading] = useState(false);
  const [isDetailLoading, setIsDetailLoading] = useState(false);

  const since = useMemo(() => getSinceDate(sinceKey), [sinceKey]);

  useEffect(() => {
    if (!isOpen) {
      setSummary(null);
      setInsights(null);
      setUsers([]);
      setSelectedUserId(null);
      setUserDetail(null);
      setSummaryError(null);
      setInsightsError(null);
      setUsersError(null);
      setDetailError(null);
      setIsSummaryLoading(false);
      setIsInsightsLoading(false);
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
  const metrics = userDetail?.metrics;
  const topPaths = userDetail?.top_paths ?? [];
  const daily = userDetail?.daily ?? [];
  const highlights = insights?.highlights ?? [];

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
