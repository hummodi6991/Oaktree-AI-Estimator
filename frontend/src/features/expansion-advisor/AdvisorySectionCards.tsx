import { useTranslation } from "react-i18next";
import type {
  MemoCompetitiveLandscape,
  MemoFinancialFraming,
  MemoMarketContext,
  MemoPropertyOverview,
  StructuredMemo,
} from "../../lib/api/expansionAdvisor";
import { fmtM2, fmtMeters, fmtSAR, fmtScore } from "./formatHelpers";

type Lang = "en" | "ar";

interface FieldRowProps {
  label: string;
  value: React.ReactNode;
}

function FieldRow({ label, value }: FieldRowProps) {
  return (
    <div className="ea-detail__kv ea-memo-section__kv">
      <span className="ea-detail__kv-label">{label}</span>
      <span className="ea-detail__kv-value">{value}</span>
    </div>
  );
}

function pctFromFraction(frac: number | null): string | null {
  if (frac == null || !Number.isFinite(frac)) return null;
  const clamped = Math.max(0, Math.min(1, frac));
  return `${Math.round(clamped * 100)}th percentile`;
}

function PropertyOverviewCard({
  section,
  lang,
}: {
  section: MemoPropertyOverview;
  lang: Lang;
}) {
  const { t } = useTranslation();
  const summary = (section.summary || "").trim();
  // Render the section even when summary is empty — body fields may carry
  // signal. The card is a no-op only when *every* field is null/empty.
  const fieldEntries: Array<[string, React.ReactNode]> = [];
  if (section.area_m2 != null) fieldEntries.push([t("expansionAdvisor.areaLabel"), fmtM2(section.area_m2)]);
  if (section.frontage_width_m != null) fieldEntries.push([t("expansionAdvisor.advisorySection.frontage"), fmtMeters(section.frontage_width_m)]);
  if (section.street_type) fieldEntries.push([t("expansionAdvisor.advisorySection.streetType"), section.street_type]);
  if (section.parking_evidence) fieldEntries.push([t("expansionAdvisor.advisorySection.parkingEvidence"), section.parking_evidence]);
  if (section.visibility_score != null) fieldEntries.push([t("expansionAdvisor.accessVisibility"), `${section.visibility_score}/100`]);
  if (section.listing_age_days != null) fieldEntries.push([t("expansionAdvisor.advisorySection.listingAge"), `${section.listing_age_days} d`]);
  if (section.vacancy_status) fieldEntries.push([t("expansionAdvisor.advisorySection.vacancyStatus"), section.vacancy_status]);

  if (!summary && fieldEntries.length === 0) return null;
  return (
    <details
      className="ea-memo-section ea-memo-section--property-overview"
      lang={lang}
    >
      <summary className="ea-memo-section__summary">
        <span className="ea-memo-section__title">{t("expansionAdvisor.advisorySection.propertyOverview.title")}</span>
        {summary && <span className="ea-memo-section__summary-text">{summary}</span>}
      </summary>
      <div className="ea-memo-section__body">
        {fieldEntries.length > 0 && (
          <div className="ea-detail__grid">
            {fieldEntries.map(([label, value]) => (
              <FieldRow key={label} label={label} value={value} />
            ))}
          </div>
        )}
      </div>
    </details>
  );
}

function FinancialFramingCard({
  section,
  lang,
}: {
  section: MemoFinancialFraming;
  lang: Lang;
}) {
  const { t } = useTranslation();
  const summary = (section.summary || "").trim();
  const thesis = (section.thesis || "").trim();
  const fieldEntries: Array<[string, React.ReactNode]> = [];
  if (section.annual_rent_sar != null) fieldEntries.push([t("expansionAdvisor.annualRent"), fmtSAR(section.annual_rent_sar)]);
  if (section.comparable_median_annual_rent_sar != null) {
    fieldEntries.push([t("expansionAdvisor.advisorySection.comparableMedian"), fmtSAR(section.comparable_median_annual_rent_sar)]);
  }
  const pctLabel = pctFromFraction(section.rent_percentile_vs_comparables);
  if (pctLabel) fieldEntries.push([t("expansionAdvisor.advisorySection.rentPercentile"), pctLabel]);
  if (section.comparable_n != null) fieldEntries.push([t("expansionAdvisor.advisorySection.comparableN"), section.comparable_n]);
  if (section.comparable_scope) fieldEntries.push([t("expansionAdvisor.advisorySection.comparableScope"), section.comparable_scope]);
  if (section.spread_to_median_sar != null) {
    const sign = section.spread_to_median_sar > 0 ? "+" : section.spread_to_median_sar < 0 ? "−" : "";
    fieldEntries.push([
      t("expansionAdvisor.advisorySection.spreadToMedian"),
      `${sign}${fmtSAR(Math.abs(section.spread_to_median_sar))}`,
    ]);
  }

  if (!summary && !thesis && fieldEntries.length === 0) return null;
  return (
    <details
      className="ea-memo-section ea-memo-section--financial-framing"
      lang={lang}
    >
      <summary className="ea-memo-section__summary">
        <span className="ea-memo-section__title">{t("expansionAdvisor.advisorySection.financialFraming.title")}</span>
        {summary && <span className="ea-memo-section__summary-text">{summary}</span>}
      </summary>
      <div className="ea-memo-section__body">
        {thesis && <p className="ea-memo-section__thesis">{thesis}</p>}
        {fieldEntries.length > 0 && (
          <div className="ea-detail__grid">
            {fieldEntries.map(([label, value]) => (
              <FieldRow key={label} label={label} value={value} />
            ))}
          </div>
        )}
      </div>
    </details>
  );
}

function MarketContextCard({
  section,
  lang,
}: {
  section: MemoMarketContext;
  lang: Lang;
}) {
  const { t } = useTranslation();
  const summary = (section.summary || "").trim();
  const thesis = (section.demand_thesis || "").trim();
  const fieldEntries: Array<[string, React.ReactNode]> = [];
  if (section.population_reach != null) {
    fieldEntries.push([t("expansionAdvisor.advisorySection.populationReach"), fmtScore(section.population_reach)]);
  }
  if (section.district_momentum) {
    fieldEntries.push([t("expansionAdvisor.advisorySection.districtMomentum"), section.district_momentum]);
  }
  if (section.realized_demand_30d != null) {
    fieldEntries.push([t("expansionAdvisor.realizedDemand30d"), fmtScore(section.realized_demand_30d)]);
  }
  if (section.realized_demand_branches != null) {
    fieldEntries.push([t("expansionAdvisor.advisorySection.realizedDemandBranches"), section.realized_demand_branches]);
  }
  if (section.delivery_listing_count != null) {
    fieldEntries.push([t("expansionAdvisor.advisorySection.deliveryListingCount"), section.delivery_listing_count]);
  }

  if (!summary && !thesis && fieldEntries.length === 0) return null;
  return (
    <details
      className="ea-memo-section ea-memo-section--market-context"
      lang={lang}
    >
      <summary className="ea-memo-section__summary">
        <span className="ea-memo-section__title">{t("expansionAdvisor.advisorySection.marketContext.title")}</span>
        {summary && <span className="ea-memo-section__summary-text">{summary}</span>}
      </summary>
      <div className="ea-memo-section__body">
        {thesis && <p className="ea-memo-section__thesis">{thesis}</p>}
        {fieldEntries.length > 0 && (
          <div className="ea-detail__grid">
            {fieldEntries.map(([label, value]) => (
              <FieldRow key={label} label={label} value={value} />
            ))}
          </div>
        )}
      </div>
    </details>
  );
}

function CompetitiveLandscapeCard({
  section,
  lang,
}: {
  section: MemoCompetitiveLandscape;
  lang: Lang;
}) {
  const { t } = useTranslation();
  const summary = (section.summary || "").trim();
  const thesis = (section.saturation_thesis || "").trim();
  const isArabic = lang === "ar";
  const topChains = Array.isArray(section.top_chains) ? section.top_chains : [];
  const peers = Array.isArray(section.comparable_competitors) ? section.comparable_competitors : [];
  const next = section.next_candidate_summary;

  if (!summary && !thesis && topChains.length === 0 && peers.length === 0 && !next) {
    return null;
  }
  return (
    <details
      className="ea-memo-section ea-memo-section--competitive-landscape"
      lang={lang}
    >
      <summary className="ea-memo-section__summary">
        <span className="ea-memo-section__title">{t("expansionAdvisor.advisorySection.competitiveLandscape.title")}</span>
        {summary && <span className="ea-memo-section__summary-text">{summary}</span>}
      </summary>
      <div className="ea-memo-section__body">
        {thesis && <p className="ea-memo-section__thesis">{thesis}</p>}
        {topChains.length > 0 && (
          <div className="ea-memo-section__group">
            <h6 className="ea-memo-section__group-title">{t("expansionAdvisor.advisorySection.topChains")}</h6>
            <ul className="ea-memo-section__list">
              {topChains.map((chain, idx) => {
                const display = (isArabic ? chain.display_name_ar : chain.display_name_en) ?? chain.display_name_en ?? "—";
                const distance = chain.nearest_distance_m != null ? ` · ${fmtMeters(chain.nearest_distance_m)}` : "";
                return (
                  <li key={`chain-${idx}`}>
                    {display} ({chain.branch_count}){distance}
                  </li>
                );
              })}
            </ul>
          </div>
        )}
        {peers.length > 0 && (
          <div className="ea-memo-section__group">
            <h6 className="ea-memo-section__group-title">{t("expansionAdvisor.advisorySection.comparableCompetitors")}</h6>
            <ul className="ea-memo-section__list">
              {peers.map((peer, idx) => (
                <li key={`peer-${peer.id ?? idx}`}>{peer.name}</li>
              ))}
            </ul>
          </div>
        )}
        {next && (
          <div className="ea-memo-section__group">
            <h6 className="ea-memo-section__group-title">{t("expansionAdvisor.advisorySection.nextCandidate", { rank: next.rank })}</h6>
            <div className="ea-detail__grid">
              {next.district && <FieldRow label={t("expansionAdvisor.district")} value={next.district} />}
              {next.annual_rent_sar != null && (
                <FieldRow label={t("expansionAdvisor.annualRent")} value={fmtSAR(next.annual_rent_sar)} />
              )}
              {pctFromFraction(next.rent_percentile_vs_comparables) && (
                <FieldRow
                  label={t("expansionAdvisor.advisorySection.rentPercentile")}
                  value={pctFromFraction(next.rent_percentile_vs_comparables)}
                />
              )}
              {next.access_visibility_score != null && (
                <FieldRow
                  label={t("expansionAdvisor.accessVisibility")}
                  value={`${Math.round(next.access_visibility_score)}/100`}
                />
              )}
            </div>
          </div>
        )}
      </div>
    </details>
  );
}

interface AdvisorySectionCardsProps {
  memo: StructuredMemo;
  lang: Lang;
}

export default function AdvisorySectionCards({ memo, lang }: AdvisorySectionCardsProps) {
  const cards = [
    memo.property_overview ? (
      <PropertyOverviewCard
        key="property"
        section={memo.property_overview}
        lang={lang}
      />
    ) : null,
    memo.financial_framing ? (
      <FinancialFramingCard
        key="financial"
        section={memo.financial_framing}
        lang={lang}
      />
    ) : null,
    memo.market_context ? (
      <MarketContextCard
        key="market"
        section={memo.market_context}
        lang={lang}
      />
    ) : null,
    memo.competitive_landscape ? (
      <CompetitiveLandscapeCard
        key="competitive"
        section={memo.competitive_landscape}
        lang={lang}
      />
    ) : null,
  ].filter((c): c is React.ReactElement => c !== null);

  if (cards.length === 0) return null;

  return (
    <div
      className="ea-memo-advisory-cards"
      dir={lang === "ar" ? "rtl" : "ltr"}
      data-testid="ea-memo-advisory-cards"
    >
      {cards}
    </div>
  );
}
