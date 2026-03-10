import { useTranslation } from "react-i18next";

export default function ExpansionComparePanel({ compareIds, onCompare }: { compareIds: string[]; onCompare: () => void }) {
  const { t } = useTranslation();
  const enabled = compareIds.length >= 2 && compareIds.length <= 6;
  return <button disabled={!enabled} onClick={onCompare}>{enabled ? t("expansionAdvisor.compareCandidates") : t("expansionAdvisor.compareNeedTwo")}</button>;
}
