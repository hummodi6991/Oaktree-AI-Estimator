import { useState } from "react";
import { useTranslation } from "react-i18next";

type Props = {
  defaultTitle: string;
  defaultDescription?: string;
  defaultStatus?: "draft" | "final";
  saving: boolean;
  error: string | null;
  isUpdate?: boolean;
  onSave: (title: string, description: string, status: "draft" | "final") => void;
  onClose: () => void;
};

export default function SaveStudyDialog({ defaultTitle, defaultDescription, defaultStatus, saving, error, isUpdate, onSave, onClose }: Props) {
  const { t } = useTranslation();
  const [title, setTitle] = useState(defaultTitle);
  const [description, setDescription] = useState(defaultDescription || "");
  const [status, setStatus] = useState<"draft" | "final">(defaultStatus || "draft");

  return (
    <div className="ea-dialog-backdrop" onClick={onClose}>
      <div className="ea-dialog" onClick={(e) => e.stopPropagation()}>
        <div className="ea-dialog__header">
          <h3 className="ea-dialog__title">{isUpdate ? t("expansionAdvisor.updateStudyTitle") : t("expansionAdvisor.saveStudyTitle")}</h3>
        </div>
        <div className="ea-dialog__body">
          <div className="ea-form__field">
            <label className="ea-form__label">{t("expansionAdvisor.studyTitle")}</label>
            <input className="ea-form__input" value={title} onChange={(e) => setTitle(e.target.value)} />
          </div>
          <div className="ea-form__field">
            <label className="ea-form__label">{t("expansionAdvisor.studyDescription")}</label>
            <input className="ea-form__input" value={description} onChange={(e) => setDescription(e.target.value)} />
          </div>
          <div className="ea-form__field">
            <label className="ea-form__label">{t("expansionAdvisor.studyStatus")}</label>
            <select className="ea-form__select" value={status} onChange={(e) => setStatus(e.target.value as "draft" | "final")}>
              <option value="draft">{t("expansionAdvisor.draft")}</option>
              <option value="final">{t("expansionAdvisor.final")}</option>
            </select>
          </div>
          {error && <div className="ea-state ea-state--error">{error}</div>}
        </div>
        <div className="ea-dialog__footer">
          <button className="oak-btn oak-btn--tertiary" onClick={onClose} disabled={saving}>
            {t("expansionAdvisor.cancel")}
          </button>
          <button
            className="oak-btn oak-btn--primary"
            disabled={saving || !title.trim()}
            onClick={() => onSave(title.trim(), description.trim(), status)}
          >
            {saving ? t("expansionAdvisor.saving") : isUpdate ? t("expansionAdvisor.update") : t("expansionAdvisor.save")}
          </button>
        </div>
      </div>
    </div>
  );
}
