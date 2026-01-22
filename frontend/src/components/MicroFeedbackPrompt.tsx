import { useState } from "react";

import { trackEvent } from "../api";

type MicroFeedbackPromptProps = {
  isOpen: boolean;
  context: "estimate" | "pdf";
  estimateId?: string | null;
  onDismiss: () => void;
  onSubmit: () => void;
};

const DOWN_REASONS = [
  { value: "land_price_wrong", label: "Land price seems wrong" },
  { value: "far_wrong", label: "FAR looks off" },
  { value: "costs_wrong", label: "Costs look off" },
  { value: "confusing", label: "Confusing output" },
  { value: "missing_data", label: "Missing data" },
  { value: "other", label: "Other" },
];

export default function MicroFeedbackPrompt({
  isOpen,
  context,
  estimateId,
  onDismiss,
  onSubmit,
}: MicroFeedbackPromptProps) {
  const [isDownVote, setIsDownVote] = useState(false);
  const [selectedReasons, setSelectedReasons] = useState<string[]>([]);

  if (!isOpen) return null;

  const resetPrompt = () => {
    setIsDownVote(false);
    setSelectedReasons([]);
  };
  const dismissPrompt = () => {
    resetPrompt();
    onDismiss();
  };

  const handleUpVote = async () => {
    await trackEvent("feedback_vote", {
      estimateId: estimateId ?? undefined,
      meta: { vote: "up", context },
    });
    resetPrompt();
    onSubmit();
  };

  const handleDownVote = () => {
    setIsDownVote(true);
  };

  const handleReasonToggle = (value: string) => {
    setSelectedReasons((current) =>
      current.includes(value) ? current.filter((item) => item !== value) : [...current, value],
    );
  };

  const handleSubmitDownVote = async () => {
    await trackEvent("feedback_vote", {
      estimateId: estimateId ?? undefined,
      meta: { vote: "down", context, reasons: selectedReasons },
    });
    resetPrompt();
    onSubmit();
  };

  return (
    <div className="micro-feedback-overlay" role="presentation">
      <div className="micro-feedback-card" role="dialog" aria-modal="true" aria-label="Feedback prompt">
        <h3>Was this output useful?</h3>
        {!isDownVote ? (
          <div className="micro-feedback-actions">
            <button type="button" className="micro-feedback-thumb" onClick={handleUpVote}>
              👍
            </button>
            <button type="button" className="micro-feedback-thumb" onClick={handleDownVote}>
              👎
            </button>
          </div>
        ) : (
          <div className="micro-feedback-reasons">
            <p>Select any reasons:</p>
            <div className="micro-feedback-reasons-grid">
              {DOWN_REASONS.map((reason) => (
                <label key={reason.value} className="micro-feedback-reason">
                  <input
                    type="checkbox"
                    checked={selectedReasons.includes(reason.value)}
                    onChange={() => handleReasonToggle(reason.value)}
                  />
                  <span>{reason.label}</span>
                </label>
              ))}
            </div>
            <button type="button" onClick={handleSubmitDownVote}>
              Send feedback
            </button>
          </div>
        )}
        <button type="button" className="micro-feedback-dismiss" onClick={dismissPrompt}>
          Not now
        </button>
      </div>
    </div>
  );
}
