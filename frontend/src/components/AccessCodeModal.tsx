import type { FormEvent } from "react";
import { useEffect, useRef, useState } from "react";

type AccessCodeModalProps = {
  isOpen: boolean;
  onSubmit: (code: string) => void;
};

export default function AccessCodeModal({ isOpen, onSubmit }: AccessCodeModalProps) {
  const [value, setValue] = useState("");
  const inputRef = useRef<HTMLInputElement | null>(null);

  useEffect(() => {
    if (!isOpen) return;
    setValue("");
    const frame = requestAnimationFrame(() => inputRef.current?.focus());
    return () => cancelAnimationFrame(frame);
  }, [isOpen]);

  if (!isOpen) return null;

  const handleSubmit = (event: FormEvent<HTMLFormElement>) => {
    event.preventDefault();
    const trimmed = value.trim();
    if (!trimmed) return;
    onSubmit(trimmed);
  };

  return (
    <div className="access-modal-overlay" role="presentation">
      <div className="access-modal" role="dialog" aria-modal="true" aria-labelledby="access-modal-title">
        <h2 id="access-modal-title" className="access-modal-title">Enter access code</h2>
        <p className="access-modal-body">
          Use your assigned access code to continue. If authentication is disabled locally, any value will work.
        </p>
        <form className="access-modal-form" onSubmit={handleSubmit}>
          <label className="access-modal-label" htmlFor="access-code-input">
            Access code
          </label>
          <input
            id="access-code-input"
            ref={inputRef}
            className="access-modal-input"
            type="password"
            value={value}
            onChange={(event) => setValue(event.target.value)}
            autoComplete="current-password"
          />
          <button className="primary-button access-modal-button" type="submit" disabled={!value.trim()}>
            Continue
          </button>
        </form>
      </div>
    </div>
  );
}
