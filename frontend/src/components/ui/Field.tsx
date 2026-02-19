import type { ReactNode } from "react";

type FieldProps = {
  label: string;
  children: ReactNode;
  hint?: ReactNode;
};

export default function Field({ label, children, hint }: FieldProps) {
  return (
    <label className="ot-field">
      <span className="ot-field__label">{label}</span>
      {children}
      {hint ? <span className="ot-field__hint">{hint}</span> : null}
    </label>
  );
}
