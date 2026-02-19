import type { InputHTMLAttributes } from "react";

type RadioProps = Omit<InputHTMLAttributes<HTMLInputElement>, "type"> & {
  label: string;
};

export default function Radio({ label, className = "", ...props }: RadioProps) {
  return (
    <label className={`ot-radio ${className}`.trim()}>
      <input type="radio" {...props} />
      <span>{label}</span>
    </label>
  );
}
