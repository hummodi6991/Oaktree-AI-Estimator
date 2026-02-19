import type { InputHTMLAttributes } from "react";

type InputProps = InputHTMLAttributes<HTMLInputElement> & {
  fullWidth?: boolean;
};

export default function Input({ fullWidth = false, className = "", ...props }: InputProps) {
  return <input className={`ot-input ${fullWidth ? "ot-input--full" : ""} ${className}`.trim()} {...props} />;
}
