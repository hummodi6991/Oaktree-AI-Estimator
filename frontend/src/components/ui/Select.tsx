import type { SelectHTMLAttributes } from "react";

type SelectProps = SelectHTMLAttributes<HTMLSelectElement> & {
  fullWidth?: boolean;
};

export default function Select({ fullWidth = false, className = "", children, ...props }: SelectProps) {
  return (
    <select className={`ot-input ot-select ${fullWidth ? "ot-input--full" : ""} ${className}`.trim()} {...props}>
      {children}
    </select>
  );
}
