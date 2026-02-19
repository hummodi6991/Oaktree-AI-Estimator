import type { ButtonHTMLAttributes } from "react";

type ToggleChipProps = Omit<ButtonHTMLAttributes<HTMLButtonElement>, "onClick"> & {
  active: boolean;
  label: string;
  onClick: () => void;
};

export default function ToggleChip({ active, label, className = "", onClick, type = "button", ...props }: ToggleChipProps) {
  return (
    <button
      type={type}
      className={`ot-toggle-chip ${active ? "is-active" : ""} ${className}`.trim()}
      onClick={onClick}
      {...props}
    >
      {label}
    </button>
  );
}
