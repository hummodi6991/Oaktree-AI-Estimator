import type { InputHTMLAttributes } from "react";

type InputProps = Omit<InputHTMLAttributes<HTMLInputElement>, "size"> & {
  fullWidth?: boolean;
  size?: "sm" | "md";
};

export default function Input({ fullWidth = false, size = "md", className = "", ...props }: InputProps) {
  return (
    <input
      className={`ot-input ot-input--${size} ${fullWidth ? "ot-input--full" : ""} ${className}`.trim()}
      {...props}
    />
  );
}
