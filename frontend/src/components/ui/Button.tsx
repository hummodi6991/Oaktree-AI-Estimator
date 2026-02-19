import type { ButtonHTMLAttributes } from "react";

type ButtonProps = ButtonHTMLAttributes<HTMLButtonElement> & {
  variant?: "primary" | "secondary";
};

export default function Button({ variant = "primary", className = "", type = "button", ...props }: ButtonProps) {
  const variantClass = variant === "secondary" ? "ot-btn-secondary" : "ot-btn-primary";
  return <button type={type} className={`${variantClass} ot-btn ${className}`.trim()} {...props} />;
}
