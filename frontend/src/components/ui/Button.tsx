import type { ButtonHTMLAttributes } from "react";

type ButtonProps = ButtonHTMLAttributes<HTMLButtonElement> & {
  variant?: "primary" | "secondary" | "ghost";
  size?: "sm" | "md";
};

export default function Button({
  variant = "primary",
  size = "md",
  className = "",
  type = "button",
  ...props
}: ButtonProps) {
  const variantClass =
    variant === "secondary" ? "ot-btn-secondary" : variant === "ghost" ? "ot-btn-ghost" : "ot-btn-primary";
  return <button type={type} className={`${variantClass} ot-btn ot-btn--${size} ${className}`.trim()} {...props} />;
}
