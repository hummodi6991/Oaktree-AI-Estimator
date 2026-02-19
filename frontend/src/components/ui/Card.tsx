import type { ReactNode } from "react";

type CardProps = {
  title?: string;
  children: ReactNode;
  footer?: ReactNode;
};

export default function Card({ title, children, footer }: CardProps) {
  return (
    <section className="ot-card calc-card">
      {title ? <header className="calc-card__header">{title}</header> : null}
      <div className="calc-card__body">{children}</div>
      {footer ? <footer className="calc-card__footer">{footer}</footer> : null}
    </section>
  );
}
