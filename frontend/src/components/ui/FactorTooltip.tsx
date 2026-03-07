import { useCallback, useRef, useState } from "react";

type Props = {
  text: string;
};

/**
 * Lightweight info-icon tooltip. Hover on desktop, tap on touch.
 * Positions above the icon; auto-flips if near the top of the viewport.
 */
export default function FactorTooltip({ text }: Props) {
  const [open, setOpen] = useState(false);
  const timeout = useRef<number | null>(null);
  const ref = useRef<HTMLButtonElement>(null);

  const show = useCallback(() => {
    if (timeout.current) clearTimeout(timeout.current);
    setOpen(true);
  }, []);

  const hide = useCallback(() => {
    timeout.current = window.setTimeout(() => setOpen(false), 120);
  }, []);

  const toggle = useCallback(() => setOpen((v) => !v), []);

  return (
    <span style={{ position: "relative", display: "inline-flex", alignItems: "center" }}>
      <button
        ref={ref}
        type="button"
        aria-label={text}
        onClick={toggle}
        onMouseEnter={show}
        onMouseLeave={hide}
        onFocus={show}
        onBlur={hide}
        style={{
          display: "inline-flex",
          alignItems: "center",
          justifyContent: "center",
          width: 14,
          height: 14,
          borderRadius: "50%",
          border: "1px solid var(--oak-outlines, #d4d4d4)",
          background: "transparent",
          color: "var(--oak-text-light, #828282)",
          fontSize: 9,
          fontWeight: 700,
          lineHeight: 1,
          cursor: "help",
          padding: 0,
          flexShrink: 0,
          transition: "border-color 120ms ease, color 120ms ease",
          ...(open
            ? { borderColor: "var(--oak-text-gray, #4c4c4c)", color: "var(--oak-text-gray, #4c4c4c)" }
            : {}),
        }}
      >
        i
      </button>
      {open && (
        <div
          role="tooltip"
          style={{
            position: "absolute",
            bottom: "calc(100% + 6px)",
            left: "50%",
            transform: "translateX(-50%)",
            width: "max-content",
            maxWidth: 220,
            padding: "6px 10px",
            borderRadius: 6,
            background: "var(--oak-text-dark, #171717)",
            color: "#fff",
            fontSize: 11,
            fontWeight: 400,
            lineHeight: 1.45,
            boxShadow: "0 2px 8px rgba(0,0,0,0.18)",
            zIndex: 10,
            pointerEvents: "none",
            whiteSpace: "normal",
          }}
        >
          {text}
        </div>
      )}
    </span>
  );
}
