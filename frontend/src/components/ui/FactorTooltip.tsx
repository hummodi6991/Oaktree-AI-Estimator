import { useCallback, useEffect, useRef, useState } from "react";
import { createPortal } from "react-dom";

type Props = {
  text: string;
};

/**
 * Lightweight info-icon tooltip. Hover on desktop, tap on touch.
 * Renders via portal to document.body so it is never clipped by
 * overflow:hidden ancestors or stacking-context issues.
 */
export default function FactorTooltip({ text }: Props) {
  const [open, setOpen] = useState(false);
  const timeout = useRef<number | null>(null);
  const isTouchRef = useRef(false);
  const ref = useRef<HTMLButtonElement>(null);
  const [pos, setPos] = useState<{ top: number; left: number } | null>(null);

  const updatePos = useCallback(() => {
    if (!ref.current) return;
    const rect = ref.current.getBoundingClientRect();
    setPos({
      top: rect.top + window.scrollY,
      left: rect.left + rect.width / 2 + window.scrollX,
    });
  }, []);

  const show = useCallback(() => {
    if (isTouchRef.current) return;
    if (timeout.current) clearTimeout(timeout.current);
    updatePos();
    setOpen(true);
  }, [updatePos]);

  const hide = useCallback(() => {
    if (isTouchRef.current) return;
    timeout.current = window.setTimeout(() => setOpen(false), 120);
  }, []);

  const handleTouchEnd = useCallback(
    (e: React.TouchEvent) => {
      e.preventDefault();
      isTouchRef.current = true;
      updatePos();
      setOpen((v) => !v);
    },
    [updatePos],
  );

  const handleClick = useCallback(
    (e: React.MouseEvent) => {
      if (isTouchRef.current) {
        e.preventDefault();
        return;
      }
      updatePos();
      setOpen((v) => !v);
    },
    [updatePos],
  );

  // Close on outside tap (touch devices)
  useEffect(() => {
    if (!open) return;
    const onTouch = (e: TouchEvent) => {
      if (ref.current && !ref.current.contains(e.target as Node)) {
        setOpen(false);
      }
    };
    document.addEventListener("touchstart", onTouch, { passive: true });
    return () => document.removeEventListener("touchstart", onTouch);
  }, [open]);

  // Update position on scroll/resize while open
  useEffect(() => {
    if (!open) return;
    const onUpdate = () => updatePos();
    window.addEventListener("scroll", onUpdate, { passive: true, capture: true });
    window.addEventListener("resize", onUpdate, { passive: true });
    return () => {
      window.removeEventListener("scroll", onUpdate, true);
      window.removeEventListener("resize", onUpdate);
    };
  }, [open, updatePos]);

  const tooltip =
    open && pos
      ? createPortal(
          <div
            role="tooltip"
            style={{
              position: "absolute",
              top: pos.top - 6,
              left: pos.left,
              transform: "translateX(-50%) translateY(-100%)",
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
              zIndex: 10000,
              pointerEvents: "none",
              whiteSpace: "normal",
            }}
          >
            {text}
          </div>,
          document.body,
        )
      : null;

  return (
    <span style={{ position: "relative", display: "inline-flex", alignItems: "center" }}>
      <button
        ref={ref}
        type="button"
        aria-label={text}
        onClick={handleClick}
        onTouchEnd={handleTouchEnd}
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
      {tooltip}
    </span>
  );
}
