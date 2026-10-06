"use client";

export function PrintButton({ label = "Print or save PDF" }: { label?: string }) {
  return (
    <button type="button" className="button quiet noPrint" onClick={() => window.print()}>
      {label}
    </button>
  );
}
