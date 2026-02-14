import React from "react";

export function ScreenPlaceholder(props: { title: string }) {
  return (
    <div className="min-h-screen bg-white flex items-center justify-center p-6">
      <div className="max-w-xl w-full border border-gray-200 rounded-xl p-6">
        <div className="text-xl font-semibold">{props.title}</div>
        <div className="text-sm text-gray-600 mt-2">
          This route is wired. To make it pixel-identical, export Figma design context for this frame ID and I’ll drop
          the generated component here.
        </div>
        <div className="text-sm text-gray-600 mt-2">
          You already have the full implementation for Desktop-6 at <code>/dashboard/summary</code>.
        </div>
      </div>
    </div>
  );
}
