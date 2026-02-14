import React from "react";
import { Routes, Route, Navigate } from "react-router-dom";
import { ScreenDesktop6 } from "../screens/ScreenDesktop6";
import { ScreenPlaceholder } from "../screens/ScreenPlaceholder";

/**
 * Routes map to Figma screens.
 * Desktop-6 is implemented pixel-accurately from Figma codegen.
 * Other screens are wired and can be swapped to exact Figma code once exported.
 */
export default function App() {
  return (
    <Routes>
      <Route path="/" element={<Navigate to="/dashboard/summary" replace />} />

      {/* Main (Figma Desktop-6) */}
      <Route path="/dashboard/summary" element={<ScreenDesktop6 />} />

      {/* Other screens (replace with exact Figma codegen per frame) */}
      <Route path="/dashboard/desktop-2" element={<ScreenPlaceholder title="Desktop - 2 (31:2)" />} />
      <Route path="/dashboard/desktop-3" element={<ScreenPlaceholder title="Desktop - 3 (35:112)" />} />
      <Route path="/dashboard/desktop-4" element={<ScreenPlaceholder title="Desktop - 4 (38:2)" />} />
      <Route path="/dashboard/desktop-5" element={<ScreenPlaceholder title="Desktop - 5 (38:158)" />} />
      <Route path="/dashboard/financial-breakdown" element={<ScreenPlaceholder title="Financial Breakdown (46:406)" />} />
      <Route path="/dashboard/revenue-breakdown" element={<ScreenPlaceholder title="Revenue Breakdown (56:1043)" />} />
      <Route path="/dashboard/parking" element={<ScreenPlaceholder title="Parking (64:1037)" />} />

      <Route path="*" element={<Navigate to="/dashboard/summary" replace />} />
    </Routes>
  );
}
