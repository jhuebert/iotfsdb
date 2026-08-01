import { BrowserRouter, Navigate, Route, Routes } from "react-router";
import { AppShell } from "./app-shell";
import { SeriesPage } from "@/features/series/series-page";
import { DataPage } from "@/features/data/data-page";
import { TransferPage } from "@/features/transfer/transfer-page";
import { NotFoundPage } from "./not-found";

export function AppRouter() {
  return (
    <BrowserRouter>
      <Routes>
        <Route element={<AppShell />}>
          <Route index element={<Navigate to="/series" replace />} />
          <Route path="/series" element={<SeriesPage />} />
          <Route path="/data" element={<DataPage />} />
          <Route path="/transfer" element={<TransferPage />} />
          <Route path="*" element={<NotFoundPage />} />
        </Route>
      </Routes>
    </BrowserRouter>
  );
}
