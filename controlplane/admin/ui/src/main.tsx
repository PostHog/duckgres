import { StrictMode } from "react";
import { createRoot } from "react-dom/client";
import { BrowserRouter } from "react-router-dom";
import { QueryClientProvider } from "@tanstack/react-query";
import { TooltipProvider } from "@/components/ui/tooltip";
import { queryClient } from "@/lib/query";
import { IdentityProvider } from "@/components/IdentityProvider";
import App from "./App";
import "./index.css";

createRoot(document.getElementById("root")!).render(
  <StrictMode>
    <QueryClientProvider client={queryClient}>
      {/* base path is relative so the SPA works whether served at / or /admin */}
      <BrowserRouter>
        <IdentityProvider>
          <TooltipProvider delayDuration={150}>
            <App />
          </TooltipProvider>
        </IdentityProvider>
      </BrowserRouter>
    </QueryClientProvider>
  </StrictMode>,
);
