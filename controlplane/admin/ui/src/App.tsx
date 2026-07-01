import { Navigate, Route, Routes } from "react-router-dom";
import { AppShell } from "@/components/AppShell";
import { useIdentity } from "@/components/IdentityProvider";
import { NotAuthorized } from "@/pages/NotAuthorized";
import { NotFound } from "@/pages/NotFound";
import { Overview } from "@/pages/Overview";
import { Orgs } from "@/pages/Orgs";
import { OrgDetail } from "@/pages/OrgDetail";
import { UsersPage } from "@/pages/Users";
import { Live } from "@/pages/Live";
import { Errors } from "@/pages/Errors";
import { Workers } from "@/pages/Workers";
import { Metrics } from "@/pages/Metrics";
import { ConfigStore } from "@/pages/ConfigStore";
import { Impersonate } from "@/pages/Impersonate";
import { Audit } from "@/pages/Audit";

export default function App() {
  const { unauthorized } = useIdentity();

  // A 401 from /me means the upstream ALB did not authenticate the request.
  if (unauthorized) {
    return <NotAuthorized />;
  }

  return (
    <AppShell>
      <Routes>
        <Route path="/" element={<Overview />} />
        <Route path="/orgs" element={<Orgs />} />
        <Route path="/orgs/:id" element={<OrgDetail />} />
        <Route path="/users" element={<UsersPage />} />
        <Route path="/live" element={<Live />} />
        <Route path="/errors" element={<Errors />} />
        <Route path="/workers" element={<Workers />} />
        <Route path="/metrics" element={<Metrics />} />
        <Route path="/configstore" element={<ConfigStore />} />
        <Route path="/impersonate" element={<Impersonate />} />
        <Route path="/audit" element={<Audit />} />
        <Route path="/404" element={<NotFound />} />
        <Route path="*" element={<Navigate to="/404" replace />} />
      </Routes>
    </AppShell>
  );
}
