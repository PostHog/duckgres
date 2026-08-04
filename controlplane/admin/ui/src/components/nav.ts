import {
  Activity,
  AlertTriangle,
  ArrowLeftRight,
  Building2,
  Database,
  Layers,
  LayoutDashboard,
  LineChart,
  Network,
  ScrollText,
  Server,
  ShieldCheck,
  TerminalSquare,
  Users,
  type LucideIcon,
} from "lucide-react";

export interface NavItem {
  to: string;
  label: string;
  icon: LucideIcon;
  adminOnly?: boolean;
  end?: boolean;
}

export const NAV: NavItem[] = [
  { to: "/", label: "Overview", icon: LayoutDashboard, end: true },
  { to: "/orgs", label: "Organizations", icon: Building2 },
  { to: "/org-teams", label: "Org teams", icon: Layers },
  { to: "/users", label: "Org Users", icon: Users },
  { to: "/operators", label: "Operators", icon: ShieldCheck, adminOnly: true },
  { to: "/live", label: "Live", icon: Activity },
  { to: "/errors", label: "Errors", icon: AlertTriangle },
  { to: "/nodes", label: "Nodes", icon: Network },
  { to: "/workers", label: "Workers", icon: Server },
  { to: "/metrics", label: "Metrics", icon: LineChart },
  { to: "/reshards", label: "Reshards", icon: ArrowLeftRight },
  { to: "/configstore", label: "Config Store", icon: Database },
  { to: "/impersonate", label: "Impersonate", icon: TerminalSquare, adminOnly: true },
  { to: "/audit", label: "Audit", icon: ScrollText, adminOnly: true },
];
