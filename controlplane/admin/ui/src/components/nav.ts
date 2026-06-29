import {
  Activity,
  Building2,
  Database,
  GaugeCircle,
  LayoutDashboard,
  LineChart,
  ScrollText,
  Server,
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
  { to: "/users", label: "Users", icon: Users },
  { to: "/live", label: "Live", icon: Activity },
  { to: "/workers", label: "Workers", icon: Server },
  { to: "/metrics", label: "Metrics", icon: LineChart },
  { to: "/configstore", label: "Config Store", icon: Database },
  { to: "/impersonate", label: "Impersonate", icon: TerminalSquare, adminOnly: true },
  { to: "/audit", label: "Audit", icon: ScrollText, adminOnly: true },
];

export const BRAND_ICON = GaugeCircle;
