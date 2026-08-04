import { NavLink } from "react-router-dom";
import { Lock } from "lucide-react";
import { NAV } from "@/components/nav";
import { useIdentity } from "@/components/IdentityProvider";
import { cn } from "@/lib/utils";
import brandLogo from "@/assets/hotdog.png";

export function Sidebar() {
  const { isAdmin } = useIdentity();
  return (
    <aside className="flex w-56 shrink-0 flex-col border-r border-border bg-card/40">
      <div className="flex h-14 items-center gap-2 border-b border-border px-4">
        <img src={brandLogo} alt="Duckgres" className="mt-1 h-6 w-6" />
        <div className="leading-tight">
          <div className="text-sm font-semibold">Duckgres</div>
          <div className="text-[10px] uppercase tracking-widest text-muted-foreground">Control Plane</div>
        </div>
      </div>
      <nav className="flex-1 space-y-0.5 overflow-y-auto p-2">
        {NAV.map((item) => {
          const locked = item.adminOnly && !isAdmin;
          return (
            <NavLink
              key={item.to}
              to={item.to}
              end={item.end}
              className={({ isActive }) =>
                cn(
                  "group flex items-center gap-2.5 rounded-md px-2.5 py-2 text-sm font-medium transition-colors",
                  isActive
                    ? "bg-primary/15 text-primary"
                    : "text-muted-foreground hover:bg-accent hover:text-foreground",
                  locked && "opacity-50",
                )
              }
            >
              <item.icon className="h-4 w-4" />
              <span className="flex-1">{item.label}</span>
              {locked && <Lock className="h-3 w-3" />}
            </NavLink>
          );
        })}
      </nav>
      <div className="border-t border-border p-3 text-[10px] text-muted-foreground">
        Admin console · v0.1
      </div>
    </aside>
  );
}
