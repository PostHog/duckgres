import { useEffect, useRef } from "react";
import { mountPeepernetes } from "./nodes/peepernetes";
import "./nodes/peepernetes.css";

// Nodes hosts the peepernetes live cluster visualizer. It's an imperative,
// self-contained DOM/animation view (a port of the standalone tool), so the
// React layer is just a full-height mount point: build the view into the div on
// mount, tear it down on unmount. It intentionally skips PageHeader/PageBody —
// the view owns its own header, scroll area, and event ticker and fills the
// whole content region like a wall display.
export function Nodes() {
  const ref = useRef<HTMLDivElement>(null);
  useEffect(() => {
    if (!ref.current) return;
    return mountPeepernetes(ref.current);
  }, []);
  return <div ref={ref} className="h-full min-h-0" />;
}
