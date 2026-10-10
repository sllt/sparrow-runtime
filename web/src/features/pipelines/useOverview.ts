import { useMemo } from "react";
import { usePoll } from "../../api/usePoll";
import { loadOverview } from "../../api/overview";
import { deriveView, isFresh, type PipelineView } from "../../lib/health";
import { errorCopy } from "../../components/ui";
import { useRates } from "./useRates";

/** Overview sweep every 10 s (bounded: ≤50 status reads, concurrency 4). */
export function useOverview() {
  const [state, refresh] = usePoll(loadOverview, "overview", 10_000);
  const fresh = isFresh(state, Date.now());
  const { views, errors } = useMemo(() => {
    const views: PipelineView[] = [];
    const errors: Record<string, string> = {};
    for (const name of state.data?.names ?? []) {
      const s = state.data!.status[name];
      if (s && "body" in s) views.push(deriveView(name, s.body, fresh));
      else {
        const v = deriveView(name, null, fresh);
        if (s && "error" in s) {
          if (s.error.kind === "forbidden") v.health = "denied";
          errors[name] = errorCopy(s.error).title;
        }
        views.push(v);
      }
    }
    return { views, errors };
  }, [state.data, fresh]);
  const rates = useRates(views, state.data?.at ?? null);
  return { state, refresh, views, errors, rates };
}
