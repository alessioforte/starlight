                    ┌──────────────────────┐
                    │    CoarseClock       │
                    │  (1 per workflow)    │
                    │  background task     │
                    │  tick ogni 100ms     │
                    │  AtomicI64 millis    │
                    └──────────┬───────────┘
                               │ .clone()
          ┌────────────────────┼─────────────────────┐
          ▼                    ▼                     ▼
    ┌───────────┐        ┌───────────┐         ┌───────────┐
    │ TaskCtx A │        │ TaskCtx B │         │ TaskCtx C │
    │ metrics ──┼──┐     │ metrics ──┼──┐      │ metrics ──┼──┐
    │ clock     │  │     │ clock     │  │      │ clock     │  │
    └───────────┘  │     └───────────┘  │      └───────────┘  │
                   ▼                    ▼                     ▼
              TaskMetrics          TaskMetrics           TaskMetrics
              (Arc shared          (Arc shared            (Arc shared
               w/ Input            w/ Input               w/ Input
               & Output)           & Output)              & Output)
