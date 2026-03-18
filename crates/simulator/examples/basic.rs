use simulator::{
    Simulator,
    models::{
        AnomalyModel, ClampModel, MaxModel, ProductModel, RandomModel,
        RandomWalkModel, ScaleModel, SineModel, SumModel, TrendModel,
    },
};

fn main() {
    // ── 1. Pure sine wave ────────────────────────────────────────────────────
    println!("=== Sine wave (amplitude=10, freq=1Hz) ===");
    let mut sim = Simulator::with_step(
        vec![Box::new(SineModel::new(10.0, 1.0, 0.0))],
        0,
        250, // 250 ms step → 4 samples per second
    );
    for _ in 0..8 {
        println!("  t={:>6} ms  value={:>8.3}", sim.current_time_ms(), sim.tick());
    }

    // ── 2. Gaussian noise ────────────────────────────────────────────────────
    println!("\n=== Gaussian noise (mean=0, stddev=5) — seeded ===");
    let mut noisy = Simulator::with_step(
        vec![Box::new(RandomModel::with_seed(0.0, 5.0, 42))],
        0,
        1000,
    );
    for _ in 0..6 {
        println!("  t={:>6} ms  value={:>8.3}", noisy.current_time_ms(), noisy.tick());
    }

    // ── 3. Random walk ───────────────────────────────────────────────────────
    println!("\n=== Random walk (start=100, drift=0.5, volatility=3) — seeded ===");
    let mut walk = Simulator::with_step(
        vec![Box::new(RandomWalkModel::with_seed(100.0, 0.5, 3.0, 99))],
        0,
        1000,
    );
    for _ in 0..8 {
        println!("  t={:>6} ms  value={:>8.3}", walk.current_time_ms(), walk.tick());
    }

    // ── 4. Composed signal ───────────────────────────────────────────────────
    // sine baseline + gaussian noise + slow drift, all summed
    println!("\n=== Composed: sine + noise + drift ===");
    let mut composed = Simulator::with_step(
        vec![
            Box::new(SineModel::new(50.0, 1.0 / 10_000.0, 0.0)), // 1 cycle / 10 s
            Box::new(RandomModel::with_seed(0.0, 3.0, 7)),
            Box::new(RandomWalkModel::with_seed(0.0, 0.1, 1.0, 13)),
        ],
        0,
        1000,
    );
    for _ in 0..10 {
        println!("  t={:>6} ms  value={:>8.3}", composed.current_time_ms(), composed.tick());
    }

    // ── 5. Anomaly injection ─────────────────────────────────────────────────
    // Wrap the composed signal with occasional spikes
    println!("\n=== Anomaly injection (p=0.3, magnitude 20–40, bidirectional) ===");
    let base: Box<dyn simulator::models::Model> = Box::new(SineModel::new(50.0, 1.0 / 10_000.0, 0.0));
    let mut anomalous = Simulator::with_step(
        vec![Box::new(AnomalyModel::with_seed(base, 0.3, 20.0, 40.0, true, 55))],
        0,
        1000,
    );
    for _ in 0..12 {
        println!("  t={:>6} ms  value={:>8.3}", anomalous.current_time_ms(), anomalous.tick());
    }

    // ── 6. Linear trend ──────────────────────────────────────────────────────
    println!("\n=== Linear trend (slope=2/s, intercept=10) ===");
    let mut linear = Simulator::with_step(
        vec![Box::new(TrendModel::linear(2.0, 10.0))],
        0,
        1000,
    );
    for _ in 0..6 {
        println!("  t={:>6} ms  value={:>8.3}", linear.current_time_ms(), linear.tick());
    }

    // ── 7. Exponential decay + noise  ────────────────────────────────────────
    // Models a metric that initially spikes then decays back toward zero,
    // with realistic noise on top.
    println!("\n=== Exponential decay (initial=200, rate=-0.3/s) + noise ===");
    let mut decay = Simulator::with_step(
        vec![
            Box::new(TrendModel::exponential(200.0, -0.3)),
            Box::new(RandomModel::with_seed(0.0, 4.0, 77)),
        ],
        0,
        1000,
    );
    for _ in 0..8 {
        println!("  t={:>6} ms  value={:>8.3}", decay.current_time_ms(), decay.tick());
    }

    // ── 8. Realistic metric: linear growth + seasonality + noise ─────────────
    // "Requests per second" growing over time with a daily pattern and noise.
    println!("\n=== Realistic metric: growth + seasonality + noise ===");
    let mut realistic = Simulator::with_step(
        vec![
            Box::new(TrendModel::linear(0.005, 100.0)),          // slow growth
            Box::new(SineModel::new(20.0, 1.0 / 86_400.0, 0.0)), // 24 h seasonality
            Box::new(RandomModel::with_seed(0.0, 2.0, 31)),
        ],
        0,
        3_600_000, // 1 h steps
    );
    for _ in 0..12 {
        let t_h = realistic.current_time_ms() / 3_600_000;
        println!("  t={:>4} h  value={:>8.3}", t_h, realistic.tick());
    }

    // ── 9. ProductModel: decaying sine (damped oscillation) ──────────────────
    // Exponential envelope modulates the sine's amplitude over time.
    // Useful for modelling post-restart memory oscillations, spring-damping, etc.
    println!("\n=== ProductModel: decaying sine (envelope × oscillation) ===");
    let envelope = Box::new(TrendModel::exponential(50.0, -0.3)); // 50 * e^(-0.3t)
    let wave = Box::new(SineModel::new(1.0, 0.5, 0.0));           // unit sine at 0.5 Hz
    let mut damped = Simulator::with_step(
        vec![Box::new(ProductModel::new(envelope, wave))],
        0,
        500,
    );
    for _ in 0..8 {
        println!("  t={:>6} ms  value={:>8.3}", damped.current_time_ms(), damped.tick());
    }

    // ── 10. ClampModel: bounded CPU usage ────────────────────────────────────
    // A random walk that would drift below 0% or above 100% is clamped to [0, 100].
    println!("\n=== ClampModel: CPU usage bounded to [0, 100] ===");
    let unbounded = SumModel::new(vec![
        Box::new(TrendModel::linear(0.0, 70.0)),               // baseline at 70%
        Box::new(RandomWalkModel::with_seed(0.0, 0.5, 8.0, 5)), // volatile walk
    ]);
    let mut cpu = Simulator::with_step(
        vec![Box::new(ClampModel::new(Box::new(unbounded), 0.0, 100.0))],
        0,
        1000,
    );
    for _ in 0..10 {
        println!("  t={:>6} ms  cpu={:>6.1}%", cpu.current_time_ms(), cpu.tick());
    }

    // ── 11. ScaleModel + MaxModel: two services, take the busier one ──────────
    // Two independent load signals; MaxModel picks whichever is higher at each tick.
    println!("\n=== MaxModel: peak load across two services ===");
    let service_a = ScaleModel::new(
        Box::new(SineModel::new(1.0, 1.0 / 20_000.0, 0.0)),
        40.0,
    ); // 0..40
    let service_b = ScaleModel::new(
        Box::new(SineModel::new(1.0, 1.0 / 12_000.0, 1.5)),
        60.0,
    ); // 0..60, different phase
    let mut peak = Simulator::with_step(
        vec![Box::new(MaxModel::new(vec![
            Box::new(service_a),
            Box::new(service_b),
        ]))],
        0,
        2000,
    );
    for _ in 0..8 {
        println!("  t={:>6} ms  peak={:>7.2}", peak.current_time_ms(), peak.tick());
    }

    // ── 12. Full nested pipeline ──────────────────────────────────────────────
    // "p99 latency" — a realistic metric built from nested combinators:
    //   ClampModel(
    //     SumModel(
    //       TrendModel::linear(growth),       ← slow baseline increase
    //       ProductModel(                     ← amplitude-modulated noise
    //         TrendModel::exponential(spike), ← spike envelope decaying
    //         RandomModel(noise),
    //       ),
    //       AnomalyModel(SineModel(diurnal)), ← daily pattern with rare spikes
    //     ),
    //     min=1.0, max=5000.0,               ← latency can't be ≤0 or absurd
    //   )
    println!("\n=== Nested pipeline: p99 latency (ms) ===");
    let growth = Box::new(TrendModel::linear(0.002, 120.0));
    let spike_envelope = Box::new(TrendModel::exponential(30.0, -0.0005));
    let jitter = Box::new(RandomModel::with_seed(0.0, 1.0, 19));
    let modulated_noise = Box::new(ProductModel::new(spike_envelope, jitter));
    let diurnal_base = Box::new(SineModel::new(20.0, 1.0 / 86_400.0, -1.57));
    let diurnal = Box::new(AnomalyModel::with_seed(diurnal_base, 0.02, 200.0, 800.0, false, 88));
    let raw = SumModel::new(vec![growth, modulated_noise, diurnal]);
    let p99 = ClampModel::new(Box::new(raw), 1.0, 5000.0);
    let mut latency_sim = Simulator::with_step(vec![Box::new(p99)], 0, 3_600_000);
    for _ in 0..12 {
        let h = latency_sim.current_time_ms() / 3_600_000;
        println!("  t={:>3} h  p99={:>8.2} ms", h, latency_sim.tick());
    }

    // ── 13. Reproducibility demo ──────────────────────────────────────────────
    println!("\n=== Reproducibility: two identical seeded runs ===");
    let make_sim = || {
        Simulator::with_step(
            vec![
                Box::new(RandomModel::with_seed(0.0, 10.0, 1)) as Box<dyn simulator::models::Model>,
                Box::new(RandomWalkModel::with_seed(50.0, 0.0, 5.0, 2)),
            ],
            0,
            1000,
        )
    };
    let mut run_a = make_sim();
    let mut run_b = make_sim();
    let mut identical = true;
    for _ in 0..10 {
        let a = run_a.tick();
        let b = run_b.tick();
        if (a - b).abs() > f64::EPSILON {
            identical = false;
        }
    }
    println!("  Runs produce identical output: {identical}");
}
