use super::Model;
use rand::{Rng, SeedableRng};
use rand::rngs::StdRng;

/// Injects occasional spikes or dips around a baseline model.
/// The anomaly event is sampled independently each `generate` call.
pub struct AnomalyModel {
    /// Baseline generator for normal behavior.
    base: Box<dyn Model>,
    /// Probability (0.0..=1.0) of emitting an anomaly on each tick.
    probability: f64,
    /// Minimum absolute magnitude of anomaly.
    min_magnitude: f64,
    /// Maximum absolute magnitude of anomaly.
    max_magnitude: f64,
    /// If true, anomalies can be both spikes and dips.
    /// If false, anomalies are always positive spikes.
    bidirectional: bool,
    rng: StdRng,
}

impl AnomalyModel {
    pub fn new(
        base: Box<dyn Model>,
        probability: f64,
        min_magnitude: f64,
        max_magnitude: f64,
        bidirectional: bool,
    ) -> Self {
        Self::with_seed(base, probability, min_magnitude, max_magnitude, bidirectional, rand::random())
    }

    pub fn with_seed(
        base: Box<dyn Model>,
        probability: f64,
        min_magnitude: f64,
        max_magnitude: f64,
        bidirectional: bool,
        seed: u64,
    ) -> Self {
        Self {
            base,
            probability: probability.clamp(0.0, 1.0),
            min_magnitude: min_magnitude.min(max_magnitude),
            max_magnitude: max_magnitude.max(min_magnitude),
            bidirectional,
            rng: StdRng::seed_from_u64(seed),
        }
    }
}

impl Model for AnomalyModel {
    fn generate(&mut self, time: u128) -> f64 {
        let baseline = self.base.generate(time);

        let roll: f64 = self.rng.random();
        if roll < self.probability {
            let magnitude = self.rng.random_range(self.min_magnitude..=self.max_magnitude);
            let sign = if self.bidirectional {
                if self.rng.random::<bool>() { 1.0 } else { -1.0 }
            } else {
                1.0
            };
            baseline + sign * magnitude
        } else {
            baseline
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::models::RandomModel;

    #[test]
    fn test_anomaly_model_emits_baseline_or_anomaly() {
        let base = Box::new(RandomModel::new(0.0, 1.0));
        let mut model = AnomalyModel::new(base, 0.5, 5.0, 10.0, true);
        assert!(model.generate(0).is_finite());
    }

    #[test]
    fn test_anomaly_model_spike_only() {
        let base = Box::new(RandomModel::with_seed(0.0, 0.0, 0));
        let mut model = AnomalyModel::with_seed(base, 1.0, 5.0, 5.0, false, 0);
        assert!(model.generate(0) >= 5.0);
    }

    #[test]
    fn test_reproducible_with_same_seed() {
        let make = || {
            let base = Box::new(RandomModel::with_seed(0.0, 1.0, 7));
            AnomalyModel::with_seed(base, 0.3, 5.0, 10.0, true, 42)
        };
        let mut a = make();
        let mut b = make();
        for t in 0..20 {
            assert_eq!(a.generate(t), b.generate(t));
        }
    }
}
