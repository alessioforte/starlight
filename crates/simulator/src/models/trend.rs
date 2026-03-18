use super::Model;

/// The shape of the trend to apply over time.
pub enum TrendKind {
    /// `y = slope * t + intercept`  (t in seconds)
    Linear { slope: f64, intercept: f64 },
    /// `y = initial * e^(rate * t)` (t in seconds)
    ///
    /// - `rate > 0` → exponential growth
    /// - `rate < 0` → exponential decay
    /// - `rate = 0` → constant (`initial`)
    Exponential { initial: f64, rate: f64 },
}

pub struct TrendModel {
    kind: TrendKind,
}

impl TrendModel {
    pub fn linear(slope: f64, intercept: f64) -> Self {
        Self { kind: TrendKind::Linear { slope, intercept } }
    }

    pub fn exponential(initial: f64, rate: f64) -> Self {
        Self { kind: TrendKind::Exponential { initial, rate } }
    }
}

impl Model for TrendModel {
    fn generate(&mut self, time: u128) -> f64 {
        let t = time as f64 / 1000.0; // ms → seconds
        match self.kind {
            TrendKind::Linear { slope, intercept } => slope * t + intercept,
            TrendKind::Exponential { initial, rate } => initial * (rate * t).exp(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_linear_at_origin() {
        let mut m = TrendModel::linear(2.0, 5.0);
        assert_eq!(m.generate(0), 5.0); // slope * 0 + intercept
    }

    #[test]
    fn test_linear_slope() {
        let mut m = TrendModel::linear(3.0, 0.0);
        // t = 2000 ms = 2 s → y = 3 * 2 = 6
        assert!((m.generate(2000) - 6.0).abs() < 1e-10);
    }

    #[test]
    fn test_linear_negative_slope() {
        let mut m = TrendModel::linear(-1.0, 100.0);
        // t = 10 s → y = -10 + 100 = 90
        assert!((m.generate(10_000) - 90.0).abs() < 1e-10);
    }

    #[test]
    fn test_exponential_at_origin() {
        let mut m = TrendModel::exponential(7.0, 0.5);
        // e^0 = 1 → y = 7
        assert_eq!(m.generate(0), 7.0);
    }

    #[test]
    fn test_exponential_growth() {
        let mut m = TrendModel::exponential(1.0, 1.0);
        // t = 1 s → y = e^1
        assert!((m.generate(1000) - std::f64::consts::E).abs() < 1e-10);
    }

    #[test]
    fn test_exponential_decay() {
        let mut m = TrendModel::exponential(100.0, -0.5);
        let v = m.generate(2000); // t = 2 s → 100 * e^(-1)
        assert!((v - 100.0 / std::f64::consts::E).abs() < 1e-10);
    }

    #[test]
    fn test_exponential_zero_rate_is_constant() {
        let mut m = TrendModel::exponential(42.0, 0.0);
        assert_eq!(m.generate(0), 42.0);
        assert_eq!(m.generate(999_999), 42.0);
    }

    #[test]
    fn test_linear_is_monotone_with_positive_slope() {
        let mut m = TrendModel::linear(1.0, 0.0);
        let values: Vec<f64> = (0..10).map(|i| m.generate(i * 1000)).collect();
        for w in values.windows(2) {
            assert!(w[1] > w[0]);
        }
    }
}
