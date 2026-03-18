use super::Model;

// ── Sum ───────────────────────────────────────────────────────────────────────

/// Adds the outputs of N models together: `y = m1(t) + m2(t) + … + mN(t)`.
///
/// The identity value for an empty list is `0.0`.
pub struct SumModel {
    models: Vec<Box<dyn Model>>,
}

impl SumModel {
    pub fn new(models: Vec<Box<dyn Model>>) -> Self {
        Self { models }
    }
}

impl Model for SumModel {
    fn generate(&mut self, time: u128) -> f64 {
        self.models.iter_mut().map(|m| m.generate(time)).sum()
    }
}

// ── Product ───────────────────────────────────────────────────────────────────

/// Multiplies two model outputs: `y = a(t) * b(t)`.
///
/// The primary use case is **amplitude modulation**: one model acts as an
/// envelope that scales the amplitude of another over time.
///
/// ```text
/// ProductModel(TrendModel::exponential(1.0, -0.2), SineModel(50.0, 1.0, 0.0))
/// → decaying oscillation (damped sine wave)
/// ```
pub struct ProductModel {
    a: Box<dyn Model>,
    b: Box<dyn Model>,
}

impl ProductModel {
    pub fn new(a: Box<dyn Model>, b: Box<dyn Model>) -> Self {
        Self { a, b }
    }
}

impl Model for ProductModel {
    fn generate(&mut self, time: u128) -> f64 {
        self.a.generate(time) * self.b.generate(time)
    }
}

// ── Max ───────────────────────────────────────────────────────────────────────

/// Returns the maximum output across N models: `y = max(m1(t), m2(t), …)`.
///
/// Panics at construction if `models` is empty.
pub struct MaxModel {
    models: Vec<Box<dyn Model>>,
}

impl MaxModel {
    pub fn new(models: Vec<Box<dyn Model>>) -> Self {
        assert!(!models.is_empty(), "MaxModel requires at least one model");
        Self { models }
    }
}

impl Model for MaxModel {
    fn generate(&mut self, time: u128) -> f64 {
        self.models
            .iter_mut()
            .map(|m| m.generate(time))
            .fold(f64::NEG_INFINITY, f64::max)
    }
}

// ── Min ───────────────────────────────────────────────────────────────────────

/// Returns the minimum output across N models: `y = min(m1(t), m2(t), …)`.
///
/// Panics at construction if `models` is empty.
pub struct MinModel {
    models: Vec<Box<dyn Model>>,
}

impl MinModel {
    pub fn new(models: Vec<Box<dyn Model>>) -> Self {
        assert!(!models.is_empty(), "MinModel requires at least one model");
        Self { models }
    }
}

impl Model for MinModel {
    fn generate(&mut self, time: u128) -> f64 {
        self.models
            .iter_mut()
            .map(|m| m.generate(time))
            .fold(f64::INFINITY, f64::min)
    }
}

// ── Scale ─────────────────────────────────────────────────────────────────────

/// Multiplies a model's output by a constant: `y = factor * inner(t)`.
pub struct ScaleModel {
    inner: Box<dyn Model>,
    factor: f64,
}

impl ScaleModel {
    pub fn new(inner: Box<dyn Model>, factor: f64) -> Self {
        Self { inner, factor }
    }
}

impl Model for ScaleModel {
    fn generate(&mut self, time: u128) -> f64 {
        self.inner.generate(time) * self.factor
    }
}

// ── Clamp ─────────────────────────────────────────────────────────────────────

/// Constrains a model's output to `[min, max]`: `y = clamp(inner(t), min, max)`.
///
/// Essential for keeping simulated values physically meaningful (e.g. CPU in
/// [0, 100], latency ≥ 0).
///
/// Panics at construction if `min > max`.
pub struct ClampModel {
    inner: Box<dyn Model>,
    min: f64,
    max: f64,
}

impl ClampModel {
    pub fn new(inner: Box<dyn Model>, min: f64, max: f64) -> Self {
        assert!(min <= max, "ClampModel: min ({min}) must be ≤ max ({max})");
        Self { inner, min, max }
    }
}

impl Model for ClampModel {
    fn generate(&mut self, time: u128) -> f64 {
        self.inner.generate(time).clamp(self.min, self.max)
    }
}

// ── Tests ─────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;
    use crate::models::{SineModel, TrendModel};

    fn constant(v: f64) -> Box<dyn Model> {
        // TrendModel::linear(0, v) → y = 0*t + v = v always
        Box::new(TrendModel::linear(0.0, v))
    }

    // ── SumModel ──────────────────────────────────────────────────────────────

    #[test]
    fn test_sum_empty_is_zero() {
        let mut m = SumModel::new(vec![]);
        assert_eq!(m.generate(0), 0.0);
    }

    #[test]
    fn test_sum_single() {
        let mut m = SumModel::new(vec![constant(7.0)]);
        assert_eq!(m.generate(0), 7.0);
    }

    #[test]
    fn test_sum_multiple() {
        let mut m = SumModel::new(vec![constant(3.0), constant(4.0), constant(-1.0)]);
        assert_eq!(m.generate(0), 6.0);
    }

    // ── ProductModel ──────────────────────────────────────────────────────────

    #[test]
    fn test_product_basic() {
        let mut m = ProductModel::new(constant(3.0), constant(4.0));
        assert_eq!(m.generate(0), 12.0);
    }

    #[test]
    fn test_product_zero_factor() {
        let mut m = ProductModel::new(constant(0.0), constant(999.0));
        assert_eq!(m.generate(0), 0.0);
    }

    #[test]
    fn test_product_negative() {
        let mut m = ProductModel::new(constant(-1.0), constant(5.0));
        assert_eq!(m.generate(0), -5.0);
    }

    #[test]
    fn test_product_modulates_amplitude() {
        // Envelope at t=0: e^0 = 1.0; sine at t=0: 0.0 → product = 0.0
        let envelope = Box::new(TrendModel::exponential(1.0, -0.5));
        let wave = Box::new(SineModel::new(10.0, 1.0, 0.0));
        let mut m = ProductModel::new(envelope, wave);
        assert!(m.generate(0).abs() < 1e-10); // sin(0) = 0
    }

    // ── MaxModel ──────────────────────────────────────────────────────────────

    #[test]
    fn test_max_single() {
        let mut m = MaxModel::new(vec![constant(5.0)]);
        assert_eq!(m.generate(0), 5.0);
    }

    #[test]
    fn test_max_picks_largest() {
        let mut m = MaxModel::new(vec![constant(1.0), constant(9.0), constant(3.0)]);
        assert_eq!(m.generate(0), 9.0);
    }

    #[test]
    #[should_panic(expected = "MaxModel requires at least one model")]
    fn test_max_empty_panics() {
        MaxModel::new(vec![]);
    }

    // ── MinModel ──────────────────────────────────────────────────────────────

    #[test]
    fn test_min_single() {
        let mut m = MinModel::new(vec![constant(5.0)]);
        assert_eq!(m.generate(0), 5.0);
    }

    #[test]
    fn test_min_picks_smallest() {
        let mut m = MinModel::new(vec![constant(1.0), constant(9.0), constant(3.0)]);
        assert_eq!(m.generate(0), 1.0);
    }

    #[test]
    #[should_panic(expected = "MinModel requires at least one model")]
    fn test_min_empty_panics() {
        MinModel::new(vec![]);
    }

    // ── ScaleModel ────────────────────────────────────────────────────────────

    #[test]
    fn test_scale_doubles() {
        let mut m = ScaleModel::new(constant(5.0), 2.0);
        assert_eq!(m.generate(0), 10.0);
    }

    #[test]
    fn test_scale_zero() {
        let mut m = ScaleModel::new(constant(999.0), 0.0);
        assert_eq!(m.generate(0), 0.0);
    }

    #[test]
    fn test_scale_negate() {
        let mut m = ScaleModel::new(constant(3.0), -1.0);
        assert_eq!(m.generate(0), -3.0);
    }

    // ── ClampModel ────────────────────────────────────────────────────────────

    #[test]
    fn test_clamp_within_bounds() {
        let mut m = ClampModel::new(constant(50.0), 0.0, 100.0);
        assert_eq!(m.generate(0), 50.0);
    }

    #[test]
    fn test_clamp_above_max() {
        let mut m = ClampModel::new(constant(150.0), 0.0, 100.0);
        assert_eq!(m.generate(0), 100.0);
    }

    #[test]
    fn test_clamp_below_min() {
        let mut m = ClampModel::new(constant(-30.0), 0.0, 100.0);
        assert_eq!(m.generate(0), 0.0);
    }

    #[test]
    #[should_panic(expected = "min")]
    fn test_clamp_invalid_range_panics() {
        ClampModel::new(constant(0.0), 100.0, 0.0);
    }

    // ── Nested composition ────────────────────────────────────────────────────

    #[test]
    fn test_nested_product_inside_sum() {
        // (3 * 4) + 5 = 17
        let product = Box::new(ProductModel::new(constant(3.0), constant(4.0)));
        let mut m = SumModel::new(vec![product, constant(5.0)]);
        assert_eq!(m.generate(0), 17.0);
    }

    #[test]
    fn test_clamp_applied_to_sum() {
        // sum = 200 + 50 = 250, clamped to [0, 100] → 100
        let sum = Box::new(SumModel::new(vec![constant(200.0), constant(50.0)]));
        let mut m = ClampModel::new(sum, 0.0, 100.0);
        assert_eq!(m.generate(0), 100.0);
    }
}
