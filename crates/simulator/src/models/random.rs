use super::Model;
use rand::SeedableRng;
use rand::rngs::StdRng;
use rand_distr::{Distribution, Normal};

pub struct RandomModel {
    normal: Normal<f64>,
    rng: StdRng,
}

impl RandomModel {
    pub fn new(mean: f64, stddev: f64) -> Self {
        Self::with_seed(mean, stddev, rand::random())
    }

    pub fn with_seed(mean: f64, stddev: f64, seed: u64) -> Self {
        RandomModel {
            normal: Normal::new(mean, stddev).unwrap(),
            rng: StdRng::seed_from_u64(seed),
        }
    }
}

impl Model for RandomModel {
    fn generate(&mut self, _time: u128) -> f64 {
        self.normal.sample(&mut self.rng)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_random_model() {
        let mut model = RandomModel::new(0.0, 1.0);
        assert!(model.generate(0).is_finite());
    }

    #[test]
    fn test_random_model_with_time() {
        let mut model = RandomModel::new(5.0, 2.0);
        assert!(model.generate(0).is_finite());
    }

    #[test]
    fn test_reproducible_with_same_seed() {
        let mut a = RandomModel::with_seed(0.0, 1.0, 42);
        let mut b = RandomModel::with_seed(0.0, 1.0, 42);
        for t in 0..20 {
            assert_eq!(a.generate(t), b.generate(t));
        }
    }

    #[test]
    fn test_different_seeds_differ() {
        let mut a = RandomModel::with_seed(0.0, 1.0, 1);
        let mut b = RandomModel::with_seed(0.0, 1.0, 2);
        let values_a: Vec<f64> = (0..10).map(|t| a.generate(t)).collect();
        let values_b: Vec<f64> = (0..10).map(|t| b.generate(t)).collect();
        assert_ne!(values_a, values_b);
    }
}
