use super::Model;
use rand::rngs::StdRng;
use rand::{Rng, SeedableRng};

pub struct RandomWalkModel {
    pub current: f64,
    pub drift: f64,
    pub volatility: f64,
    rng: StdRng,
}

impl RandomWalkModel {
    pub fn new(current: f64, drift: f64, volatility: f64) -> Self {
        Self::with_seed(current, drift, volatility, rand::random())
    }

    pub fn with_seed(current: f64, drift: f64, volatility: f64, seed: u64) -> Self {
        Self {
            current,
            drift,
            volatility,
            rng: StdRng::seed_from_u64(seed),
        }
    }
}

impl Model for RandomWalkModel {
    fn generate(&mut self, _time: u128) -> f64 {
        let change = self.drift + self.rng.random_range(-self.volatility..self.volatility);
        self.current += change;
        self.current
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_reproducible_with_same_seed() {
        let mut a = RandomWalkModel::with_seed(0.0, 0.0, 1.0, 99);
        let mut b = RandomWalkModel::with_seed(0.0, 0.0, 1.0, 99);
        for t in 0..20 {
            assert_eq!(a.generate(t), b.generate(t));
        }
    }

    #[test]
    fn test_different_seeds_differ() {
        let mut a = RandomWalkModel::with_seed(0.0, 0.0, 1.0, 1);
        let mut b = RandomWalkModel::with_seed(0.0, 0.0, 1.0, 2);
        let values_a: Vec<f64> = (0..10).map(|t| a.generate(t)).collect();
        let values_b: Vec<f64> = (0..10).map(|t| b.generate(t)).collect();
        assert_ne!(values_a, values_b);
    }
}
