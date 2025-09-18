use super::Model;
use rand_distr::{Distribution, Normal};

pub struct RandomModel {
    normal: Normal<f64>,
}

impl RandomModel {
    pub fn new(mean: f64, stddev: f64) -> Self {
        let normal = Normal::new(mean, stddev).unwrap();
        RandomModel { normal }
    }
}

impl Model for RandomModel {
    fn generate(&mut self, _time: u128) -> f64 {
        self.normal.sample(&mut rand::rng())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::SystemTime;

    #[test]
    fn test_random_model() {
        let mut model = RandomModel::new(0.0, 1.0);
        let time = SystemTime::now()
            .duration_since(SystemTime::UNIX_EPOCH)
            .unwrap()
            .as_millis();
        let value = model.generate(time);
        assert!(value.is_finite());
    }

    #[test]
    fn test_random_model_with_time() {
        let mut model = RandomModel::new(5.0, 2.0);
        let time = SystemTime::now()
            .duration_since(SystemTime::UNIX_EPOCH)
            .unwrap()
            .as_millis();
        let value = model.generate(time);
        assert!(value.is_finite());
    }
}
