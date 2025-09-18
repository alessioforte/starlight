use super::Model;
use rand::Rng;

pub struct RandomWalkModel {
    pub current: f64,
    pub drift: f64,
    pub volatility: f64,
}

impl RandomWalkModel {
    pub fn new(current: f64, drift: f64, volatility: f64) -> Self {
        Self {
            current,
            drift,
            volatility,
        }
    }
}

impl Model for RandomWalkModel {
    fn generate(&mut self, _time: u128) -> f64 {
        // let random_step = rand::random::<f64>() * 2.0 - 1.0; // Random step between -1 and 1
        // self.current += self.drift + self.volatility * random_step;
        // self.current

        let change = self.drift + rand::rng().random_range(-self.volatility..self.volatility);
        self.current += change;
        self.current
    }
}
