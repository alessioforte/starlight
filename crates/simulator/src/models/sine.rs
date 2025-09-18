use super::Model;

pub struct SineModel {
    pub amplitude: f64,
    pub frequency: f64,
    pub phase: f64,
}

impl SineModel {
    pub fn new(amplitude: f64, frequency: f64, phase: f64) -> Self {
        SineModel {
            amplitude,
            frequency,
            phase,
        }
    }
}

impl Model for SineModel {
    fn generate(&mut self, time: u128) -> f64 {
        // Convert time to seconds (assuming time is in milliseconds
        // let t = time as f64 / 1000.0;
        let t = time as f64; // Convert timestamp to f64

        self.amplitude * (self.frequency * t + self.phase).sin()
        // self.amplitude * (2.0 * std::f64::consts::PI * t / self.frequency + self.phase).sin()
    }
}
