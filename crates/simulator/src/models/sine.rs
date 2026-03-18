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
        // Treat `frequency` as Hz (cycles per second) and `time` as milliseconds.
        let t = time as f64 / 1000.0;
        let omega = 2.0 * std::f64::consts::PI * self.frequency;

        self.amplitude * (omega * t + self.phase).sin()
    }
}
