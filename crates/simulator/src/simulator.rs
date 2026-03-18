use crate::models::Model;

pub struct Simulator {
    pub models: Vec<Box<dyn Model>>,
    current_time_ms: u128,
    step_ms: u128,
}

impl Simulator {
    pub fn new(models: Vec<Box<dyn Model>>) -> Self {
        Simulator {
            models,
            current_time_ms: Self::now_millis(),
            step_ms: 1000,
        }
    }

    pub fn with_step(models: Vec<Box<dyn Model>>, start_time_ms: u128, step_ms: u128) -> Self {
        Simulator {
            models,
            current_time_ms: start_time_ms,
            step_ms,
        }
    }

    pub fn add_model(&mut self, model: Box<dyn Model>) {
        self.models.push(model);
    }

    pub fn set_step_ms(&mut self, step_ms: u128) {
        self.step_ms = step_ms;
    }

    pub fn current_time_ms(&self) -> u128 {
        self.current_time_ms
    }

    pub fn generate_at(&mut self, time_ms: u128) -> f64 {
        self.models
            .iter_mut()
            .map(|model| model.generate(time_ms))
            .sum()
    }

    pub fn tick(&mut self) -> f64 {
        let value = self.generate_at(self.current_time_ms);
        self.current_time_ms = self.current_time_ms.saturating_add(self.step_ms);
        value
    }

    pub fn generate(&mut self) -> f64 {
        self.current_time_ms = Self::now_millis();
        self.generate_at(self.current_time_ms)
    }

    fn now_millis() -> u128 {
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .expect("Time went backwards")
            .as_millis()
    }
}

impl Default for Simulator {
    fn default() -> Self {
        Simulator {
            models: Vec::new(),
            current_time_ms: 0,
            step_ms: 1000,
        }
    }
}
