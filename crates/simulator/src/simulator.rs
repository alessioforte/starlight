use crate::models::Model;

pub struct Simulator {
    pub models: Vec<Box<dyn Model>>,
}

impl Simulator {
    pub fn new(models: Vec<Box<dyn Model>>) -> Self {
        Simulator { models }
    }

    pub fn add_model(&mut self, model: Box<dyn Model>) {
        self.models.push(model);
    }

    pub fn generate(&mut self) -> f64 {
        let current_time: u128 = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .expect("Time went backwards")
            .as_millis(); // Get current time in milliseconds since epoch

        let value = self
            .models
            .iter_mut()
            .map(|model| model.generate(current_time))
            .sum();

        value
    }
}

impl Default for Simulator {
    fn default() -> Self {
        Simulator { models: Vec::new() }
    }
}

// use crate::models::Model;

// pub struct Simulator {
//     pub model: Box<dyn Model>,
// }

// impl Simulator {
//     pub fn new(model: Box<dyn Model>) -> Self {
//         Simulator { model }
//     }

//     pub fn generate(&mut self) -> f64 {
//         // let current_time: DateTime<Utc> = Utc::now();
//         let current_time: u128 = std::time::SystemTime::now()
//             .duration_since(std::time::UNIX_EPOCH)
//             .expect("Time went backwards")
//             .as_millis(); // Get current time in milliseconds since epoch

//         let value = self.model.generate(current_time);
//         value
//     }
// }
