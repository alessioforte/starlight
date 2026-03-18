pub mod anomaly;
pub mod combinators;
pub mod random;
pub mod random_walk;
pub mod sine;
pub mod trend;

pub use anomaly::AnomalyModel;
pub use combinators::{ClampModel, MaxModel, MinModel, ProductModel, ScaleModel, SumModel};
pub use random::RandomModel;
pub use random_walk::RandomWalkModel;
pub use sine::SineModel;
pub use trend::TrendModel;

pub trait Model: Send {
    fn generate(&mut self, time: u128) -> f64;
}
