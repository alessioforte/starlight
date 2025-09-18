pub mod random;
pub mod random_walk;
pub mod sine;

pub use random::RandomModel;
pub use random_walk::RandomWalkModel;
pub use sine::SineModel;

pub trait Model: Send {
    fn generate(&mut self, time: u128) -> f64;
}
