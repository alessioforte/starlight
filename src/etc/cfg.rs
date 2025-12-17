// use engine::Engine;
use eng::Engine;
use std::sync::Arc;
use tokio::sync::Mutex;

#[derive(Clone)]
pub struct AppState {
    pub engine: Arc<Mutex<Engine>>,
}
