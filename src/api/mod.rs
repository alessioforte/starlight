mod generate;
mod health;
mod workflows;

use crate::etc::cfg::AppState;

pub fn routes() -> axum::Router<AppState> {
    axum::Router::new()
        .route("/health", axum::routing::get(health::health))
        .merge(workflows::routes())
        .merge(generate::routes())
}
