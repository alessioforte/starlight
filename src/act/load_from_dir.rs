// use engine::{Config, Engine};
use eng::{Config, Engine};
use std::sync::Arc;
use tokio::sync::Mutex;

pub async fn load_workflow_from_dir(engine: &Arc<Mutex<Engine>>) {
    let dir = std::env::var("ENGINE_DIR").unwrap_or_else(|_| ".starlight/engine".to_string());
    let mut engine = engine.lock().await;
    let path_dir = format!("{}/workflows", dir);
    if std::path::Path::new(&path_dir).exists() {
        let paths = std::fs::read_dir(&path_dir).unwrap();
        for path in paths {
            let path = path.unwrap().path();
            if let Some(ext) = path.extension() {
                let file = std::fs::File::open(path.clone()).expect("file not found");
                if ext != "json" && ext != "yaml" {
                    continue;
                }
                let config = if ext == "json" {
                    let cfg: Config = serde_json::from_reader(file)
                        .expect("error while reading JSON config file");
                    cfg
                } else if ext == "yaml" {
                    let cfg: Config = serde_yaml_bw::from_reader(file)
                        .expect("error while reading YAML config file");
                    cfg
                } else {
                    continue;
                };
                match engine.add(config) {
                    Ok(_) => {
                        tracing::info!("Loaded workflow from file: {}", path.to_string_lossy());
                    }
                    Err(e) => {
                        tracing::error!(
                            "Error loading workflow from file {}: {}",
                            path.to_string_lossy(),
                            e
                        );
                    }
                }
            } else {
                continue;
            }
        }
    }
}
