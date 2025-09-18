use serde::{Deserialize, Serialize};
use serde_yml;
use std::collections::HashMap;

#[derive(Debug, Clone, Serialize, Deserialize)]
struct Config {
    current_context: String,
    contexts: HashMap<String, Context>,
}

impl Default for Config {
    fn default() -> Self {
        Self {
            current_context: String::new(),
            contexts: HashMap::new(),
        }
    }
}

#[derive(Serialize, Deserialize, Clone, Debug)]
pub struct Context {
    pub name: String,
    pub description: String,
    pub endpoint: String,
    pub api_key: String,
}

impl Default for Context {
    fn default() -> Self {
        Self {
            name: "default".to_string(),
            description: "".to_string(),
            endpoint: "http://localhost:8246".to_string(),
            api_key: "".to_string(),
        }
    }
}

impl Context {
    pub fn new(
        name: String,
        endpoint: String,
        api_key: Option<String>,
        description: Option<String>,
    ) -> Self {
        Self {
            name,
            description: description.unwrap_or_default(),
            endpoint,
            api_key: api_key.unwrap_or_default(),
        }
    }

    pub fn name(&self) -> &str {
        &self.name
    }

    pub fn endpoint(&self) -> &str {
        &self.endpoint
    }

    pub fn api_key(&self) -> &str {
        &self.api_key
    }
}

pub fn get_contexts() -> Result<Vec<Context>, std::io::Error> {
    let config = get_config()?;
    let contexts: Vec<Context> = config
        .contexts
        .into_iter()
        .map(|(_, context)| context)
        .collect();
    Ok(contexts)
}

pub fn get_current_context() -> Result<Context, std::io::Error> {
    let config = get_config()?;
    if let Some(context) = config.contexts.get(&config.current_context) {
        Ok(context.clone())
    } else {
        Err(std::io::Error::new(
            std::io::ErrorKind::NotFound,
            "Current context not found",
        ))
    }
}

pub fn set_current_context(name: &str) -> Result<(), std::io::Error> {
    let config = get_config()?;
    if config.contexts.contains_key(name) {
        let mut new_config = config;
        new_config.current_context = name.to_string();
        save_config(&new_config)?;
        Ok(())
    } else {
        Err(std::io::Error::new(
            std::io::ErrorKind::NotFound,
            "Context not found",
        ))
    }
}

pub fn add_context(context: Context) -> Result<(), std::io::Error> {
    let mut config = get_config()?;
    config.contexts.insert(context.name.clone(), context);
    save_config(&config)?;
    Ok(())
}

pub fn remove_context(name: &str) -> Result<(), std::io::Error> {
    let config = get_config()?;
    if config.contexts.contains_key(name) {
        let mut new_config = config;
        new_config.contexts.remove(name);
        if new_config.current_context == name {
            new_config.current_context = String::new(); // Reset current context if removed
        }
        save_config(&new_config)?;
        Ok(())
    } else {
        Err(std::io::Error::new(
            std::io::ErrorKind::NotFound,
            "Context not found",
        ))
    }
}

fn save_config(config: &Config) -> Result<(), std::io::Error> {
    let content = serde_yml::to_string(config)
        .map_err(|e| std::io::Error::new(std::io::ErrorKind::InvalidData, e))?;
    std::fs::write(get_config_path(), content)?;
    Ok(())
}

fn get_config() -> Result<Config, std::io::Error> {
    let path = get_config_path();
    let content = std::fs::read_to_string(&path)?;
    serde_yml::from_str(&content)
        .map_err(|e| std::io::Error::new(std::io::ErrorKind::InvalidData, e))
}

fn get_config_path() -> String {
    let home_dir = std::env::var("HOME").unwrap_or_else(|_| ".".to_string());
    let base_path = std::env::var("CLI_DIR").unwrap_or_else(|_| ".starlight/cli".to_string());
    let dir = std::path::Path::new(&home_dir).join(base_path);
    let filepath = format!("{}/config.yaml", dir.display());
    if !std::path::Path::new(&filepath).exists() {
        std::fs::create_dir_all(std::path::Path::new(&dir))
            .expect("Unable to create CLI directory");
        let default_config = Config::default();
        let content =
            serde_yml::to_string(&default_config).expect("Unable to serialize default config");
        std::fs::write(&filepath, content).expect("Unable to write default config file");
    }
    filepath
}
