use regex::Regex;
use tokio::time::Duration;

#[derive(Debug)]
pub enum ParseDurationError {
    InvalidFormat,
    InvalidNumber,
    UnknownUnit,
}

pub trait DurationExt {
    fn parse(input: &str) -> Result<Self, ParseDurationError>
    where
        Self: Sized;
}

impl DurationExt for Duration {
    fn parse(input: &str) -> Result<Self, ParseDurationError> {
        let re = Regex::new(r"(?i)^(\d+)(s|m|h|d|w|ns|us|ms)$")
            .map_err(|_| ParseDurationError::InvalidFormat)?;
        if let Some(caps) = re.captures(input.trim()) {
            let value: u64 = caps[1]
                .parse()
                .map_err(|_| ParseDurationError::InvalidNumber)?;
            let unit = &caps[2].to_lowercase();

            match unit.as_str() {
                "ns" => Ok(Duration::from_nanos(value)),
                "us" => Ok(Duration::from_micros(value)),
                "ms" => Ok(Duration::from_millis(value)),
                "s" => Ok(Duration::from_secs(value)),
                "m" => Ok(Duration::from_secs(value * 60)),
                "d" => Ok(Duration::from_secs(value * 60 * 60 * 24)),
                "h" => Ok(Duration::from_secs(value * 60 * 60)),
                "w" => Ok(Duration::from_secs(value * 60 * 60 * 24 * 7)),
                _ => Err(ParseDurationError::UnknownUnit),
            }
        } else {
            Err(ParseDurationError::InvalidFormat)
        }
    }
}
