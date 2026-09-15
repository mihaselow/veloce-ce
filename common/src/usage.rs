use serde::{Deserialize, Serialize};
use std::collections::HashMap;

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct UsageTracker {
    pub usage: HashMap<String, f64>,
    pub last_decay: u64,
}

impl Default for UsageTracker {
    fn default() -> Self {
        Self::new()
    }
}

impl UsageTracker {
    pub fn new() -> Self {
        Self {
            usage: HashMap::new(),
            last_decay: std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_secs(),
        }
    }

    pub fn accrue(&mut self, user: &str, cost: f64) {
        let entry = self.usage.entry(user.to_string()).or_insert(0.0);
        *entry += cost;
    }

    pub fn get_usage(&self, user: &str) -> f64 {
        *self.usage.get(user).unwrap_or(&0.0)
    }

    pub fn apply_decay(&mut self) {
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_secs();
        let elapsed = now.saturating_sub(self.last_decay);

        if elapsed > 60 {
            // Decay every minute in tests/normal use
            let actual_factor = 0.9f64.powf(elapsed as f64 / 60.0);
            for usage in self.usage.values_mut() {
                *usage *= actual_factor;
            }
            self.last_decay = now;
        }
    }
}
