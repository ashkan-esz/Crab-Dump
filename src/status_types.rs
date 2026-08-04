use serde::{Serialize, Deserialize};

#[derive(Debug, Serialize, Deserialize, Clone, Copy)]
pub enum ServiceState {
    Up,          // Fully functional
    Degraded,    // Functional, but with temporary issues (e.g., rate limit)
    Down,         // Critical failure or missing dependency
}

impl ToString for ServiceState {
    fn to_string(&self) -> String {
        match self {
            ServiceState::Up => "UP".to_string(),
            ServiceState::Degraded => "DEGRADED".to_string(),
            ServiceState::Down => "DOWN".to_string(),
        }
    }
}

#[derive(Debug, Serialize, Deserialize)]
pub struct OperationalStatus {
    pub service_name: String,
    pub state: ServiceState,
    pub message: String, // Detailed explanation of the current status
    pub timestamp: String,
}

// Helper function to create a standard status object
pub fn new_status(service_name: &str, state: ServiceState, message: impl Into<String>) -> OperationalStatus {
    OperationalStatus {
        service_name: service_name.to_string(),
        state,
        message: message.into(),
        timestamp: chrono::Utc::now().to_rfc3339(),
    }
}