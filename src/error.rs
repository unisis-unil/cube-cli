use serde_json::json;
use std::fmt;

#[derive(Debug)]
pub struct CubeError {
    pub code: u16,
    pub message: String,
    pub reason: String,
}

impl CubeError {
    pub fn validation(message: impl Into<String>) -> anyhow::Error {
        CubeError {
            code: 400,
            message: message.into(),
            reason: "validationError".to_string(),
        }
        .into()
    }

    pub fn not_found(message: impl Into<String>) -> anyhow::Error {
        CubeError {
            code: 404,
            message: message.into(),
            reason: "notFound".to_string(),
        }
        .into()
    }

    pub fn unavailable(message: impl Into<String>) -> anyhow::Error {
        CubeError {
            code: 503,
            message: message.into(),
            reason: "exportInProgress".to_string(),
        }
        .into()
    }
}

impl fmt::Display for CubeError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.message)
    }
}

impl std::error::Error for CubeError {}

pub fn print_json_error(err: &anyhow::Error) {
    let (code, reason) = if let Some(ce) = err.downcast_ref::<CubeError>() {
        (ce.code, ce.reason.as_str())
    } else {
        (500, "internalError")
    };

    let output = json!({
        "error": {
            "code": code,
            "message": format!("{err:#}"),
            "reason": reason,
        }
    });
    eprintln!("{}", serde_json::to_string_pretty(&output).unwrap());
}
