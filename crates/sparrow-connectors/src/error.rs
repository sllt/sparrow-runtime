use sparrow_model::ErrorCode;

#[derive(Debug)]
pub struct ConnectorError {
    pub code: ErrorCode,
    pub message: String,
}

impl ConnectorError {
    pub fn new(code: ErrorCode, message: impl Into<String>) -> Self {
        Self {
            code,
            message: message.into(),
        }
    }

    pub fn code(&self) -> ErrorCode {
        self.code
    }
}

impl std::fmt::Display for ConnectorError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}: {}", self.code, self.message)
    }
}

impl std::error::Error for ConnectorError {}

impl From<ConnectorError> for sparrow_model::SparrowError {
    fn from(err: ConnectorError) -> Self {
        sparrow_model::SparrowError::new(err.code, err.message)
    }
}

pub type Result<T> = std::result::Result<T, ConnectorError>;
