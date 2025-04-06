use thiserror::Error;
use tokio::task::JoinError;

#[derive(Error, Debug)]
pub enum AppError {
    #[error("HTTP request failed: {0}")]
    Reqwest(#[from] reqwest::Error),

    #[error("Filesystem I/O error: {0}")]
    Io(#[from] std::io::Error),

    #[error("JSON serialization error: {0}")]
    SerdeJsonSerialize(#[from] serde_json::Error),

    #[error("SIMD JSON parsing error: {0}")]
    SimdJsonParse(#[from] simd_json::Error),

    #[error("API returned an error: retcode={retcode}, message={message}")]
    ApiError { retcode: i64, message: String },

    #[error("API response structure invalid: {0}")]
    ApiResponseInvalid(String),

    #[error("Data processing error: {0}")]
    Processing(String),

    #[error("Directory operation failed: {0}")]
    FsExtra(#[from] fs_extra::error::Error),

    #[error("Invalid argument: {0}")]
    Argument(String),

    #[error("Task join error: {0}")]
    JoinError(#[from] JoinError),

    #[error("Timeout during operation")]
    Timeout,

    #[error("Recursion depth limit reached during {context}")]
    RecursionLimit { context: String },

    #[error("Hex decoding error: {0}")]
    HexDecode(#[from] hex::FromHexError),

    #[error("Unexpected error: {0}")]
    Unexpected(String),
}

pub type AppResult<T> = Result<T, AppError>;
