pub type Result<T> = std::result::Result<T, Error>;

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Error {
    NotFound,
    InvalidArgument,
    Other(String),
}

impl Error {
    pub fn errno(self) -> libc::c_int {
        let var_name = match self {
            Error::NotFound => libc::ENOENT,
            Error::InvalidArgument => libc::EINVAL,
            Error::Other(_) => libc::ENOTSUP, // Need better code
        };
        var_name
    }
}

impl From<rusqlite::Error> for Error {
    fn from(err: rusqlite::Error) -> Self {
        match err {
            rusqlite::Error::QueryReturnedNoRows => Error::NotFound,
            _ => Error::Other(err.to_string()),
        }
    }
}

impl std::fmt::Display for Error {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Error::NotFound => write!(f, "Not Found"),
            Error::InvalidArgument => write!(f, "Invalid Argument"),
            Error::Other(msg) => write!(f, "Other: {}", msg),
        }
    }
}

impl std::error::Error for Error {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        None
    }

    fn description(&self) -> &str {
        "description() is deprecated; use Display"
    }

    fn cause(&self) -> Option<&dyn std::error::Error> {
        self.source()
    }
}
