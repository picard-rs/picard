use thiserror::Error;

fn all_chars(s: &str, allowed: impl Fn(char) -> bool) -> bool {
    !s.is_empty() && s.chars().all(allowed)
}

pub fn validate_name(name: &str) -> Result<(), InvalidName> {
    if all_chars(name, |c| c.is_ascii_alphanumeric() || c == '_' || c == '-') {
        Ok(())
    } else {
        Err(InvalidName { name: name.into() })
    }
}

#[derive(Error, Debug)]
#[error("\"{name}\". Invalid name. Must contain only a-Z, 0-9, _, or -")]
pub struct InvalidName {
    pub name: String,
}

pub fn validate_path(path: &str) -> Result<(), InvalidPath> {
    if all_chars(path, |c| {
        c.is_ascii_alphanumeric() || matches!(c, '/' | '*' | '.' | '_' | '-')
    }) {
        Ok(())
    } else {
        Err(InvalidPath { path: path.into() })
    }
}

#[derive(Error, Debug)]
#[error("\"{path}\". Invalid path. Must contain only a-Z, 0-9, /, _, .., - or *")]
pub struct InvalidPath {
    path: String,
}
