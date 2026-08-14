use cedar_policy::entities_errors::EntitiesError;
use cedar_policy::{CedarSchemaError, ParseErrors, ValidationResult};

#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error("failed to parse Cedar policies: {0}")]
    PolicyParse(Box<ParseErrors>),
    #[error("failed to parse Cedar schema: {0}")]
    SchemaParse(Box<CedarSchemaError>),
    #[error("failed to parse Cedar entities: {0}")]
    EntitiesParse(Box<EntitiesError>),
    #[error("policy validation failed:\n{0}")]
    Validation(ValidationError),
    #[error("failed to build Cedar context: {0}")]
    Context(String),
    #[error("failed to build Cedar entity: {0}")]
    Entity(String),
}

#[derive(Debug)]
pub struct ValidationError {
    pub errors: Vec<String>,
    pub warnings: Vec<String>,
}

impl std::fmt::Display for ValidationError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        for e in &self.errors {
            writeln!(f, "  error: {e}")?;
        }
        for w in &self.warnings {
            writeln!(f, "  warning: {w}")?;
        }
        Ok(())
    }
}

impl From<ParseErrors> for Error {
    fn from(e: ParseErrors) -> Self {
        Self::PolicyParse(Box::new(e))
    }
}

impl From<CedarSchemaError> for Error {
    fn from(e: CedarSchemaError) -> Self {
        Self::SchemaParse(Box::new(e))
    }
}

impl From<EntitiesError> for Error {
    fn from(e: EntitiesError) -> Self {
        Self::EntitiesParse(Box::new(e))
    }
}

impl ValidationError {
    pub(crate) fn from_result(result: &ValidationResult) -> Option<Self> {
        let errors: Vec<String> = result.validation_errors().map(|e| e.to_string()).collect();
        let warnings: Vec<String> = result.validation_warnings().map(|w| w.to_string()).collect();

        if errors.is_empty() {
            None
        } else {
            Some(Self { errors, warnings })
        }
    }
}
