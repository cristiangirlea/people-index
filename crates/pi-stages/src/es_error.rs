//! Which Elasticsearch failures are worth a task retry.

use pi_coord::permanent;
use pi_es::EsError;

/// Converts an Elasticsearch error for the executor: answers about the request itself (bad
/// configuration, documents the mapping refuses, a count that does not match, a concrete
/// index in the alias's place, a 4xx) are permanent, since sending the same thing again gets
/// the same answer; transport errors, busy clusters and timeouts are left to the retry policy.
pub(crate) fn es_error(err: EsError, doing: &str) -> anyhow::Error {
    let is_permanent = match &err {
        EsError::Config(_)
        | EsError::BulkItems { .. }
        | EsError::DocumentTooLarge { .. }
        | EsError::InvalidDocument { .. }
        | EsError::CountMismatch { .. }
        | EsError::ConcreteIndexAtAlias(_)
        | EsError::NewerRunServing { .. }
        | EsError::IndexOwnership { .. }
        | EsError::Expected(_) => true,
        EsError::Http { status, .. } => (400..500).contains(status) && !matches!(status, 408 | 429),
        _ => false,
    };
    let err = anyhow::Error::new(err).context(doing.to_owned());
    if is_permanent { permanent(err) } else { err }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn http(status: u16) -> EsError {
        EsError::Http {
            method: "GET".into(),
            path: "/".into(),
            status,
            body: String::new(),
        }
    }

    #[test]
    fn requests_answers_are_permanent_and_hiccups_are_not() {
        assert!(pi_coord::is_permanent(&es_error(http(400), "x")));
        assert!(pi_coord::is_permanent(&es_error(http(403), "x")));
        assert!(!pi_coord::is_permanent(&es_error(http(429), "x")));
        assert!(!pi_coord::is_permanent(&es_error(http(503), "x")));
        assert!(!pi_coord::is_permanent(&es_error(
            EsError::TimedOut { path: "/".into() },
            "x"
        )));
        assert!(pi_coord::is_permanent(&es_error(
            EsError::CountMismatch {
                index: "i".into(),
                expected: 1,
                actual: 0
            },
            "x"
        )));
    }
}
