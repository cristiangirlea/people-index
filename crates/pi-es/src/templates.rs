// Index patterns observed from GET _index_template on Elasticsearch 9.5.5.
const BUILTIN_TEMPLATES: &[(&str, &str)] = &[
    (".deprecation-indexing-template", ".logs-deprecation.*"),
    (
        ".deprecation-indexing-template-9",
        ".logs-elasticsearch.deprecation-*",
    ),
    (
        ".fleet-fileds-fromhost-data",
        ".fleet-fileds-fromhost-data-*",
    ),
    (
        ".fleet-fileds-fromhost-meta",
        ".fleet-fileds-fromhost-meta-*",
    ),
    (".fleet-fileds-tohost-data", ".fleet-fileds-tohost-data-*"),
    (".fleet-fileds-tohost-meta", ".fleet-fileds-tohost-meta-*"),
    (".kibana-reporting", ".kibana-reporting*"),
    (".ml-anomalies-", ".ml-anomalies-*"),
    (".ml-anomalies-", ".reindexed-v7-ml-anomalies-*"),
    (".ml-anomalies-", ".reindexed-v8-ml-anomalies-*"),
    (".ml-notifications-000002", ".ml-notifications-*"),
    (".ml-state", ".ml-state*"),
    (".ml-state", ".reindexed-v7-ml-state*"),
    (".ml-state", ".reindexed-v8-ml-state*"),
    (".ml-stats", ".ml-stats-*"),
    (".monitoring-beats-mb", ".monitoring-beats-8-*"),
    (".monitoring-ent-search-mb", ".monitoring-ent-search-8-*"),
    (".monitoring-es-mb", ".monitoring-es-8-*"),
    (".monitoring-kibana-mb", ".monitoring-kibana-8-*"),
    (".monitoring-logstash-mb", ".monitoring-logstash-8-*"),
    (".slm-history-7", ".slm-history-7*"),
    (".watch-history-18", ".watcher-history-18*"),
    ("agentless", "agentless-*-*"),
    (
        "behavioral_analytics-events-default",
        "behavioral_analytics-events-*",
    ),
    ("elastic-connectors", ".elastic-connectors-v1"),
    (
        "elastic-connectors-sync-jobs",
        ".elastic-connectors-sync-jobs-v1",
    ),
    ("ilm-history-7", "ilm-history-7*"),
    ("logs", "logs-*-*"),
    ("logs-apm.app@template", "logs-apm.app.*-*"),
    ("logs-apm.error@template", "logs-apm.error-*"),
    (
        "logs-elasticsearch.querylog@template",
        "logs-elasticsearch.querylog-*",
    ),
    ("logs-otel@template", "logs-*.otel-*"),
    ("metrics", "metrics-*-*"),
    ("metrics-apm.app@template", "metrics-apm.app.*-*"),
    ("metrics-apm.internal@template", "metrics-apm.internal-*"),
    (
        "metrics-apm.service_destination.10m@template",
        "metrics-apm.service_destination.10m-*",
    ),
    (
        "metrics-apm.service_destination.1m@template",
        "metrics-apm.service_destination.1m-*",
    ),
    (
        "metrics-apm.service_destination.60m@template",
        "metrics-apm.service_destination.60m-*",
    ),
    (
        "metrics-apm.service_summary.10m@template",
        "metrics-apm.service_summary.10m-*",
    ),
    (
        "metrics-apm.service_summary.1m@template",
        "metrics-apm.service_summary.1m-*",
    ),
    (
        "metrics-apm.service_summary.60m@template",
        "metrics-apm.service_summary.60m-*",
    ),
    (
        "metrics-apm.service_transaction.10m@template",
        "metrics-apm.service_transaction.10m-*",
    ),
    (
        "metrics-apm.service_transaction.1m@template",
        "metrics-apm.service_transaction.1m-*",
    ),
    (
        "metrics-apm.service_transaction.60m@template",
        "metrics-apm.service_transaction.60m-*",
    ),
    (
        "metrics-apm.transaction.10m@template",
        "metrics-apm.transaction.10m-*",
    ),
    (
        "metrics-apm.transaction.1m@template",
        "metrics-apm.transaction.1m-*",
    ),
    (
        "metrics-apm.transaction.60m@template",
        "metrics-apm.transaction.60m-*",
    ),
    ("metrics-otel@template", "metrics-*.otel-*"),
    ("metrics-prometheus@template", "metrics-*.prometheus-*"),
    (
        "metrics-service_destination.10m.otel@template",
        "metrics-service_destination.10m.otel-*",
    ),
    (
        "metrics-service_destination.1m.otel@template",
        "metrics-service_destination.1m.otel-*",
    ),
    (
        "metrics-service_destination.60m.otel@template",
        "metrics-service_destination.60m.otel-*",
    ),
    (
        "metrics-service_summary.10m.otel@template",
        "metrics-service_summary.10m.otel-*",
    ),
    (
        "metrics-service_summary.1m.otel@template",
        "metrics-service_summary.1m.otel-*",
    ),
    (
        "metrics-service_summary.60m.otel@template",
        "metrics-service_summary.60m.otel-*",
    ),
    (
        "metrics-service_transaction.10m.otel@template",
        "metrics-service_transaction.10m.otel-*",
    ),
    (
        "metrics-service_transaction.1m.otel@template",
        "metrics-service_transaction.1m.otel-*",
    ),
    (
        "metrics-service_transaction.60m.otel@template",
        "metrics-service_transaction.60m.otel-*",
    ),
    (
        "metrics-transaction.10m.otel@template",
        "metrics-transaction.10m.otel-*",
    ),
    (
        "metrics-transaction.1m.otel@template",
        "metrics-transaction.1m.otel-*",
    ),
    (
        "metrics-transaction.60m.otel@template",
        "metrics-transaction.60m.otel-*",
    ),
    ("search-acl-filter", ".search-acl-filter-*"),
    ("synthetics", "synthetics-*-*"),
    ("traces-apm.rum@template", "traces-apm.rum-*"),
    ("traces-apm.sampled@template", "traces-apm.sampled-*"),
    ("traces-apm@template", "traces-apm-*"),
    ("traces-otel@template", "traces-*.otel-*"),
];

/// Returns a built-in template whose pattern can match a run index with this prefix.
#[must_use]
pub fn builtin_template_for_prefix(prefix: &str) -> Option<(&'static str, &'static str)> {
    BUILTIN_TEMPLATES
        .iter()
        .copied()
        .find(|(_, pattern)| template_can_match_run(pattern, prefix))
}

// Match the fixed prefix with ES's '*' wildcard, retaining all possible pattern positions.
// The remainder must be satisfiable by a nonempty, at-most-128-byte safe run ID.
fn template_can_match_run(pattern: &str, prefix: &str) -> bool {
    let pattern = pattern.as_bytes();
    let mut states = vec![false; pattern.len() + 1];
    states[0] = true;
    for byte in prefix.bytes() {
        for i in 0..pattern.len() {
            if states[i] && pattern[i] == b'*' {
                states[i + 1] = true;
            }
        }
        let mut next = vec![false; states.len()];
        for i in 0..pattern.len() {
            if states[i] {
                if pattern[i] == b'*' {
                    next[i] = true;
                } else if pattern[i] == byte {
                    next[i + 1] = true;
                }
            }
        }
        states = next;
    }
    states.iter().enumerate().any(|(i, &reachable)| {
        let rest = &pattern[i..];
        let literals = rest.iter().filter(|&&b| b != b'*').count();
        reachable
            && literals <= 128
            && (literals > 0 || rest.contains(&b'*'))
            && rest.iter().all(|&b| {
                b == b'*'
                    || b.is_ascii_lowercase()
                    || b.is_ascii_digit()
                    || matches!(b, b'.' | b'_' | b'=' | b'-')
            })
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn run_pattern_edges() {
        assert!(template_can_match_run("a*b*c", "ax-b"));
        assert!(!template_can_match_run("a*b*c", "zz-"));
        assert!(!template_can_match_run("a-", "a-"));
        assert!(template_can_match_run(
            ".elastic-connectors-v1",
            ".elastic-connectors-"
        ));
        assert!(!template_can_match_run("x-@*", "x-"));
        assert!(!template_can_match_run("x-A*", "x-"));
        assert!(!template_can_match_run(
            &format!("x-{}", "a".repeat(129)),
            "x-"
        ));
        assert!(builtin_template_for_prefix("logs-x-").is_some());
        assert!(builtin_template_for_prefix("synthetics-x-").is_some());
    }
}
