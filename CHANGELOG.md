# Changelog

## [Unreleased]

- Refuse aliases whose run indexes can match Elasticsearch 9.5.5 built-in templates.
- Count ignored values for the built-in date format, including multi-fields, with documented gaps.
- Add a signed container release workflow with image scanning, SBOM and provenance.
- Refuse unsafe effective index settings and mapping drift before loading.
