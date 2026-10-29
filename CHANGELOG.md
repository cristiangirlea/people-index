# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/).

## [Unreleased]

### Added

- Staged Rust pipeline joining gzipped person and organization feeds and publishing documents to Elasticsearch behind an alias.
- Local and distributed execution with object-store commits and leases, plus synthetic data generation and verification.
- Docker Compose and Kubernetes deployment configurations, tests, and operational documentation.
- Count ignored values for the built-in date format, including multi-fields, with documented gaps.
- Add a signed container release workflow with image scanning, SBOM and provenance.
- Refuse unsafe effective index settings and mapping drift before loading.
