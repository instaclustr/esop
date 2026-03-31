# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [unreleased]

## [4.1.5]

### Changed

- Bumped software.amazon.awssdk to 2.40.17 (#111)

## [4.1.4]

### Added

- S3 CircleCI job to run tests for esop-s3 (#109)
- Changelog to track changes between releases (#110)

### Security

- INS-40014 - io.netty:netty-handler for esop-s3 was bumped to 4.1.129.Final in order to address vulnerability CVE-2025-67735 (#107)
- INS-40333 [cassandra-image] Bump jackson.core version from 2.19.2 to 2.21.1 for esop to address vulnerability GHSA-72hv-8253-57qq (#108)

