# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/).

## [Unreleased]

### Added

- Optional `living-city` distributed-district mode using authenticated WebSocket remoting, with
  peer status/snapshot aggregation and a nine-district Docker Compose deployment.
- Vehicles in the distributed `living-city` mode migrate between districts running in different
  processes.
- The `picard-console` TUI shows the actor tree collapsed; Right/Left expand and collapse children.
