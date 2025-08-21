# IBM UCD **Complete-ish** Sample Export (Dummy Data)

This package simulates a reasonably complete **UrbanCode Deploy (UCD)** export for a demo application. It is **not** an official IBM export, but the structure and fields are designed to feel familiar for testing and POCs (e.g., mapping to Harness).

**Generated:** 2025-08-19T20:26:48.566532Z

## Application: `SampleBankingApp`
- Components: WebAppComponent, DatabaseComponent, BatchJobComponent, ConfigComponent
- App Processes: Deploy All, Blue-Green, Rollback All
- Generic Processes: Health Check, Restart Service
- Environments: Dev, QA, Prod
- Resource tree with agents and component mappings
- Snapshots: 1.0.0 (pre-release tag)
- Approvals: PreDeploy & PostDeploy gates
- Integrations: Nexus (artifact repo), GitHub (SCM)
- Version examples: 1.0.0 & 1.1.0 for WebApp, 1.0.0 for DB

> Secrets are referenced as secure properties (e.g., `${p:?DatabaseComponent/dbPassword}`). Replace with your secret store when importing elsewhere.