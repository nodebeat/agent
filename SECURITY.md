# Security Policy

## Supported versions

Latest release only. Pre-1.0: best effort backports for critical issues.

## Reporting a vulnerability

Email security@nodebeat.stream with:

* affected version / commit,
* reproduction steps,
* impact assessment (especially any path beyond metrics leak — see `docs/THREAT_MODEL.md`).

We acknowledge within 48h and aim to ship a fix within 14 days for critical issues. Please do not open public issues for undisclosed vulnerabilities.

## Scope

This repo (the agent). The SaaS control plane is private; reports against it are still welcome at the same address but are triaged separately.
