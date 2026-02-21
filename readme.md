# AI Accounts Payable Employee

Autonomous multi-agent system that runs Accounts Payable end-to-end:
invoice → extraction → validation → approvals → payment → audit.

Designed for **safe, deterministic, and compliant execution** using
orchestration, guardrails, memory, and audit trails.

📊 Architecture: [[IMAGE_LINK](https://drive.google.com/file/d/1mGVbKXstmyWUKc1SxHAgv2P-bCJusZxE/view?usp=sharing)]

## Architecture (high level)
- Orchestrator (brain / state machine)
- Agents: Extraction, Matching, Communication, Payment
- Guardrails (policy + safety checks)
- StateDB (Postgres source of truth)
- Vector Memory + Redis cache (learning)
- Audit logs (full traceability)

## Features
- End-to-end autonomous execution
- Deterministic workflow (no hidden decisions)
- Policy-based approvals
- Fraud & compliance checks
- Full auditability
