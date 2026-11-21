# Day 1: Architecture, Foundations & System Execution Guide
**Project:** `nyc-taxi-mlops-lab` | **Environment:** AWS Academy Learner Lab (`us-east-1`)  
**Student:** `anand24mishra` | **Date:** 2026-11-21

---

## 1. Executive Summary & Core Problem

### The Business Challenge
In a ride-hailing app (like Uber or Lyft), customers requesting rides require accurate estimated trip duration quotes before booking. Quoting poorly degrades user trust:
- **Under-quoting**: Leads to unhappy riders who miss appointments and complain to support.
- **Over-quoting**: Deters price/time-sensitive riders who choose competing transit options.

### The Baseline vs. Target State
- **Baseline (Stub)**: Quotes a static median ($12.25$ minutes) regardless of pickup location, destination, time of day, or day of week.
- **Target System (Production MLOps)**: An end-to-end, automated machine learning platform that ingests raw monthly parquet trip datasets, transforms features in a cloud data warehouse (AWS Redshift), trains gradient-boosted decision trees (LightGBM) tracked in MLflow, validates models against strict automated quality gates, serves real-time predictions via zero-downtime blue-green deployments, continuously monitors for data drift, and provides an LLM support assistant backed by Model Context Protocol (MCP) to explain prediction quotes.

---

## 2. High-Level Architectural View

```mermaid
flowchart TD
    subgraph ClientLayer ["1. Client & Traffic Ingress"]
        Rider["Rider / API Client"] -->|HTTP Port 80| Traefik["Traefik Reverse Proxy<br/>(Blue/Green Router)"]
    end

    subgraph AppHost ["2. App Host (EC2 Graviton / lab-app)"]
        Traefik -->|Internal 8000| AppAPI["App Backend (FastAPI)<br/>/predict & /version"]
        AppAPI -->|Internal 8001| Serving["Model Serving Engine<br/>(MLflow PyFunc)"]
        AppAPI -->|Internal 8002| MCPServer["MCP Server<br/>(Support Assistant Tools)"]
    end

    subgraph PlatformHost ["3. Platform Host (EC2 Graviton / lab-platform)"]
        Dagster["Dagster Orchestrator<br/>(Port 3000)"]
        MLflow["MLflow Tracking & Registry<br/>(Port 5000)"]
        Postgres["RDS PostgreSQL<br/>(Dagster & MLflow DB)"]
    end

    subgraph DataWarehouse ["4. Storage & Warehouse Layer"]
        S3Data["Instructor S3 Bucket<br/>(Trips Parquet & Zones)"]
        S3Artifacts["Student Artifact S3 Bucket<br/>(MLflow Artifacts & Models)"]
        Redshift["AWS Redshift Data Warehouse<br/>(Schemas: ingest, staging, marts)"]
    end

    subgraph LLMHost ["5. LLM Host (EC2 Graviton / lab-llm)"]
        LLMChat["LLM Chat Engine<br/>(:8080)"]
        LLMEmbed["Embedding Engine<br/>(:8081)"]
        VectorDB["Wiki Knowledge Base<br/>(RAG Support)"]
    end

    %% Interactions
    S3Data -->|Parquet Sensor| Dagster
    Dagster -->|Data API SQL| Redshift
    Dagster -->|Train & Register| MLflow
    MLflow -->|Saves Models| S3Artifacts
    MLflow -.->|Polls Champion/Challenger| Serving
    MCPServer -->|Read-Only SQL (lab_ro)| Redshift
    MCPServer -->|Query Predictions| Serving
    LLMChat -->|Queries Tools| MCPServer
    LLMChat -->|Retrieval| VectorDB
    Dagster --- Postgres
    MLflow --- Postgres
```

---

## 3. Deep Theoretical Foundations: How, Why & What Happens

### A. Spatio-Temporal Modeling with LightGBM
1. **Why LightGBM for Taxi Duration?**
   - Trip duration is fundamentally non-linear with sharp combinatorial thresholds (e.g., crossing Manhattan bridges during rush hour vs. 3 AM).
   - Deep neural networks on tabular spatio-temporal data typically require massive tuning, suffer from training-serving latency overhead, and struggle with categorical identifiers (`PULocationID`, `DOLocationID`).
   - LightGBM uses **Histogram-based Gradient Boosting Decision Trees (GBDT)** with **Leaf-wise (best-first) tree growth**. This dramatically cuts memory usage and training time while capturing complex feature interactions (zone pair $\times$ pickup hour $\times$ weekday).

2. **How Features are Engineered (`contracts/features.yaml`)**:
   - `pickup_zone_id`, `dropoff_zone_id`: Categorical location indices.
   - `pickup_hour`, `pickup_day_of_week`: Cyclic temporal attributes.
   - `haversine_distance`: Approximate spatial geodesic distance computed from zone centroid coordinates.
   - *Strict Guardrail*: Inputs known only *after* the ride concludes (actual meter fare, real dropoff time, congestion surcharge) are strictly prohibited from the feature contract to eliminate data leakage.

### B. Declarative Orchestration: Software-Defined Assets (Dagster)
1. **Why Dagster over Airflow / Legacy Task DAGs?**
   - Traditional workflow engines execute black-box tasks (`TaskA -> TaskB`). If a task succeeds, the engine doesn't verify whether the data produced conforms to contract.
   - Dagster operates on **Software-Defined Assets (SDAs)**. An asset is an explicit mapping between code, its upstream dependencies, and the durable data object it materializes.
2. **Partitioning & Sensors**:
   - Monthly yellow taxi parquet files (`yellow_tripdata_YYYY-MM.parquet`) are discovered dynamically by a file sensor on S3.
   - The sensor yields dynamic partition keys (`YYYY-MM`), instantiating isolated backfill/incremental pipelines for each month.
   - Blocking asset checks validate Parquet footers and schema types against `contracts/trips.schema.yaml` *before* issuing Redshift `COPY` commands.

### C. Serverless Data Access: AWS Redshift Data API
1. **Why Redshift Data API instead of a persistent JDBC/connection pool?**
   - Traditional database connections require long-lived TCP sockets, credential rotation in code/containers, and pool management that fails under container restarts or lambda spikes.
   - The **Redshift Data API** is an HTTPS REST interface. Dagster and the MCP server submit SQL statements asynchronously using temporary IAM credentials (`--db-user labadmin` or `--db-user lab_ro`).
   - Transactions are batched into single execution calls, guaranteeing atomic table swaps without long-lived database locks.

### D. Zero-Downtime Blue-Green Deployment with Traefik
1. **How It Works**:
   - Two container stacks (`app-blue` and `app-green`) run side-by-side on the app host.
   - Traefik acts as a dynamic reverse proxy reading Docker labels or volume states.
   - When a new image is deployed:
     1. The inactive color is updated with the new image and started.
     2. Health checks (`/healthz`) poll until the container reports ready.
     3. Traefik atomically cuts traffic over to the newly verified color.
     4. If health checks fail, traffic remains on the active color with zero dropped requests.

### E. Model Promotion & Approval Gate
1. **Challenger vs. Champion Lifecycle**:
   - When a new model trains in Dagster, automated asset checks benchmark its metrics (MAE, RMSE, latency) against baseline thresholds.
   - If checks pass, the pipeline tags the model with the **`challenger`** alias in MLflow.
   - **Never auto-promote to production**: Promoting a model to **`champion`** is an explicit human decision. GitHub Actions uses a protected `production` environment with manual approval gates. Only when a human signs off does the promote workflow point the `champion` alias at the candidate model.

---

## 4. Platform Constraints (AWS Academy Learner Lab)

| Restriction | Why It Exists in Learner Lab | Architectural Solution |
| :--- | :--- | :--- |
| **No IAM Creation** | Learner Lab policies prevent `iam:CreateRole`, `iam:CreatePolicy`, etc. | Use pre-created `arn:aws:iam::<account>:role/LabRole` and `LabInstanceProfile`. |
| **No `aws_s3_bucket` in Terraform** | Learner Lab SCP denies `s3:GetBucketObjectLockConfiguration`, breaking Terraform plans. | Use custom module `infra/modules/bucket` (invokes AWS CLI underneath). |
| **Graviton Hosts (`linux/arm64`)** | Cost-performance optimization on EC2 instances. | All Docker images must be cross-built for `linux/arm64`. |
| **No ECR Write** | `LabRole` has read-only access to ECR. | Container registry is GitHub Container Registry (`ghcr.io/{owner}/{repo}/{image}`). |
| **No SSH / Port 22 Closed** | Security boundary enforcement on student VPCs. | Shell access and UI access strictly via **AWS Systems Manager (SSM)** Session Manager and port-forwarding. |

---

## 5. Stage S0: The Walking Skeleton (Detailed Notes)

### What is a "Walking Skeleton"?
In software engineering, a **walking skeleton** is an implementation of the thinnest possible slice of real business functionality that links the entire architectural system together from end to end:
```
Git Commit (Local) ──> GitHub Actions CI (ubuntu) ──> Build arm64 Image ──> Push to GHCR
                                                                                │
AWS EC2 (lab-app) <── Self-Hosted Runner (Docker Compose) <─────────────────────┘
       │
    Traefik (Port 80) ──> FastAPI App (:8000) ──> Returns /version (Git SHA) & /predict (Stub)
```

### Why Build This First?
1. **De-risking Deployment**: Building complex ML models first often results in deployment deadlocks (missing system libraries, network timeouts, auth errors). Proving the deployment pipeline with a simple stub guarantees that when intelligent models arrive, shipping them to users takes zero extra overhead.
2. **Deterministic Feedback Loop**: Developers immediately verify changes live in under 2 minutes after pushing to `main`.
3. **Validating Cloud Infrastructure**: Verifies that Terraform state locking, EC2 Graviton instances, SSM communication, and security group routing work as designed.

### Key Technologies Used in Stage S0:
1. **Terraform 1.17**:
   - Manages infrastructure as code.
   - State stored in remote S3 bucket `lab-tfstate-062781275914` with native S3 lockfile (no DynamoDB table needed).
   - `infra/live/main.tf` orchestrates:
     - `module.app`: EC2 Graviton instance running Traefik and App backend.
     - `module.platform`: EC2 Graviton instance for Dagster and MLflow.
     - `module.llm`: EC2 Graviton instance for future LLM chat and embeddings.
     - `module.rds`: AWS RDS PostgreSQL instance for Dagster/MLflow metadata.
     - `module.artifact_bucket`: S3 bucket for model artifacts.
     - `module.wiki_secret`: Secrets Manager container for wiki docs.
2. **Traefik v3.7 (Reverse Proxy)**:
   - Listens on public HTTP port 80.
   - Forwards traffic internally to `app:8000`.
   - Traefik uses file providers (`traefik.yaml`), avoiding mounting the host Docker socket directly for strict security hardening.
3. **AWS Systems Manager (SSM) Run Command**:
   - Executes commands on EC2 instances over HTTPS via the AWS SSM agent.
   - Enables fully automated host registration as a GitHub Actions runner without opening port 22 or managing SSH private keys.
4. **GitHub Container Registry (GHCR)**:
   - Hosts our `linux/arm64` container images at `ghcr.io/anand24mishra/nyc-taxi-mlops-lab/app:<tag>`.
5. **Docker Compose v5**:
   - Manages service lifecycle on the host with `restart: unless-stopped`. When Learner Lab restarts instances, the stack comes back up automatically.

---

## 6. S0 Decision Records

### Decision 001: Runner Credentials
- **Choice**: The self-hosted runner on the EC2 host uses the attached **`LabInstanceProfile`** via IMDSv2.
- **Rejected Alternative**: Storing static session tokens in GitHub Secrets.
- **Why**: Learner Lab tokens expire every 4 hours. Static tokens break deployments every 4 hours; instance profiles never expire and require zero manual maintenance.

### Decision 002: Image Tagging Strategy
- **Choice**: Short Git commit SHA (`sha-${GITHUB_SHA::7}`).
- **Rejected Alternative**: Mutable `:latest` or manual SemVer (`v1.0.0`).
- **Why**: Git SHAs are immutable, deterministic, and make rollback instant (point Compose to previous commit SHA).

### Decision 003: Ingress Scope
- **Choice**: Restrict port 80 HTTP ingress to student laptop IP (`115.244.141.202/32`) and grader IP.
- **Rejected Alternative**: Open to `0.0.0.0/0`.
- **Why**: Eliminates malicious Internet bots and vulnerability scrapers from overwhelming the small lab EC2 instances.

---

## 7. Execution Log & Status

- **2026-11-21 09:05**: Public repo initialized and upstream tracked (`balwa/nyc-taxi-mlops-lab`).
- **2026-11-21 09:15**: Initial `make check` passed green.
- **2026-11-21 11:39**: AWS credentials configured for Account `062781275914` in `us-east-1`.
- **2026-11-21 11:40**: State bucket `lab-tfstate-062781275914` bootstrapped via `infra/bootstrap/state-bucket.sh`.
- **2026-11-21 11:41**: Terraform root module wired (`main.tf`, `security.tf`, `outputs.tf`, `terraform.tfvars`).
- **2026-11-21 11:42**: `terraform plan` executed: **Plan: 28 to add, 0 to change, 0 to destroy**.
- **2026-11-21 11:43**: Implemented `scripts/register-runner.sh` (SSM-based self-hosted runner setup).
- **2026-11-21 11:43**: Implemented `.github/workflows/ci.yaml` and `.github/workflows/pr.yaml` (Actionlint and repo workflow rules compliant).
- **2026-11-21 11:45**: Drafted decision records `001-runner-credentials.md`, `002-image-tag-scheme.md`, `003-ingress-scope.md`.
- **2026-11-21 11:51**: User approved `terraform apply` -> 28 resources successfully provisioned on AWS in `us-east-1` (3 EC2 Graviton hosts, RDS Postgres instance, artifact S3 bucket, secret containers, security groups).
- **2026-11-21 11:58**: Registered self-hosted runner `lab-app` on EC2 (`i-071591009d55bdf07`) via SSM with latest runner `v2.338.2`. Runner reported `online`.
- **2026-11-21 12:02**: GitHub Actions `ci.yaml` ran end-to-end:
  - `check`: green (1m 57s)
  - `image`: built `linux/arm64` container image and pushed to GHCR (31s)
  - `deploy`: deployed image on `lab-app` self-hosted EC2 runner with Traefik + FastAPI (27s)
- **2026-11-21 12:06**: Live HTTP verification on public IP `34.196.103.251`:
  - `GET /version` -> `200 OK` (returns git SHA `5527a2f...` and image tag `sha-5527a2f`)
  - `POST /predict` (valid) -> `200 OK` (returns static median quote `12.25` min)
  - `POST /predict` (invalid zone `9999`) -> `422 Unprocessable Entity`
- **2026-11-21 12:07**: Local `make check` verified 100% green. Stage S0 is **COMPLETE**!
