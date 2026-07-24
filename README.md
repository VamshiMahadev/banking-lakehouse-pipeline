# Banking Lakehouse ETL Pipeline

An end-to-end Data Engineering project that implements a Banking Lakehouse using Azure Databricks, Delta Lake, Unity Catalog, and GitHub Actions following the Medallion Architecture (Bronze → Silver → Gold).

## Project Overview

This project demonstrates how modern data platforms process banking transactions using scalable and governed Lakehouse principles.

The pipeline performs:

- Raw data ingestion
- Data cleansing and standardization
- Incremental processing
- Delta Lake transformations
- Business aggregations
- Workflow orchestration
- Automated CI/CD deployment

---

## Architecture

```
                Source Dataset
                       |
                  Raw Files
                       |
                  Bronze Layer
                (Raw Delta Tables)
                       |
                       ↓
                 Silver Layer
            (Cleaned & Enriched Data)
                       |
                       ↓
                   Gold Layer
             (Business Aggregations)
                       |
                       ↓
                Databricks Workflow
                       |
                       ↓
                 Unity Catalog
                       |
                       ↓
                 Analytics Layer
                       |
                       ↓
                   CI/CD
                GitHub Actions
```

---

## Technology Stack

| Component | Technology |
|----------|------------|
| Cloud | Microsoft Azure |
| Processing | Apache Spark |
| Platform | Azure Databricks |
| Storage | Delta Lake |
| Governance | Unity Catalog |
| Language | Python & SQL |
| Orchestration | Databricks Workflows |
| CI/CD | GitHub Actions |
| Infrastructure | Databricks Asset Bundles |
| Version Control | Git |

---

## Medallion Architecture

### Bronze Layer

Responsible for:

- Raw data ingestion
- Schema preservation
- Metadata generation
- Audit columns

### Silver Layer

Responsible for:

- Data cleansing
- Standardization
- Deduplication
- Business validations

### Gold Layer

Responsible for:

- Business aggregations
- Analytical tables
- KPI generation
- Reporting datasets


---

## CI/CD Pipeline

GitHub Actions performs:

```
Validate Bundle
       |
       ↓
Deploy Bundle
       |
       ↓
Execute Workflow
       |
       ↓
Validate Pipeline
       |
       ↓
Successful Deployment
```


---

## Project Features

- End-to-End ETL Pipeline
- Delta Lake Processing
- Unity Catalog Integration
- Medallion Architecture
- Incremental Processing
- Automated Deployments
- Databricks Workflows
- GitHub Actions Integration
- Production Ready CI/CD Design
- Portfolio Friendly Documentation

---

## Running the Project

### Validate

```bash
databricks bundle validate -t dev
```

### Deploy

```bash
databricks bundle deploy -t prod
```

### Execute Workflow

```bash
databricks bundle run banking_etl_pipeline -t prod
```

---

## Repository Structure

```text
.
├── bronze
├── silver
├── gold
├── notebooks
├── resources
├── .github/workflows
├── databricks.yml
└── README.md
```


---

## Future Enhancements

- Streaming Ingestion
- CDC Processing
- SCD Type-2 Implementation
- Data Quality Framework
- Monitoring Dashboards
- Automated Testing
- Infrastructure as Code


