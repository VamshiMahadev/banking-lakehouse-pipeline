# Architecture Overview

## Pipeline Flow

```text
        Banking Dataset
               |
               ↓
            Bronze
               |
               ↓
            Silver
               |
               ↓
             Gold
               |
               ↓
      Databricks Workflow
               |
               ↓
         Unity Catalog
               |
               ↓
         Analytical Layer
               |
               ↓
         GitHub Actions
               |
               ↓
            Deployment

```

### Processing Layers

- Bronze
    - Raw Ingestion

- Silver
    - Cleansing
    - Standardization
    - Validation

- Gold
    - Analytics
    - Aggregations
    - Reporting
