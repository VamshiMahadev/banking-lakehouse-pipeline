banking-lakehouse-pipeline/
├── .github/
│   └── workflows/
│       └── databricks_cicd.yml       # Multi-stage GitHub Actions CI/CD Pipeline
├── ddl/
│   └── 00_setup_environment.sql      # Unity Catalog schemas & external tables setup
├── notebooks/
│   ├── 01_bronze_ingestion.py        # Ingestion layer (Auto Loader)
│   ├── 02_silver_transformations.py # Data cleaning, relationships, Delta MERGE
│   └── 03_gold_reporting.py          # Business logic & reporting views
├── databricks.yml                    # Orchestration via Databricks Asset Bundles (DAB)
└── README.md                         # Architecture documentation