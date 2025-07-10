# 🛠️ DB Migration Tool

**Enterprise Database Migration Tool**  
Automate your transition from **SQL Server to PostgreSQL** with confidence.

## 📦 Overview

The **DB Migration Tool** is a production-grade utility built to streamline and simplify enterprise-grade migrations from Microsoft SQL Server to PostgreSQL. Designed for reliability and extensibility, the tool covers every step of the migration journey — from schema conversion to performance tuning.

---

## ✨ Features

- 🔁 **Schema Conversion**
  - Converts tables, views, indexes, primary & foreign keys
  - Handles T-SQL to PL/pgSQL translation (where applicable)

- 🔤 **Data Type Mapping**
  - Intelligently maps SQL Server types (e.g., `NVARCHAR`, `DATETIME`) to PostgreSQL equivalents (`TEXT`, `TIMESTAMP`)
  - Customizable mapping rules via config

- 🧩 **Constraint & Index Migration**
  - Preserves PKs, FKs, unique constraints, and default values
  - Translates clustered and non-clustered indexes

- 🚀 **Performance Optimization Recommendations**
  - Suggests partitioning, indexing strategies, and parameter adjustments
  - Highlights costly queries and proposes rewrites

- 📊 **Logging & Audit Trail**
  - Detailed logs of schema mappings, warnings, errors, and skipped objects

- 🔧 **Configurable Migration Profiles**
  - Include/exclude objects, control batch size, retry logic, etc.

---

## 📁 Directory Structure

```

db-migration-tool/
├── config/                # Data type mappings and conversion rules
├── logs/                  # Migration logs
├── src/                   # Source code
│   ├── converters/        # Schema & data converters
│   ├── extractors/        # SQL Server metadata extraction
│   ├── loaders/           # PostgreSQL loaders
│   ├── analyzers/         # Query & performance analyzers
│   └── utils/             # Helpers and common utilities
├── tests/                 # Unit and integration tests
├── README.md
└── requirements.txt

````

---

## 🧪 Requirements

- Python 3.9+
- `pyodbc`, `psycopg2`, `sqlparse`, `sqlalchemy`, `pydantic`
- SQL Server ODBC driver installed
- PostgreSQL 12+

---

## 🚀 Getting Started

1. **Install dependencies**
   ```bash
   pip install -r requirements.txt
    ```

2. **Set up your config**

   ```bash
   cp config/example.yaml config/migration.yaml
   ```

3. **Run the migration**

   ```bash
   python main.py --config config/migration.yaml
   ```

---

## 🛡️ Safety Features

* Dry-run mode to preview changes
* Transactional migrations with rollback support
* Custom exclusion lists

---

## 🧠 Pro Tips

* Always back up your source and target DBs before running full migrations.
* Use the `--analyze` flag to generate a pre-migration report with recommendations.
* Integrate into CI/CD pipelines for automated rollout.

---

## 🧑‍💻 Author

**Jamaurice Holt**
*Senior Database Architect | Cloud Migration Specialist*
📫 [Connect on LinkedIn](https://www.linkedin.com/in/jamauriceholt)

---

## 📄 License

MIT License – feel free to fork and adapt for internal use.

---

## 📌 Tags

\#SQLServerMigration #PostgreSQL #DatabaseTools #CloudMigration #DataEngineering #ETL #Automation #SchemaConversion #DBDevOps

