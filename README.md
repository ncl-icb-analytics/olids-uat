# OLIDS UAT Testing Framework

A scalable testing framework for OLIDS (One London Integrated Data Set) User Acceptance Testing (UAT) with a rich CLI.

<img width="1324" height="1382" alt="image" src="https://github.com/user-attachments/assets/930eab9e-dee5-4dde-9f2d-5ee749c22ea8" />

## 📋 Overview

The OLIDS UAT Testing Framework provides validation of healthcare data quality across multiple dimensions:

- **Data Quality**: NULL column detection, table completeness, field validation
- **Referential Integrity**: Foreign key relationship validation across 85 relationships
- **Concept Mapping**: Terminology validation through CONCEPT_MAP → CONCEPT chains
- **Business Rules**: Person/patient validation patterns and clinical data quality

### Key Features (highlights)

- YAML-driven configuration
- Rich CLI with progress and colour
- Environment switching
- Parallel execution
- Table/JSON/CSV outputs
- Results include totals, failures, success rate, and timing

## 📁 Project Structure

```
config/
  environments/
    dev.yml
    uat.yml
    prod.yml
  concept_mapping_tests.yml
  person_pattern_mappings.yml
  referential_mappings.yml
  test_suites.yml

src/
  olids_testing/
    cli/
      main.py
      commands/
        config_cmd.py
        run_cmd.py
        deploy_cmd.py
      ui/
        display.py
    core/
      config.py
      connection.py
      runner.py
      test_base.py
      sql_logger.py
    tests/
      data_quality/
      referential_integrity/
      concept_mapping/
      person_patterns/

sql_logs/              # SQL logs saved from test runs
pyproject.toml
README.md
```

## 🛠️ Technology Stack

### Core Technologies
- **Python 3.10+**: Python
- **Snowflake Snowpark** (v1.33.0+): Native Python API for Snowflake data processing
- **Snow CLI** (v3.10.0+): Snowflake's command-line tool to run SQL against snowflake
- **Click** (v8.1.8): Command-line interface framework for building the CLI
- **Rich** (v14.0.0): Terminal formatting library for beautiful console output

### Data & Configuration
- **Pydantic** (v2.11.7): Data validation using Python type annotations
- **PyYAML** (v6.0.2): YAML configuration file parsing

### Development Tools (Optional)
- **black** (v25.1.0): Code formatter
- **isort** (v6.0.1): Import statement organizer
- **flake8** (v7.3.0): Linting and style checking
- **mypy** (v1.17.1): Static type checking
- **pre-commit** (v4.3.0): Git hooks for code quality

## 🛠️ Installation & Setup

### Prerequisites

- Python 3.10+
- Snowflake account with appropriate permissions
- Snow CLI installed and configured (for authentication)
- Access to OLIDS databases in Snowflake

### Setup

1. **Clone Repository**
  ```bash
   git clone https://github.com/ncl-icb-analytics/olids-uat
   cd olids-uat
  ```

2. **Create Virtual Environment**
  ```bash
   python -m venv venv && venv\Scripts\activate
  ```

3. **Install Framework**
  ```bash
   pip install -e .
  ```

4. **Install and Configure Snow CLI**
   
   **Install Snow CLI:**
  ```bash
   # Install Snow CLI (requires Python 3.8+)
   pip install snowflake-cli-labs
   
   # Verify installation
   snow --version
  ```
   
**Configure Snow CLI Connection:**
   ```bash
   # Add a new connection (interactive setup)
   snow connection add
   
   # You'll be prompted for:
   # - Connection name (e.g., "olids-uat", "olids-dev", "olids-prod")
   # - Account identifier (e.g., "abc12345")
   # - Username
   # - Authentication method (recommend: SSO/browser-based)
   # - Role (e.g., "OLIDS_USER_ROLE")
   # - Warehouse (e.g., "OLIDS_WAREHOUSE")
   # - Database (can be left blank, will be specified in environment config)
   ```
   
**SSO/Browser Authentication (recommended):**
   ```bash
   # When prompted for authentication method, choose 'SSO'
   # This will open your browser for authentication
   snow connection add --connection-name olids-uat --account-name your-account --username your-username --authenticator externalbrowser
   ```
   
**Test your connection:**
   ```bash
   # Test the connection works
   snow connection test --connection olids-uat
   
   # List available connections
   snow connection list
   
   # Test a simple query
   snow sql --query "SELECT CURRENT_USER(), CURRENT_ROLE(), CURRENT_WAREHOUSE()" --connection olids-uat
   ```

5. **Configure OLIDS Environment**
  ```bash
   # Copy environment template
   cp config/environments/template.yml config/environments/uat.yml
   
   # Edit the configuration file with your environment details
   # Update: databases, connection details, role, warehouse
  ```
   
   **Example environment configuration:**
  ```yaml
   description: "OLIDS UAT Environment"
    databases:
      source: "Data_Store_OLIDS_UAT"
      terminology: "Data_Store_OLIDS_UAT" 
      results: "DATA_LAB_OLIDS_UAT"
      dictionary: "Dictionary"
   connection:
     snow_cli_connection: "olids-uat"  # Snow CLI connection name
     # Note: account, role, warehouse are defined in Snow CLI connection
  ```

## 🚀 Getting Started

### Initial Setup & Verification

After installation, follow these steps to get up and running:

```bash
# 1. Display the interactive quick start guide
olids-test quickstart

# 2. Validate your environment setup
olids-test validate

# 3. View available test suites
olids-test list-tests

# 4. Run a quick test to verify everything works
olids-test run suite empty_tables
```

### Environment Management

The framework supports multiple environments (dev, uat, prod):

```bash
# Switch to different environment (recommended approach)
olids-test switch dev
olids-test switch uat
olids-test switch prod

# Check current environment 
olids-test info

# Alternative: Use environment flags for one-off commands
olids-test run all -e dev
```

### Common Workflows

**Daily Testing:**
```bash
olids-test switch uat           # Set environment
olids-test validate             # Check setup
olids-test run all              # Run all validations
```

**Investigating Issues:**
```bash
olids-test run suite referential_integrity --show-passes
ls -la sql_logs/               # Review SQL queries executed
```

**Environment Comparison:**
```bash
olids-test switch dev && olids-test run all --export dev_results.json
olids-test switch uat && olids-test run all --export uat_results.json
olids-test switch prod && olids-test run all --export prod_results.json
```

## 📖 Command Reference

### Core Commands

```bash
# Environment management
olids-test switch dev               # Switch environment
olids-test info                     # Show current environment
olids-test validate                 # Check setup

# Test execution
olids-test list-tests               # View available test suites
olids-test run all                  # Run all test suites
olids-test run suite referential_integrity   # Run specific suite
olids-test run suite all_null_columns --show-passes  # With detailed output

# Configuration
olids-test config show              # View current config
olids-test config validate         # Validate config files
```

### Test Suites Available

| Suite | Data Tests | Description |
|-------|------------|-------------|
| `referential_integrity` | 85 | Foreign key relationship validation |
| `concept_mapping` | 28 | Terminology mapping validation |  
| `person_patterns` | 13 | Business rule validation |
| `all_null_columns` | 710 | NULL column detection |
| `empty_tables` | 28 | Empty table detection |
| `column_completeness` | 6 | Column completeness validation |

### Export & Analysis

```bash
# Export results for analysis
olids-test run all --output json --export results.json
olids-test run all --output csv --export results.csv

# Review SQL queries executed
ls -la sql_logs/
```

## ⚙️ Configuration

### Environment Configuration

Configure database connections in `config/environments/`:

```yaml
# config/environments/uat.yml
description: "OLIDS UAT Environment"
databases:
  source: "Data_Store_OLIDS_UAT"
  terminology: "Data_Store_OLIDS_UAT"
  results: "DATA_LAB_OLIDS_UAT"
  dictionary: "Dictionary"
schemas:
  masked: "OLIDS_MASKED"
  terminology: "OLIDS_TERMINOLOGY"
  tests: "TESTS"
connection:
  snow_cli_connection: "olids-uat"  # Snow CLI connection name
  # Note: account, role, warehouse are defined in Snow CLI connection
execution:
  parallel_workers: 4
  timeout_seconds: 300
  retry_attempts: 3
```

**Snow CLI Connection Configuration:**
- **Automatic Default Detection**: Framework automatically uses your default Snow CLI connection
- **Environment-Specific Connections**: Optionally specify `snow_cli_connection` per environment
- **Centralized Management**: All connection details managed through Snow CLI (account, role, warehouse, authentication)
- **Minimal Configuration**: Environment configs only need databases and schemas - connection details are automatic
- **Optional Overrides**: Available if needed (account, role, warehouse, host)

**Connection Priority:**
1. Environment-specific `snow_cli_connection` field (if specified)
2. `SNOWFLAKE_CONNECTION` environment variable (if set)
3. Default Snow CLI connection (automatically detected)
4. First available Snow CLI connection (fallback)

### Test Configuration

Define test suites in `config/test_suites.yml`:

```yaml
data_quality:
  description: "Data quality and completeness validation"
  tests:
    - all_null_columns
    - empty_tables
    - column_completeness
```

### Custom Test Patterns

Top-level YAMLs define mappings and rules (see files in `config/`).

## 🔧 Development

### Adding New Tests

1. **Create Test Class**
   ```python
   # src/olids_testing/tests/my_category/my_test.py
   from olids_testing.core.test_base import BaseTest, TestResult, TestStatus, TestContext

   class MyTest(BaseTest):
       def __init__(self):
           super().__init__(
               name="my_test",
               description="My test description",
               category="my_category"
           )
       
       def execute(self, context: TestContext) -> TestResult:
           # Test implementation
           return TestResult(
               test_name=self.name,
               test_description=self.description,
               status=TestStatus.PASSED,
               total_tested=100,
               failed_records=0
           )
   ```

2. **Register Test**
   ```python
   # src/olids_testing/tests/__init__.py
   from .my_category import MyTest

   TEST_REGISTRY = {
       # ... existing tests
       'my_test': MyTest,
   }
   ```

3. **Configure Test Suite**
   ```yaml
   # config/test_suites.yml
   my_suite:
     description: "My test suite"
     tests:
       - my_test
   ```

## 🔐 Security & Authentication

- **SSO Integration**: Uses Snowflake SSO through Snow CLI
- **No Stored Credentials**: Authentication handled externally
- **Environment Isolation**: Separate configurations per environment
- **Audit Trail**: SQL query logging to support further investigation

### Troubleshooting Authentication

**Snow CLI Connection Issues:**
```bash
# Check Snow CLI installation
snow --version

# List existing connections
snow connection list

# Test specific connection
snow connection test --connection your-connection-name

# Re-authenticate if token expired
snow connection add --connection-name your-connection-name --account-name your-account --username your-username --authenticator externalbrowser
```

**Required Snowflake Permissions:**
```sql
-- Your role needs at least these permissions:
GRANT USAGE ON WAREHOUSE your_warehouse TO ROLE your_role;
GRANT USAGE ON DATABASE data_store_olids_uat TO ROLE your_role;
GRANT USAGE ON SCHEMA data_store_olids_uat.olids_masked TO ROLE your_role;
GRANT SELECT ON ALL TABLES IN SCHEMA data_store_olids_uat.olids_masked TO ROLE your_role;
GRANT USAGE ON SCHEMA data_store_olids_uat.olids_terminology TO ROLE your_role;
GRANT SELECT ON ALL TABLES IN SCHEMA data_store_olids_uat.olids_terminology TO ROLE your_role;
```

## 🚦 Status & Health Checks

Monitor framework and environment health:

```bash
# Check overall status
olids-test run status
    
# Validate environment configuration
olids-test config validate
    
# Test database connectivity
olids-test config show --environment uat
```

## 🤝 Contributing

1. **Follow Architecture**: Use established test pattern architecture
2. **Configuration-driven**: Add test definitions in YAML files
3. **Documentation**: Include comprehensive test documentation
4. **Validation**: Test against existing functionality
5. **Update Documentation**: Update README for new features

The `config/environments/template.yml` serves as the reference for creating environment-specific configurations.

## 📄 License

This repository is dual licensed under the Open Government v3 & MIT. All code outputs are subject to Crown Copyright.
