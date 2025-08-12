# OLIDS UAT Testing Framework

A scalable testing framework for OLIDS (One London Integrated Data Set) User Acceptance Testing (UAT) with a rich CLI.

<img width="2122" height="1184" alt="image" src="https://github.com/user-attachments/assets/5ea35d8f-fe0e-4458-b6b4-f014b3e34079" />

## 📋 Overview

The OLIDS UAT Testing Framework provides validation of healthcare data quality across multiple dimensions:

- **Data Quality**: NULL column detection, table completeness, field validation
- **Referential Integrity**: Foreign key relationship validation across 127 relationships
- **Concept Mapping**: Terminology validation through CONCEPT_MAP → CONCEPT chains
- **Business Rules**: Person/patient validation patterns and clinical data quality

### Key Features (highlights)

- YAML-driven configuration
- Rich CLI with progress and colour
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
   # - Connection name (e.g., "olids-uat" - although you can use any name)
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
   nano config/environments/uat.yml  # or use your preferred editor
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
     account: "abc12345"
     role: "YOUR_USER_ROLE"
     warehouse: "YOUR_WAREHOUSE"
   ```

6. **Verify Setup**
   ```bash
# Test framework can connect to Snowflake
olids-test config show --environment uat
    
# Run a simple status check
olids-test run status
    
# Run a quick test to verify everything works
olids-test run test empty_tables
   ```

## 📖 Usage

### Command Line Interface

#### Basic Commands

```bash
# Show framework information
olids-test info
    
# List available tests
olids-test list-tests
    
# Check environment status  
olids-test run status
    
# Show configuration
olids-test config show
```

#### Running Tests

```bash
# Run individual test
olids-test run test empty_tables

# Run all tests
olids-test run all
    
# Run with detailed pass information
olids-test run test concept_mapping --show-passes
    
# Run test suite
olids-test run suite data_quality

# Run with parallel execution
olids-test run all --parallel
    
# Export results
olids-test run test all --output json --export results.json
olids-test run test all --output csv --export results.csv
```

#### Output Formats

- **Table** (default): Rich formatted tables with color coding
- **JSON**: Structured data for programmatic analysis
- **CSV**: Comma-separated values for spreadsheet import

### Configuration Management (common)

```bash
# Validate configuration files
python -m olids_testing.cli.main config validate

# List environments
python -m olids_testing.cli.main config environments

# Show specific environment
python -m olids_testing.cli.main config show --environment uat
```

## 🧪 Test Categories

- **Data Quality**: Empty tables, all-NULL columns, column completeness
- **Referential Integrity**: Foreign key relationship validation across core domains
- **Concept Mapping**: CONCEPT_MAP → CONCEPT chain validation and display integrity
- **Person Patterns**: Business-rule validations (identity, registrations, dates)

## 📊 Tips

- Use `--show-passes` to include details of successful checks
- Speed up runs with `--parallel`
- Export results via `--output json|csv --export <file>`

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
execution:
  parallel_workers: 4
  timeout_seconds: 300
  retry_attempts: 3
```

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
- **Audit Trail**: SQL query logging for compliance

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
