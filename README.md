# AEMO Unity Catalog Metadata Automation

Automated metadata extraction and population for AEMO (Australian Energy Market Operator) electricity market data tables in Databricks Unity Catalog.

## Overview

This Databricks notebook automates the extraction of table and column metadata from AEMO's Electricity Data Model PDF documentation and applies it to Unity Catalog tables at scale. Designed for energy market participants managing AEMO datasets in the Australian National Electricity Market (NEM).

## Features

- **PDF Metadata Extraction**: Parses AEMO Electricity Data Model PDFs to extract table schemas and column descriptions
- **Flexible Column Matching**: Configurable matching policies (exact, flexible, fuzzy) to handle column name variations
- **Mismatch Analysis**: Identifies discrepancies between PDF documentation and table schemas
- **Batch Processing**: Updates multiple tables efficiently via SQL ALTER statements
- **Review Workflow**: Preview and approve changes before applying to production
- **Pre-configured Examples**: Includes standard AEMO table definitions as reference templates

## Prerequisites

- Databricks Runtime 13.3 LTS or higher
- Unity Catalog enabled workspace
- Unity Catalog admin or table owner permissions
- AEMO Electricity Data Model PDF ([download from AEMO](https://aemo.com.au/energy-systems/electricity/national-electricity-market-nem/data-nem))

## Installation

1. Clone the repository

git clone https://github.com/pravinva/aemo-unity-catalog-metadata.git


2. Upload `aemo_unity_catalog_metadata_automation.py` to your Databricks workspace

3. Upload your AEMO Electricity Data Model PDF to Volumes or DBFS

## Configuration

Update the configuration section at the top of the notebook:

CATALOG_NAME = "your_catalog" # Your Unity Catalog name
SCHEMA_NAME = "aemo_data" # Your schema name
PDF_FILE_PATH = f"/Volumes/{CATALOG_NAME}/{SCHEMA_NAME}/pdfs/electricity_data_model.pdf"
REVIEW_OUTPUT_PATH = f"/Volumes/{CATALOG_NAME}/{SCHEMA_NAME}/metadata_review/"


## Usage

1. **Configure**: Update environment variables in Step 2
2. **Setup**: Run Steps 1-3 to install dependencies and create catalog/schema
3. **Create Tables**: Execute Step 4 to create example table structures (optional)
4. **Extract**: Run Step 7 to extract metadata from PDF
5. **Review**: Generate and review proposed changes in Steps 8-9
6. **Apply**: Execute approved changes in Step 10
7. **Verify**: Confirm updates in Step 11

### Example Table Definitions

The notebook includes example AEMO table definitions for reference:

**Bidding Data**
- `bidperoffer_d`, `bidperoffer`, `bidofferperiod`, `biddayoffer`, `biddayoffer_d`

**Pricing Data**
- `dispatchprice`, `predispatchprice`

**Settlement Data**
- `setenergygensetdetail`, `setenergytransactions`

**Billing Data**
- `billingruntrk`, `billingdaytrk`

**Generation Data**
- `rooftoppvactual`

*Note: These table definitions are provided as examples and templates. Modify them according to your specific data model requirements.*

## Documentation

- [Configuration Guide](docs/CONFIGURATION.md)
- [Usage Examples](docs/EXAMPLES.md)
- [Troubleshooting](docs/TROUBLESHOOTING.md)
- [Contributing Guidelines](CONTRIBUTING.md)

## Maintainer

**Pravin Varma**  
GitHub: [@pravinva](https://github.com/pravinva)

## License

MIT License - see [LICENSE](LICENSE) for details

## Acknowledgments

Built for energy market participants working with AEMO data in the Australian National Electricity Market (NEM).

*This project is not affiliated with or endorsed by AEMO.*

