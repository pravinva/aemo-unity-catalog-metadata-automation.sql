# Configuration Guide

## Environment Variables

Configure these variables in Step 2 of the notebook:

### Required Settings


CATALOG_NAME = "your_catalog"
SCHEMA_NAME = "your_schema"
PDF_FILE_PATH = "/path/to/electricity_data_model.pdf"
REVIEW_OUTPUT_PATH = "/path/to/review_output/"


### Path Options

**Unity Catalog Volumes (Recommended)**

PDF_FILE_PATH = f"/Volumes/{CATALOG_NAME}/{SCHEMA_NAME}/pdfs/electricity_data_model.pdf"


## Mismatch Policies

Configure column matching behavior:

unity_manager.set_mismatch_policy('pdf_extra_columns', 'warn') # warn | ignore | error
unity_manager.set_mismatch_policy('unity_extra_columns', 'warn')
unity_manager.set_mismatch_policy('column_name_matching', 'flexible') # exact | flexible | fuzzy
unity_manager.set_mismatch_policy('require_minimum_match', 0.5) # 0.0 to 1.0


## Table Definitions

Modify `aemo_table_definitions` dictionary to customize table structures:

"your_table_name": {
"description": "Table description",
"columns": [
("COLUMN_NAME", "DATA_TYPE", is_required, "Column description"),
]
}
