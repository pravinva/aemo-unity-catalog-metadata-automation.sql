# Databricks notebook source

# MAGIC %md
# MAGIC # Unity Catalog Metadata Automation - AEMO Tables
# MAGIC
# MAGIC This notebook automates the process of extracting table metadata from the **AEMO Electricity Data Model PDF** and applying them to Unity Catalog tables.
# MAGIC
# MAGIC ## Configuration Required
# MAGIC - **Catalog**: Your Unity Catalog name
# MAGIC - **Schema**: Your schema name for AEMO tables  
# MAGIC - **PDF**: Path to AEMO Electricity Data Model PDF
# MAGIC
# MAGIC ## Setup Requirements
# MAGIC - **Compute**: Use Serverless or shared cluster
# MAGIC - **Runtime**: DBR 13.3 LTS or higher
# MAGIC - **Permissions**: Unity Catalog admin or table owner permissions

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 1: Install Required Libraries

# COMMAND ----------

%pip install pdfplumber PyPDF2 pandas

# COMMAND ----------

dbutils.library.restartPython()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 2: Configuration - **UPDATE THESE VALUES FOR YOUR ENVIRONMENT**

# COMMAND ----------

import pdfplumber
import pandas as pd
import json
import re
from typing import Dict, List, Tuple, Optional
import os
from datetime import datetime
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# COMMAND ----------

# ============================================
# CONFIGURATION - UPDATE FOR YOUR ENVIRONMENT
# ============================================

CATALOG_NAME = "your_catalog"              # UPDATE: Your Unity Catalog name
SCHEMA_NAME = "aemo_data"                   # UPDATE: Your schema name for AEMO tables

# PDF file path - Choose one of these formats:
# Option 1: Unity Catalog Volumes (recommended)
PDF_FILE_PATH = f"/Volumes/{CATALOG_NAME}/{SCHEMA_NAME}/pdfs/electricity_data_model.pdf"

# Option 2: DBFS path (alternative)
# PDF_FILE_PATH = "/dbfs/FileStore/aemo/electricity_data_model.pdf"

# Review output path
REVIEW_OUTPUT_PATH = f"/Volumes/{CATALOG_NAME}/{SCHEMA_NAME}/metadata_review/"

# Alternative DBFS path
# REVIEW_OUTPUT_PATH = "/dbfs/FileStore/aemo/metadata_review/"

print("=" * 60)
print("Configuration for AEMO Metadata Automation")
print("=" * 60)
print(f"Catalog: {CATALOG_NAME}")
print(f"Schema: {SCHEMA_NAME}")
print(f"Target Tables: {CATALOG_NAME}.{SCHEMA_NAME}.*")
print(f"PDF Path: {PDF_FILE_PATH}")
print(f"Review Output: {REVIEW_OUTPUT_PATH}")
print("=" * 60)
print("This will update metadata for AEMO tables like:")
print(f"  - {CATALOG_NAME}.{SCHEMA_NAME}.bidperoffer_d")
print(f"  - {CATALOG_NAME}.{SCHEMA_NAME}.dispatchprice")
print(f"  - {CATALOG_NAME}.{SCHEMA_NAME}.setenergygensetdetail")
print("  - etc.")
print("=" * 60)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 3: Create Catalog and Schema (if needed)

# COMMAND ----------

print("=" * 60)
print("ENSURING CATALOG AND SCHEMA EXIST")
print("=" * 60)

try:
    # Create catalog
    print(f"Creating catalog {CATALOG_NAME}...")
    spark.sql(f"CREATE CATALOG IF NOT EXISTS {CATALOG_NAME}")
    print(f"✓ Catalog {CATALOG_NAME} exists or created")

    # Create schema
    print(f"Creating schema {CATALOG_NAME}.{SCHEMA_NAME}...")
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG_NAME}.{SCHEMA_NAME}")
    print(f"✓ Schema {CATALOG_NAME}.{SCHEMA_NAME} exists or created")

    # Verify
    schemas_df = spark.sql(f"SHOW SCHEMAS IN {CATALOG_NAME}")
    schemas = [row['databaseName'] for row in schemas_df.collect()]

    if SCHEMA_NAME in schemas:
        print(f"✓ Verified: Schema {SCHEMA_NAME} is available")

    print("=" * 60)
    print("Configuration Complete!")
    print("=" * 60)

except Exception as e:
    print(f"Error creating catalog/schema: {e}")
    print("\nPossible solutions:")
    print("1. Check you have CREATE CATALOG permissions")
    print("2. Check you have CREATE SCHEMA permissions")
    print("3. Try running each SQL command manually:")
    print(f"   CREATE CATALOG IF NOT EXISTS {CATALOG_NAME}")
    print(f"   CREATE SCHEMA IF NOT EXISTS {CATALOG_NAME}.{SCHEMA_NAME}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 4: Create AEMO Tables (Standard AEMO Structure)
# MAGIC
# MAGIC These are standard AEMO table definitions based on the Electricity Data Model.
# MAGIC Modify column types as needed for your specific requirements.

# COMMAND ----------

print("=" * 60)
print("CREATING AEMO TABLES")
print("=" * 60)

# AEMO Table Definitions based on Electricity Data Model
aemo_table_definitions = {
    "bidperoffer_d": {
        "description": "Bid per offer daily data",
        "columns": [
            ("SETTLEMENTDATE", "DATE", True, "Settlement date for the bid"),
            ("DUID", "STRING", True, "Dispatchable Unit Identifier"),
            ("BIDTYPE", "STRING", True, "Type of bid (ENERGY, RAISE6SEC, etc)"),
            ("OFFERDATE", "TIMESTAMP", False, "Date and time the offer was made"),
            ("VERSIONNO", "INT", False, "Version number of the bid"),
            ("PRICEBAND1", "DECIMAL(15,5)", False, "Price for band 1 in $/MWh"),
            ("PRICEBAND2", "DECIMAL(15,5)", False, "Price for band 2 in $/MWh"),
            ("PRICEBAND3", "DECIMAL(15,5)", False, "Price for band 3 in $/MWh"),
            ("PRICEBAND4", "DECIMAL(15,5)", False, "Price for band 4 in $/MWh"),
            ("PRICEBAND5", "DECIMAL(15,5)", False, "Price for band 5 in $/MWh"),
            ("PRICEBAND6", "DECIMAL(15,5)", False, "Price for band 6 in $/MWh"),
            ("PRICEBAND7", "DECIMAL(15,5)", False, "Price for band 7 in $/MWh"),
            ("PRICEBAND8", "DECIMAL(15,5)", False, "Price for band 8 in $/MWh"),
            ("PRICEBAND9", "DECIMAL(15,5)", False, "Price for band 9 in $/MWh"),
            ("PRICEBAND10", "DECIMAL(15,5)", False, "Price for band 10 in $/MWh"),
            ("LASTCHANGED", "TIMESTAMP", False, "Last date and time record changed")
        ]
    },
    "bidperoffer": {
        "description": "Bid per offer data for trading intervals",
        "columns": [
            ("DUID", "STRING", True, "Dispatchable Unit Identifier"),
            ("BIDTYPE", "STRING", True, "Type of bid"),
            ("SETTLEMENTDATE", "DATE", True, "Settlement date"),
            ("OFFERDATE", "TIMESTAMP", True, "Offer submission date and time"),
            ("PERIODID", "INT", False, "Trading interval period ID"),
            ("VERSIONNO", "INT", False, "Version number of the bid"),
            ("MAXAVAIL", "DECIMAL(15,5)", False, "Maximum availability MW"),
            ("ROCUP", "DECIMAL(15,5)", False, "Ramp up rate MW/min"),
            ("ROCDOWN", "DECIMAL(15,5)", False, "Ramp down rate MW/min"),
            ("ENABLEMENTMIN", "DECIMAL(15,5)", False, "Minimum enablement level MW"),
            ("ENABLEMENTMAX", "DECIMAL(15,5)", False, "Maximum enablement level MW"),
            ("LOWBREAKPOINT", "DECIMAL(15,5)", False, "Low breakpoint MW"),
            ("HIGHBREAKPOINT", "DECIMAL(15,5)", False, "High breakpoint MW"),
            ("LASTCHANGED", "TIMESTAMP", False, "Last date and time record changed")
        ]
    },
    "bidofferperiod": {
        "description": "Bid offer period details",
        "columns": [
            ("DUID", "STRING", True, "Dispatchable Unit Identifier"),
            ("BIDTYPE", "STRING", True, "Type of bid"),
            ("SETTLEMENTDATE", "DATE", True, "Settlement date"),
            ("OFFERDATE", "TIMESTAMP", True, "Offer date and time"),
            ("PERIODID", "INT", True, "Trading interval period ID"),
            ("VERSIONNO", "INT", False, "Version number"),
            ("MAXAVAIL", "DECIMAL(15,5)", False, "Maximum availability MW"),
            ("FIXEDLOAD", "DECIMAL(15,5)", False, "Fixed load MW"),
            ("ROCUP", "DECIMAL(15,5)", False, "Rate of change up MW/min"),
            ("ROCDOWN", "DECIMAL(15,5)", False, "Rate of change down MW/min"),
            ("ENABLEMENTMIN", "DECIMAL(15,5)", False, "Minimum enablement MW"),
            ("ENABLEMENTMAX", "DECIMAL(15,5)", False, "Maximum enablement MW"),
            ("LOWBREAKPOINT", "DECIMAL(15,5)", False, "Low breakpoint MW"),
            ("HIGHBREAKPOINT", "DECIMAL(15,5)", False, "High breakpoint MW"),
            ("BANDAVAIL1", "DECIMAL(15,5)", False, "Band 1 availability MW"),
            ("BANDAVAIL2", "DECIMAL(15,5)", False, "Band 2 availability MW"),
            ("BANDAVAIL3", "DECIMAL(15,5)", False, "Band 3 availability MW"),
            ("BANDAVAIL4", "DECIMAL(15,5)", False, "Band 4 availability MW"),
            ("BANDAVAIL5", "DECIMAL(15,5)", False, "Band 5 availability MW"),
            ("BANDAVAIL6", "DECIMAL(15,5)", False, "Band 6 availability MW"),
            ("BANDAVAIL7", "DECIMAL(15,5)", False, "Band 7 availability MW"),
            ("BANDAVAIL8", "DECIMAL(15,5)", False, "Band 8 availability MW"),
            ("BANDAVAIL9", "DECIMAL(15,5)", False, "Band 9 availability MW"),
            ("BANDAVAIL10", "DECIMAL(15,5)", False, "Band 10 availability MW"),
            ("LASTCHANGED", "TIMESTAMP", False, "Last date and time record changed"),
            ("PASAAVAILABILITY", "DECIMAL(15,5)", False, "PASA availability MW")
        ]
    },
    "biddayoffer": {
        "description": "Bid day offer data",
        "columns": [
            ("SETTLEMENTDATE", "DATE", True, "Settlement date"),
            ("DUID", "STRING", True, "Dispatchable Unit Identifier"),
            ("BIDTYPE", "STRING", True, "Type of bid"),
            ("PRICEBAND1", "DECIMAL(15,5)", False, "Price band 1 $/MWh"),
            ("PRICEBAND2", "DECIMAL(15,5)", False, "Price band 2 $/MWh"),
            ("PRICEBAND3", "DECIMAL(15,5)", False, "Price band 3 $/MWh"),
            ("PRICEBAND4", "DECIMAL(15,5)", False, "Price band 4 $/MWh"),
            ("PRICEBAND5", "DECIMAL(15,5)", False, "Price band 5 $/MWh"),
            ("PRICEBAND6", "DECIMAL(15,5)", False, "Price band 6 $/MWh"),
            ("PRICEBAND7", "DECIMAL(15,5)", False, "Price band 7 $/MWh"),
            ("PRICEBAND8", "DECIMAL(15,5)", False, "Price band 8 $/MWh"),
            ("PRICEBAND9", "DECIMAL(15,5)", False, "Price band 9 $/MWh"),
            ("PRICEBAND10", "DECIMAL(15,5)", False, "Price band 10 $/MWh"),
            ("MINIMUMLOAD", "DECIMAL(15,5)", False, "Minimum generation MW"),
            ("T1", "DECIMAL(15,5)", False, "Time to synchronize minutes"),
            ("T2", "DECIMAL(15,5)", False, "Time to minimum load minutes"),
            ("T3", "DECIMAL(15,5)", False, "Time to full capacity minutes"),
            ("T4", "DECIMAL(15,5)", False, "Time to shutdown minutes"),
            ("LASTCHANGED", "TIMESTAMP", False, "Last date and time record changed")
        ]
    },
    "biddayoffer_d": {
        "description": "Bid day offer daily summary",
        "columns": [
            ("SETTLEMENTDATE", "DATE", True, "Settlement date"),
            ("DUID", "STRING", True, "Dispatchable Unit Identifier"),
            ("BIDTYPE", "STRING", True, "Type of bid"),
            ("OFFERDATE", "TIMESTAMP", False, "Offer date and time"),
            ("VERSIONNO", "INT", False, "Version number"),
            ("PARTICIPANTID", "STRING", False, "Participant identifier"),
            ("DAILYENERGYCONSTRAINT", "DECIMAL(15,5)", False, "Daily energy constraint MWh"),
            ("REBIDEXPLANATION", "STRING", False, "Explanation for rebid"),
            ("PRICEBAND1", "DECIMAL(15,5)", False, "Price band 1 $/MWh"),
            ("PRICEBAND2", "DECIMAL(15,5)", False, "Price band 2 $/MWh"),
            ("PRICEBAND3", "DECIMAL(15,5)", False, "Price band 3 $/MWh"),
            ("PRICEBAND4", "DECIMAL(15,5)", False, "Price band 4 $/MWh"),
            ("PRICEBAND5", "DECIMAL(15,5)", False, "Price band 5 $/MWh"),
            ("PRICEBAND6", "DECIMAL(15,5)", False, "Price band 6 $/MWh"),
            ("PRICEBAND7", "DECIMAL(15,5)", False, "Price band 7 $/MWh"),
            ("PRICEBAND8", "DECIMAL(15,5)", False, "Price band 8 $/MWh"),
            ("PRICEBAND9", "DECIMAL(15,5)", False, "Price band 9 $/MWh"),
            ("PRICEBAND10", "DECIMAL(15,5)", False, "Price band 10 $/MWh"),
            ("LASTCHANGED", "TIMESTAMP", False, "Last date and time record changed")
        ]
    },
    "dispatchprice": {
        "description": "Dispatch price data for each trading interval",
        "columns": [
            ("SETTLEMENTDATE", "DATE", True, "Settlement date"),
            ("RUNNO", "INT", True, "Dispatch run number"),
            ("REGIONID", "STRING", True, "Region identifier"),
            ("PERIODID", "INT", True, "Trading interval period ID"),
            ("INTERVENTION", "INT", False, "Intervention pricing flag"),
            ("RRP", "DECIMAL(15,5)", False, "Regional Reference Price $/MWh"),
            ("EEP", "DECIMAL(15,5)", False, "Energy price $/MWh"),
            ("ROP", "DECIMAL(15,5)", False, "ROP price $/MWh"),
            ("APCFLAG", "INT", False, "Administered price cap flag"),
            ("MARKETSUSPENDEDFLAG", "INT", False, "Market suspended flag"),
            ("LASTCHANGED", "TIMESTAMP", False, "Last date and time record changed"),
            ("RAISE6SECRRP", "DECIMAL(15,5)", False, "Raise 6 second RRP $/MWh"),
            ("RAISE60SECRRP", "DECIMAL(15,5)", False, "Raise 60 second RRP $/MWh"),
            ("RAISE5MINRRP", "DECIMAL(15,5)", False, "Raise 5 minute RRP $/MWh"),
            ("RAISEREGRRP", "DECIMAL(15,5)", False, "Raise regulation RRP $/MWh"),
            ("LOWER6SECRRP", "DECIMAL(15,5)", False, "Lower 6 second RRP $/MWh"),
            ("LOWER60SECRRP", "DECIMAL(15,5)", False, "Lower 60 second RRP $/MWh"),
            ("LOWER5MINRRP", "DECIMAL(15,5)", False, "Lower 5 minute RRP $/MWh"),
            ("LOWERREGRRP", "DECIMAL(15,5)", False, "Lower regulation RRP $/MWh")
        ]
    },
    "predispatchprice": {
        "description": "Pre-dispatch price forecasts",
        "columns": [
            ("PREDISPATCHSEQNO", "STRING", True, "Pre-dispatch sequence number"),
            ("RUNNO", "INT", True, "Run number"),
            ("REGIONID", "STRING", True, "Region identifier"),
            ("PERIODID", "STRING", True, "Period identifier"),
            ("INTERVENTION", "INT", False, "Intervention flag"),
            ("RRP", "DECIMAL(15,5)", False, "Regional Reference Price $/MWh"),
            ("EEP", "DECIMAL(15,5)", False, "Energy price $/MWh"),
            ("ROP", "DECIMAL(15,5)", False, "ROP price $/MWh"),
            ("APCFLAG", "INT", False, "Administered price cap flag"),
            ("MARKETSUSPENDEDFLAG", "INT", False, "Market suspended flag"),
            ("LASTCHANGED", "TIMESTAMP", False, "Last date and time record changed"),
            ("RAISE6SECRRP", "DECIMAL(15,5)", False, "Raise 6 second RRP $/MWh"),
            ("RAISE60SECRRP", "DECIMAL(15,5)", False, "Raise 60 second RRP $/MWh"),
            ("RAISE5MINRRP", "DECIMAL(15,5)", False, "Raise 5 minute RRP $/MWh"),
            ("RAISEREGRRP", "DECIMAL(15,5)", False, "Raise regulation RRP $/MWh"),
            ("LOWER6SECRRP", "DECIMAL(15,5)", False, "Lower 6 second RRP $/MWh"),
            ("LOWER60SECRRP", "DECIMAL(15,5)", False, "Lower 60 second RRP $/MWh"),
            ("LOWER5MINRRP", "DECIMAL(15,5)", False, "Lower 5 minute RRP $/MWh"),
            ("LOWERREGRRP", "DECIMAL(15,5)", False, "Lower regulation RRP $/MWh")
        ]
    },
    "setenergygensetdetail": {
        "description": "Settlement energy generation set details",
        "columns": [
            ("SETTLEMENTDATE", "DATE", True, "Settlement date"),
            ("VERSIONNO", "INT", True, "Settlement version number"),
            ("PERIODID", "INT", True, "Trading interval period ID"),
            ("PARTICIPANTID", "STRING", True, "Participant identifier"),
            ("STATIONID", "STRING", True, "Station identifier"),
            ("DUID", "STRING", True, "Dispatchable Unit Identifier"),
            ("GENSETID", "STRING", True, "Generation set identifier"),
            ("REGIONID", "STRING", False, "Region identifier"),
            ("GENERGY", "DECIMAL(16,6)", False, "Generated energy MWh"),
            ("AENERGY", "DECIMAL(16,6)", False, "Adjusted energy MWh"),
            ("GPOWER", "DECIMAL(16,6)", False, "Generated power MW"),
            ("APOWER", "DECIMAL(16,6)", False, "Adjusted power MW"),
            ("RRP", "DECIMAL(15,5)", False, "Regional Reference Price $/MWh"),
            ("EVALUE", "DECIMAL(18,8)", False, "Energy value $"),
            ("LASTCHANGED", "TIMESTAMP", False, "Last date and time record changed")
        ]
    },
    "setenergytransactions": {
        "description": "Settlement energy transactions",
        "columns": [
            ("SETTLEMENTDATE", "DATE", True, "Settlement date"),
            ("VERSIONNO", "INT", True, "Settlement version number"),
            ("PERIODID", "INT", True, "Trading interval period ID"),
            ("PARTICIPANTID", "STRING", True, "Participant identifier"),
            ("REGIONID", "STRING", True, "Region identifier"),
            ("MLF", "DECIMAL(15,5)", False, "Marginal Loss Factor"),
            ("RRP", "DECIMAL(15,5)", False, "Regional Reference Price $/MWh"),
            ("CUSTOMERENERGY", "DECIMAL(16,6)", False, "Customer energy MWh"),
            ("GENERATORENERGY", "DECIMAL(16,6)", False, "Generator energy MWh"),
            ("PURCHASEDENERGY", "DECIMAL(16,6)", False, "Purchased energy MWh"),
            ("METERRUNNO", "INT", False, "Meter run number"),
            ("LASTCHANGED", "TIMESTAMP", False, "Last date and time record changed")
        ]
    },
    "billingruntrk": {
        "description": "Billing run tracking information",
        "columns": [
            ("CONTRACTYEAR", "INT", True, "Contract year"),
            ("WEEKNO", "INT", True, "Week number"),
            ("BILLRUNNO", "INT", True, "Billing run number"),
            ("RUNTYPE", "STRING", False, "Run type (WEEKLY, REVISION)"),
            ("STARTDATE", "TIMESTAMP", False, "Billing run start date and time"),
            ("ENDDATE", "TIMESTAMP", False, "Billing run end date and time"),
            ("STATUS", "STRING", False, "Billing run status"),
            ("LASTCHANGED", "TIMESTAMP", False, "Last date and time record changed")
        ]
    },
    "billingdaytrk": {
        "description": "Billing daily tracking information",
        "columns": [
            ("CONTRACTYEAR", "INT", True, "Contract year"),
            ("WEEKNO", "INT", True, "Week number"),
            ("BILLRUNNO", "INT", True, "Billing run number"),
            ("SETTLEMENTDATE", "DATE", True, "Settlement date"),
            ("RUNNO", "INT", False, "Settlement run number"),
            ("LASTCHANGED", "TIMESTAMP", False, "Last date and time record changed")
        ]
    },
    "rooftoppvactual": {
        "description": "Rooftop PV actual generation data",
        "columns": [
            ("INTERVAL_DATETIME", "TIMESTAMP", True, "Date and time of interval"),
            ("TYPE", "STRING", True, "Data type identifier"),
            ("REGIONID", "STRING", True, "Region identifier"),
            ("POWER", "DECIMAL(16,6)", False, "Actual power output MW"),
            ("QI", "INT", False, "Quality indicator"),
            ("LASTCHANGED", "TIMESTAMP", False, "Last date and time record changed")
        ]
    }
}

created_count = 0
failed_count = 0
failed_tables = []

for table_name, table_def in aemo_table_definitions.items():
    full_table_name = f"{CATALOG_NAME}.{SCHEMA_NAME}.{table_name}"

    try:
        print(f"Creating {table_name}...")

        # Build column definitions
        col_defs = []
        for col_name, col_type, is_required, comment in table_def["columns"]:
            not_null = "NOT NULL" if is_required else ""
            comment_escaped = comment.replace("'", "''")
            col_defs.append(f"{col_name} {col_type} {not_null} COMMENT '{comment_escaped}'")

        columns_joined = ",\n    ".join(col_defs)
        table_comment_escaped = table_def["description"].replace("'", "''")

        # Build CREATE TABLE SQL
        create_sql = f"""CREATE TABLE IF NOT EXISTS {full_table_name} (
    {columns_joined}
)
USING DELTA
COMMENT '{table_comment_escaped}'
"""

        # Execute CREATE TABLE
        spark.sql(create_sql)
        print(f"✓ Created {table_name}")
        created_count += 1

    except Exception as e:
        print(f"✗ Failed to create {table_name}: {e}")
        failed_count += 1
        failed_tables.append(table_name)

print("=" * 60)
print(f"Table Creation Complete!")
print(f"  Created: {created_count}")
print(f"  Failed: {failed_count}")
if failed_tables:
    print(f"  Failed tables: {', '.join(failed_tables)}")
print("=" * 60)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 5: PDF Metadata Extractor Class

# COMMAND ----------

class ElectricityDataModelExtractor:
    """Extract table metadata from AEMO Electricity Data Model PDF files"""

    def __init__(self):
        self.extracted_metadata = {}
        self.current_table = None
        self.processing_content = False

    def extract_table_metadata_from_pdf(self, pdf_path: str) -> Dict[str, Dict[str, str]]:
        """
        Extract table and column metadata from Electricity Data Model PDF

        Args:
            pdf_path: Path to the PDF file in Volumes or DBFS

        Returns:
            Dictionary with table names as keys and column metadata as values
        """
        metadata = {}

        try:
            with pdfplumber.open(pdf_path) as pdf:
                print(f"Processing Electricity Data Model PDF with {len(pdf.pages)} pages")

                for page_num, page in enumerate(pdf.pages, 1):
                    print(f"Processing page {page_num}")

                    # Extract text to find table sections
                    text = page.extract_text()
                    if not text:
                        continue

                    # Look for table definitions and content sections
                    page_metadata = self._extract_from_text_blocks(text, page_num)
                    metadata.update(page_metadata)

                    # Also try table extraction for structured content
                    tables = page.extract_tables()
                    if tables:
                        table_metadata = self._extract_from_tables(tables, text, page_num)
                        metadata.update(table_metadata)

        except Exception as e:
            print(f"Error extracting metadata from {pdf_path}: {e}")
            raise

        return metadata

    def _extract_from_text_blocks(self, text: str, page_num: int) -> Dict[str, Dict[str, str]]:
        """Extract metadata from text blocks using pattern matching"""
        metadata = {}

        # Split text into lines for processing
        lines = text.split('\n')
        current_table = None
        in_content_section = False
        content_lines = []

        for i, line in enumerate(lines):
            line = line.strip()

            # Look for table name pattern: "X.X Table: TABLENAME"
            table_match = re.search(r'\d+\.\d+\s+Table:\s+([A-Z_]+)', line)
            if table_match:
                # Process previous table if exists
                if current_table and content_lines:
                    table_metadata = self._parse_content_lines(content_lines)
                    if table_metadata:
                        metadata[current_table.lower()] = table_metadata
                        print(f"  Extracted {len(table_metadata)} columns for table: {current_table}")

                # Start new table
                current_table = table_match.group(1)
                in_content_section = False
                content_lines = []
                print(f"  Found table: {current_table}")
                continue

            # Look for content section header
            if re.search(r'\d+\.\d+\.\d+\s+Content', line):
                in_content_section = True
                content_lines = []
                continue

            # Look for next section (stops content collection)
            if re.search(r'\d+\.\d+(\.\d+)?\s+[A-Z]', line) and not re.search(r'Content', line):
                in_content_section = False

            # Collect content lines
            if in_content_section and current_table and line:
                content_lines.append(line)

        # Process final table
        if current_table and content_lines:
            table_metadata = self._parse_content_lines(content_lines)
            if table_metadata:
                metadata[current_table.lower()] = table_metadata
                print(f"  Extracted {len(table_metadata)} columns for table: {current_table}")

        return metadata

    def _parse_content_lines(self, lines: List[str]) -> Dict[str, Dict[str, str]]:
        """Parse content lines to extract column information"""
        columns = {}
        current_column = None
        current_data_type = None
        current_mandatory = None
        current_comment_lines = []

        # Skip header lines (Name, Data Type, Mandatory, Comment)
        start_idx = 0
        for i, line in enumerate(lines):
            if 'mandatory' in line.lower() and 'comment' in line.lower():
                start_idx = i + 1
                break

        for line in lines[start_idx:]:
            line = line.strip()
            if not line:
                continue

            # Check if this looks like a new column definition
            column_match = re.match(r'^([A-Z_][A-Z0-9_]*)\s+(VARCHAR2\(\d+\)|NUMBER\(\d+,\d+\)|NUMBER\(\d+\)|DATE)\s*(X)?\s*(.*)', line)

            if column_match:
                # Save previous column if exists
                if current_column:
                    comment_text = ' '.join(current_comment_lines).strip()
                    columns[current_column.lower()] = {
                        'comment': comment_text,
                        'data_type': current_data_type,
                        'mandatory': current_mandatory == 'X'
                    }

                # Start new column
                current_column = column_match.group(1)
                current_data_type = column_match.group(2)
                current_mandatory = column_match.group(3)
                comment_start = column_match.group(4).strip()
                current_comment_lines = [comment_start] if comment_start else []
            else:
                # Check if this is just a column name (sometimes spans multiple lines)
                column_name_match = re.match(r'^([A-Z_][A-Z0-9_]*)\s*$', line)
                if column_name_match and not current_column:
                    current_column = column_name_match.group(1)
                    current_data_type = ''
                    current_mandatory = ''
                    current_comment_lines = []
                # Check if this is a data type line
                elif re.match(r'^(VARCHAR2\(\d+\)|NUMBER\(\d+,\d+\)|NUMBER\(\d+\)|DATE)\s*(X)?\s*(.*)', line) and current_column and not current_data_type:
                    type_match = re.match(r'^(VARCHAR2\(\d+\)|NUMBER\(\d+,\d+\)|NUMBER\(\d+\)|DATE)\s*(X)?\s*(.*)', line)
                    current_data_type = type_match.group(1)
                    current_mandatory = type_match.group(2)
                    comment_start = type_match.group(3).strip()
                    current_comment_lines = [comment_start] if comment_start else []
                # Otherwise, this is likely a continuation of the comment
                elif current_column and line:
                    # Skip lines that look like section headers
                    if not re.match(r'^\d+\.\d+', line):
                        current_comment_lines.append(line)

        # Save final column
        if current_column:
            comment_text = ' '.join(current_comment_lines).strip()
            columns[current_column.lower()] = {
                'comment': comment_text,
                'data_type': current_data_type,
                'mandatory': current_mandatory == 'X'
            }

        return columns

    def _extract_from_tables(self, tables: List[List[List[str]]], text: str, page_num: int) -> Dict[str, Dict[str, str]]:
        """Extract metadata from structured tables as backup method"""
        metadata = {}

        # Find current table name from text
        table_match = re.search(r'\d+\.\d+\s+Table:\s+([A-Z_]+)', text)
        if not table_match:
            return metadata

        current_table = table_match.group(1).lower()

        for table_idx, table in enumerate(tables):
            if not table or len(table) < 2:
                continue

            # Look for content table (has Name, Data Type, Mandatory, Comment columns)
            if len(table[0]) >= 4:
                header = [str(cell).strip().lower() if cell else '' for cell in table[0]]

                # Check if this looks like a content table
                if ('name' in header[0] and 'type' in ' '.join(header) and 'comment' in ' '.join(header)):
                    print(f"  Found structured content table for {current_table}")
                    table_metadata = {}

                    for row in table[1:]:
                        if len(row) >= 4 and row[0]:
                            col_name = str(row[0]).strip()
                            data_type = str(row[1]).strip() if row[1] else ''
                            mandatory = str(row[2]).strip() if row[2] else ''
                            comment = str(row[3]).strip() if row[3] else ''

                            if col_name and col_name.replace('_', '').isalnum():
                                table_metadata[col_name.lower()] = {
                                    'comment': comment,
                                    'data_type': data_type,
                                    'mandatory': mandatory == 'X'
                                }

                    if table_metadata:
                        metadata[current_table] = table_metadata
                        print(f"  Extracted {len(table_metadata)} columns from structured table")

        return metadata

# Initialize extractor
extractor = ElectricityDataModelExtractor()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 6: Unity Catalog Metadata Manager

# COMMAND ----------

class UnityMetadataManager:
    """Manage Unity Catalog metadata operations with flexible column matching"""

    def __init__(self, catalog_name: str, schema_name: str = None):
        self.catalog_name = catalog_name
        self.schema_name = schema_name

        # Mismatch handling policies
        self.mismatch_policies = {
            'pdf_extra_columns': 'warn',  # 'warn', 'ignore', 'error'
            'unity_extra_columns': 'warn',  # 'warn', 'ignore', 'error'
            'column_name_matching': 'flexible',  # 'exact', 'flexible', 'fuzzy'
            'skip_on_mismatch': False,  # Skip entire table if major mismatches
            'require_minimum_match': 0.5  # Require at least 50% column overlap
        }

    def set_mismatch_policy(self, policy_name: str, value):
        """Set how to handle column mismatches"""
        if policy_name in self.mismatch_policies:
            self.mismatch_policies[policy_name] = value
            print(f"✓ Set {policy_name} = {value}")
        else:
            print(f"✗ Unknown policy: {policy_name}")

    def get_unity_catalog_tables(self) -> List[str]:
        """Get list of tables in Unity Catalog"""
        tables = []

        try:
            if self.schema_name:
                # Get tables from specific schema
                tables_df = spark.sql(f"SHOW TABLES IN {self.catalog_name}.{self.schema_name}")
                for row in tables_df.collect():
                    table_name = row['tableName']
                    tables.append(f"{self.catalog_name}.{self.schema_name}.{table_name}")
            else:
                # Get all schemas and their tables
                schemas_df = spark.sql(f"SHOW SCHEMAS IN {self.catalog_name}")
                for schema_row in schemas_df.collect():
                    schema_name = schema_row['databaseName']
                    try:
                        tables_df = spark.sql(f"SHOW TABLES IN {self.catalog_name}.{schema_name}")
                        for table_row in tables_df.collect():
                            table_name = table_row['tableName']
                            tables.append(f"{self.catalog_name}.{schema_name}.{table_name}")
                    except Exception as e:
                        print(f"Warning: Could not access schema {schema_name}: {e}")
        except Exception as e:
            print(f"Error getting Unity Catalog tables: {e}")
            raise

        return tables

    def get_table_columns(self, full_table_name: str) -> Dict[str, Dict[str, str]]:
        """Get current column information for a table"""
        try:
            # Use DESCRIBE to get column information
            desc_df = spark.sql(f"DESCRIBE {full_table_name}")
            columns_info = {}

            for row in desc_df.collect():
                col_name = row['col_name']
                data_type = row['data_type']
                comment = row['comment'] if 'comment' in row and row['comment'] else ''

                # Skip partition information and metadata rows
                if col_name.startswith('#') or col_name == '':
                    continue

                columns_info[col_name] = {
                    'data_type': data_type,
                    'current_comment': comment
                }

            return columns_info

        except Exception as e:
            print(f"Error getting columns for {full_table_name}: {e}")
            return {}

    def _normalize_column_name(self, col_name: str) -> str:
        """Normalize column name for flexible matching"""
        return col_name.lower().strip().replace(' ', '_').replace('-', '_')

    def _find_column_matches(self, pdf_columns: Dict[str, Dict], unity_columns: Dict[str, Dict]) -> Dict[str, Dict]:
        """Find column matches between PDF and Unity with flexible matching"""
        matches = {
            'exact_matches': {},  # PDF col -> Unity col (exact match)
            'flexible_matches': {},  # PDF col -> Unity col (normalized match)
            'fuzzy_matches': {},  # PDF col -> Unity col (similarity match)
            'pdf_only': set(),  # Columns only in PDF
            'unity_only': set(),  # Columns only in Unity
            'match_summary': {}  # Summary statistics
        }

        # Normalize all column names for comparison
        pdf_normalized = {self._normalize_column_name(k): k for k in pdf_columns.keys()}
        unity_normalized = {self._normalize_column_name(k): k for k in unity_columns.keys()}

        # Step 1: Find exact matches (case-insensitive)
        for pdf_norm, pdf_orig in pdf_normalized.items():
            if pdf_norm in unity_normalized:
                unity_orig = unity_normalized[pdf_norm]
                matches['exact_matches'][pdf_orig] = {
                    'unity_column': unity_orig,
                    'match_type': 'exact',
                    'confidence': 1.0,
                    'pdf_data': pdf_columns[pdf_orig],
                    'unity_data': unity_columns[unity_orig]
                }

        # Step 2: Find flexible matches (if enabled)
        if self.mismatch_policies['column_name_matching'] in ['flexible', 'fuzzy']:
            for pdf_orig, pdf_data in pdf_columns.items():
                if pdf_orig not in matches['exact_matches']:
                    # Try partial matching, abbreviations, etc.
                    best_match = self._find_best_column_match(pdf_orig, unity_columns.keys())
                    if best_match:
                        matches['flexible_matches'][pdf_orig] = {
                            'unity_column': best_match['column'],
                            'match_type': 'flexible',
                            'confidence': best_match['confidence'],
                            'pdf_data': pdf_data,
                            'unity_data': unity_columns[best_match['column']]
                        }

        # Step 3: Identify unmatched columns
        matched_pdf = set(matches['exact_matches'].keys()) | set(matches['flexible_matches'].keys())
        matched_unity = set()
        for match_data in list(matches['exact_matches'].values()) + list(matches['flexible_matches'].values()):
            matched_unity.add(match_data['unity_column'])

        matches['pdf_only'] = set(pdf_columns.keys()) - matched_pdf
        matches['unity_only'] = set(unity_columns.keys()) - matched_unity

        # Step 4: Calculate match statistics
        total_pdf = len(pdf_columns)
        total_unity = len(unity_columns)
        total_matched = len(matched_pdf)

        matches['match_summary'] = {
            'total_pdf_columns': total_pdf,
            'total_unity_columns': total_unity,
            'total_matched': total_matched,
            'match_percentage': (total_matched / total_pdf * 100) if total_pdf > 0 else 0,
            'coverage_percentage': (total_matched / total_unity * 100) if total_unity > 0 else 0
        }

        return matches

    def _find_best_column_match(self, pdf_column: str, unity_columns: List[str]) -> Optional[Dict]:
        """Find best matching Unity column for a PDF column using fuzzy matching"""
        # Simple fuzzy matching strategies
        pdf_norm = self._normalize_column_name(pdf_column)
        best_match = None
        best_confidence = 0.0

        for unity_col in unity_columns:
            unity_norm = self._normalize_column_name(unity_col)

            # Strategy 1: Substring matching
            if pdf_norm in unity_norm or unity_norm in pdf_norm:
                confidence = min(len(pdf_norm), len(unity_norm)) / max(len(pdf_norm), len(unity_norm))
                if confidence > best_confidence and confidence > 0.7:
                    best_match = {'column': unity_col, 'confidence': confidence}
                    best_confidence = confidence

            # Strategy 2: Common abbreviations/expansions
            abbreviations = {
                'id': 'identifier', 'desc': 'description', 'amt': 'amount',
                'qty': 'quantity', 'num': 'number', 'dt': 'date', 'tm': 'time'
            }

            for abbr, full in abbreviations.items():
                if (abbr in pdf_norm and full in unity_norm) or (full in pdf_norm and abbr in unity_norm):
                    confidence = 0.8
                    if confidence > best_confidence:
                        best_match = {'column': unity_col, 'confidence': confidence}
                        best_confidence = confidence

        return best_match if best_confidence > 0.6 else None

    def generate_review_data(self, pdf_metadata: Dict[str, Dict[str, str]]) -> Dict[str, Dict]:
        """Generate comprehensive review data with mismatch handling"""
        review_data = {}
        unity_tables = self.get_unity_catalog_tables()

        print(f"Found {len(unity_tables)} Unity Catalog tables in {self.catalog_name}.{self.schema_name}")
        print(f"Using mismatch policies: {self.mismatch_policies}")

        for table_name, column_metadata in pdf_metadata.items():
            # Find matching Unity Catalog table
            target_table_name = f"{self.catalog_name}.{self.schema_name}.{table_name}"
            matching_tables = [t for t in unity_tables if t.lower() == target_table_name.lower()]

            if not matching_tables:
                matching_tables = [t for t in unity_tables if t.split('.')[-1].lower() == table_name.lower()]

            table_review = {
                'pdf_table_name': table_name,
                'target_table_name': target_table_name,
                'unity_matches': matching_tables,
                'status': 'no_match',
                'current_columns': {},
                'proposed_changes': {},
                'column_matches': {},
                'mismatch_analysis': {},
                'warnings': [],
                'recommendations': [],
                'approval_required': True
            }

            if not matching_tables:
                table_review['warnings'].append(f"Table {target_table_name} not found in Unity Catalog")
                table_review['recommendations'].append(f"Create table {target_table_name} or check if it exists with different name")
            elif len(matching_tables) > 1:
                table_review['warnings'].append(f"Multiple matching tables: {matching_tables}")
                table_review['recommendations'].append("Review table names for conflicts")
                table_review['status'] = 'multiple_matches'
            else:
                full_table_name = matching_tables[0]
                table_review['unity_table_name'] = full_table_name

                # Get current column information
                current_columns = self.get_table_columns(full_table_name)
                table_review['current_columns'] = current_columns

                if not current_columns:
                    table_review['warnings'].append("Could not retrieve column information")
                    table_review['status'] = 'error'
                else:
                    # Perform column matching analysis
                    column_matches = self._find_column_matches(column_metadata, current_columns)
                    table_review['column_matches'] = column_matches

                    # Analyze mismatches
                    mismatch_analysis = self._analyze_mismatches(column_matches)
                    table_review['mismatch_analysis'] = mismatch_analysis

                    # Generate proposed changes based on matches
                    proposed_changes = self._generate_proposed_changes(column_matches, column_metadata, current_columns)
                    table_review['proposed_changes'] = proposed_changes

                    # Determine status and recommendations
                    status_info = self._determine_table_status(mismatch_analysis, proposed_changes)
                    table_review.update(status_info)

            review_data[table_name] = table_review

        return review_data

    def _analyze_mismatches(self, column_matches: Dict) -> Dict:
        """Analyze column mismatches and generate detailed report"""
        analysis = {
            'severity': 'low',
            'issues': [],
            'recommendations': [],
            'statistics': column_matches['match_summary']
        }

        match_pct = column_matches['match_summary']['match_percentage']
        pdf_only_count = len(column_matches['pdf_only'])
        unity_only_count = len(column_matches['unity_only'])

        # Analyze match percentage
        if match_pct < 50:
            analysis['severity'] = 'high'
            analysis['issues'].append(f"Low column match rate: {match_pct:.1f}%")
            analysis['recommendations'].append("Review table structure compatibility")
        elif match_pct < 80:
            analysis['severity'] = 'medium'
            analysis['issues'].append(f"Moderate column match rate: {match_pct:.1f}%")

        # Analyze PDF-only columns
        if pdf_only_count > 0:
            analysis['issues'].append(f"{pdf_only_count} columns in PDF not found in Unity table")
            if self.mismatch_policies['pdf_extra_columns'] == 'error':
                analysis['severity'] = 'high'
                analysis['recommendations'].append("All PDF columns must exist in Unity table")
            elif self.mismatch_policies['pdf_extra_columns'] == 'warn':
                analysis['recommendations'].append("Consider adding missing columns to Unity table")

        # Analyze Unity-only columns
        if unity_only_count > 0:
            analysis['issues'].append(f"{unity_only_count} columns in Unity table not documented in PDF")
            if self.mismatch_policies['unity_extra_columns'] == 'error':
                analysis['severity'] = 'high'
                analysis['recommendations'].append("All Unity columns must be documented in PDF")
            elif self.mismatch_policies['unity_extra_columns'] == 'warn':
                analysis['recommendations'].append("Consider documenting additional Unity columns")

        return analysis

    def _generate_proposed_changes(self, column_matches: Dict, pdf_metadata: Dict, unity_columns: Dict) -> Dict:
        """Generate proposed changes based on column matches"""
        proposed_changes = {}

        # Process exact and flexible matches
        all_matches = {**column_matches['exact_matches'], **column_matches['flexible_matches']}

        for pdf_col, match_info in all_matches.items():
            unity_col = match_info['unity_column']
            pdf_data = match_info['pdf_data']
            unity_data = match_info['unity_data']

            current_comment = unity_data['current_comment']
            new_comment = pdf_data['comment']

            proposed_changes[unity_col] = {
                'pdf_column': pdf_col,
                'current_comment': current_comment,
                'new_comment': new_comment,
                'change_type': 'update' if current_comment else 'add',
                'match_type': match_info['match_type'],
                'match_confidence': match_info['confidence'],
                'data_type': pdf_data.get('data_type', ''),
                'mandatory': pdf_data.get('mandatory', False),
                'will_change': current_comment != new_comment
            }

        return proposed_changes

    def _determine_table_status(self, mismatch_analysis: Dict, proposed_changes: Dict) -> Dict:
        """Determine table status based on mismatch analysis"""
        status_info = {
            'status': 'matched',
            'warnings': [],
            'recommendations': [],
            'approval_required': True
        }

        # Add mismatch warnings and recommendations
        status_info['warnings'].extend(mismatch_analysis['issues'])
        status_info['recommendations'].extend(mismatch_analysis['recommendations'])

        # Determine if table should be skipped
        if (self.mismatch_policies['skip_on_mismatch'] and
                mismatch_analysis['severity'] == 'high'):
            status_info['status'] = 'skipped_mismatch'
            status_info['warnings'].append("Table skipped due to high mismatch severity")
            status_info['approval_required'] = False
        elif (mismatch_analysis['statistics']['match_percentage'] <
                self.mismatch_policies['require_minimum_match'] * 100):
            status_info['status'] = 'insufficient_match'
            status_info['warnings'].append(f"Match rate below minimum threshold")
            status_info['approval_required'] = False
        else:
            # Count actual changes
            changes_count = sum(1 for change in proposed_changes.values() if change['will_change'])
            if changes_count > 0:
                status_info['status'] = 'ready_for_update'
                status_info['recommendations'].append(f"Will update {changes_count} column comments")
            else:
                status_info['status'] = 'up_to_date'
                status_info['recommendations'].append("No changes needed - comments already match")
                status_info['approval_required'] = False

        return status_info

    def apply_metadata_to_table(self, full_table_name: str, column_metadata: Dict[str, Dict[str, str]]) -> bool:
        """Apply metadata comments to Unity Catalog table using SQL"""
        try:
            success_count = 0
            for column_name, meta in column_metadata.items():
                comment = meta['comment'].replace("'", "''")  # Escape single quotes
                try:
                    # Use ALTER TABLE to add/update column comment
                    sql = f"ALTER TABLE {full_table_name} ALTER COLUMN {column_name} COMMENT '{comment}'"
                    spark.sql(sql)
                    success_count += 1
                    print(f"  ✓ Updated comment for {full_table_name}.{column_name}")
                except Exception as e:
                    print(f"  ✗ Failed to update comment for {full_table_name}.{column_name}: {e}")

            return success_count == len(column_metadata)
        except Exception as e:
            print(f"Error applying metadata to {full_table_name}: {e}")
            return False

    def apply_metadata_updates(self, review_data: Dict, approved_tables: List[str] = None):
        """Apply approved metadata updates to Unity Catalog tables"""
        results = {
            'successful_updates': 0,
            'failed_updates': 0,
            'skipped_updates': 0,
            'details': {}
        }

        for table_name, data in review_data.items():
            # Skip if not in approved list (if provided)
            if approved_tables and table_name not in approved_tables:
                results['skipped_updates'] += 1
                results['details'][table_name] = 'not_approved'
                continue

            # Only process tables that are ready for update
            if data.get('status') != 'ready_for_update':
                results['skipped_updates'] += 1
                results['details'][table_name] = data.get('status', 'unknown')
                continue

            # Check if there are actual changes to apply
            column_metadata = {}
            for unity_col, change in data.get('proposed_changes', {}).items():
                if change.get('will_change', False):
                    column_metadata[unity_col] = {'comment': change['new_comment']}

            if not column_metadata:
                results['skipped_updates'] += 1
                results['details'][table_name] = 'no_changes'
                continue

            # Apply the changes
            full_table_name = data.get('unity_table_name', f"{self.catalog_name}.{self.schema_name}.{table_name}")
            print(f"\nUpdating {table_name} ({len(column_metadata)} columns)...")

            try:
                success = self.apply_metadata_to_table(full_table_name, column_metadata)
                if success:
                    results['successful_updates'] += 1
                    results['details'][table_name] = 'success'
                    print(f"✓ Successfully updated {table_name}")
                else:
                    results['failed_updates'] += 1
                    results['details'][table_name] = 'failed'
                    print(f"✗ Failed to update {table_name}")
            except Exception as e:
                results['failed_updates'] += 1
                results['details'][table_name] = f'error: {str(e)}'
                print(f"✗ Error updating {table_name}: {e}")

        return results

# Initialize Unity Catalog manager
unity_manager = UnityMetadataManager(CATALOG_NAME, SCHEMA_NAME)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 7: Extract Metadata from PDF

# COMMAND ----------

print("=" * 60)
print("EXTRACTING METADATA FROM AEMO PDF")
print("=" * 60)

try:
    # Extract metadata from PDF
    pdf_metadata = extractor.extract_table_metadata_from_pdf(PDF_FILE_PATH)

    print(f"\n✓ Extraction complete!")
    print(f"  Tables found: {len(pdf_metadata)}")

    # Show summary
    for table_name, columns in pdf_metadata.items():
        print(f"  - {table_name}: {len(columns)} columns")

except Exception as e:
    print(f"✗ Error extracting PDF metadata: {e}")
    print("\nTroubleshooting:")
    print(f"1. Check PDF file exists at: {PDF_FILE_PATH}")
    print(f"2. Verify file is accessible from Databricks")
    print(f"3. Ensure PDF follows AEMO Electricity Data Model format")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 8: Generate Review Data

# COMMAND ----------

print("=" * 60)
print("GENERATING REVIEW DATA")
print("=" * 60)

try:
    # Ensure output directory exists
    dbutils.fs.mkdirs(REVIEW_OUTPUT_PATH)

    # Generate review data
    review_data = unity_manager.generate_review_data(pdf_metadata)

    if review_data:
        print(f"\n✓ Review data generated for {len(review_data)} tables")

        # Save review data
        review_json_path = f"{REVIEW_OUTPUT_PATH}/review_data.json"
        dbutils.fs.put(review_json_path, json.dumps(review_data, indent=2), True)
        print(f"  Saved to: {review_json_path}")

        # Show summary
        status_counts = {}
        for data in review_data.values():
            status = data.get('status', 'unknown')
            status_counts[status] = status_counts.get(status, 0) + 1

        print("\n  Status Summary:")
        for status, count in status_counts.items():
            print(f"    {status}: {count} tables")

        # Show tables ready for update
        ready_tables = [name for name, data in review_data.items() 
                       if data.get('status') == 'ready_for_update']

        if ready_tables:
            print(f"\n  Tables ready for update: {len(ready_tables)}")
            for table in ready_tables:
                changes = sum(1 for change in review_data[table].get('proposed_changes', {}).values()
                            if change.get('will_change', False))
                print(f"    - {table}: {changes} column changes")
    else:
        print("\n✗ No review data generated")

except Exception as e:
    print(f"✗ Error generating review data: {e}")
    import traceback
    traceback.print_exc()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 9: Review and Approve Changes

# COMMAND ----------

print("=" * 60)
print("REVIEW CHECKPOINT")
print("=" * 60)

if 'review_data' not in locals() or not review_data:
    print("✗ No review data available")
    print("  Run the previous cells first")
else:
    # Find tables that need updates
    update_plan = []
    total_changes = 0

    for table_name, data in review_data.items():
        status = data.get('status')
        changes = sum(1 for change in data.get('proposed_changes', {}).values()
                     if change.get('will_change', False))

        if status == 'ready_for_update' and changes > 0:
            update_plan.append({
                'table': table_name,
                'changes': changes,
                'unity_table': data.get('unity_table_name', f"{CATALOG_NAME}.{SCHEMA_NAME}.{table_name}")
            })
            total_changes += changes

    if update_plan:
        print(f"\nProposed Changes:")
        print(f"  Environment: {CATALOG_NAME}.{SCHEMA_NAME}")
        print(f"  Tables to update: {len(update_plan)}")
        print(f"  Total column changes: {total_changes}")

        print(f"\nDetailed Plan:")
        for plan in update_plan:
            print(f"  - {plan['table']}: {plan['changes']} column changes")

            # Show sample changes
            table_data = review_data[plan['table']]
            sample_count = 0
            for col_name, change in table_data.get('proposed_changes', {}).items():
                if change.get('will_change', False) and sample_count < 3:
                    current = (change.get('current_comment', '') or '')[:40]
                    new = change.get('new_comment', '')[:40]
                    print(f"      {col_name}: '{current}' → '{new}'")
                    sample_count += 1

        print(f"\n⚠️  This will modify Unity Catalog metadata")
        print(f"    Review the changes above carefully before proceeding")
        print(f"\n    To apply changes, run the next cell")

        # Store approved plan
        APPROVED_PLAN = update_plan
    else:
        print(f"\n✓ No changes needed - all tables are up to date")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 10: Apply Approved Changes

# COMMAND ----------

print("=" * 60)
print("APPLYING METADATA UPDATES")
print("=" * 60)

if 'APPROVED_PLAN' not in locals():
    print("✗ No approved plan found")
    print("  Run the review checkpoint cell above first")
else:
    # Apply the changes
    approved_tables = [plan['table'] for plan in APPROVED_PLAN]
    results = unity_manager.apply_metadata_updates(review_data, approved_tables)

    # Show results
    print(f"\nResults:")
    print(f"  ✓ Successful: {results['successful_updates']}")
    print(f"  ✗ Failed: {results['failed_updates']}")
    print(f"  ⊘ Skipped: {results['skipped_updates']}")

    if results['successful_updates'] > 0:
        print(f"\n✓ Successfully updated {results['successful_updates']} tables!")

    if results['failed_updates'] > 0:
        print(f"\n✗ {results['failed_updates']} tables failed - check error messages above")

    # Save results
    results_path = f"{REVIEW_OUTPUT_PATH}/execution_results.json"
    final_results = {
        'timestamp': datetime.now().isoformat(),
        'environment': f"{CATALOG_NAME}.{SCHEMA_NAME}",
        'statistics': {
            'successful_updates': results['successful_updates'],
            'failed_updates': results['failed_updates'],
            'skipped_updates': results['skipped_updates']
        },
        'detailed_results': results['details']
    }

    dbutils.fs.put(results_path, json.dumps(final_results, indent=2), True)
    print(f"\n  Results saved to: {results_path}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 11: Verify Changes

# COMMAND ----------

print("=" * 60)
print("VERIFYING APPLIED CHANGES")
print("=" * 60)

if 'results' not in locals() or not results:
    print("✗ No execution results found")
    print("  Run the 'Apply Changes' cell above first")
else:
    verification_passed = 0
    verification_failed = 0

    for table_name, result_status in results['details'].items():
        if result_status == 'success':
            table_data = review_data.get(table_name, {})
            full_table_name = table_data.get('unity_table_name', f"{CATALOG_NAME}.{SCHEMA_NAME}.{table_name}")

            print(f"\nVerifying {table_name}...")

            try:
                # Get current column information after updates
                current_columns = unity_manager.get_table_columns(full_table_name)

                # Check each column that was supposed to be updated
                table_passed = True
                for unity_col, change in table_data.get('proposed_changes', {}).items():
                    if change.get('will_change', False):
                        expected_comment = change['new_comment']
                        actual_comment = current_columns.get(unity_col, {}).get('current_comment', '')

                        if actual_comment == expected_comment:
                            print(f"  ✓ {unity_col}: Verified")
                        else:
                            print(f"  ✗ {unity_col}: Mismatch!")
                            print(f"    Expected: '{expected_comment[:50]}'")
                            print(f"    Actual: '{actual_comment[:50]}'")
                            table_passed = False

                if table_passed:
                    verification_passed += 1
                else:
                    verification_failed += 1

            except Exception as e:
                print(f"  ✗ Error verifying {table_name}: {e}")
                verification_failed += 1

    # Verification summary
    print(f"\n{'=' * 60}")
    print("VERIFICATION SUMMARY")
    print("=" * 60)
    print(f"  ✓ Passed: {verification_passed} tables")
    print(f"  ✗ Failed: {verification_failed} tables")

    if verification_failed == 0:
        print(f"\n✓ ALL VERIFICATIONS PASSED!")
        print(f"  Metadata automation completed successfully!")
    else:
        print(f"\n⚠️  Some verifications failed - check details above")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Usage Instructions
# MAGIC
# MAGIC ### Quick Start
# MAGIC 1. **Update Configuration** (Step 2):
# MAGIC    - Set `CATALOG_NAME` to your Unity Catalog name
# MAGIC    - Set `SCHEMA_NAME` to your schema for AEMO data
# MAGIC    - Set `PDF_FILE_PATH` to your AEMO PDF location
# MAGIC
# MAGIC 2. **Run Cells in Order**:
# MAGIC    - Steps 1-3: Setup and create catalog/schema
# MAGIC    - Step 4: Create AEMO table structures
# MAGIC    - Steps 5-6: Initialize extractors and managers
# MAGIC    - Steps 7-8: Extract PDF metadata and generate review
# MAGIC    - Steps 9-10: Review and apply changes
# MAGIC    - Step 11: Verify applied changes
# MAGIC
# MAGIC ### Supported AEMO Tables
# MAGIC - **Bidding**: bidperoffer_d, bidperoffer, bidofferperiod, biddayoffer, biddayoffer_d
# MAGIC - **Pricing**: dispatchprice, predispatchprice
# MAGIC - **Settlement**: setenergygensetdetail, setenergytransactions
# MAGIC - **Billing**: billingruntrk, billingdaytrk
# MAGIC - **Solar**: rooftoppvactual
# MAGIC
# MAGIC ### Configuration Options
# MAGIC Adjust mismatch policies before generating review:
# MAGIC ```python
# MAGIC unity_manager.set_mismatch_policy('pdf_extra_columns', 'warn')  # warn, ignore, error
# MAGIC unity_manager.set_mismatch_policy('unity_extra_columns', 'warn')
# MAGIC unity_manager.set_mismatch_policy('column_name_matching', 'flexible')  # exact, flexible, fuzzy
# MAGIC unity_manager.set_mismatch_policy('require_minimum_match', 0.5)  # 50% minimum overlap
# MAGIC ```
