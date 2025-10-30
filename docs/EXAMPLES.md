1. Initialize managers
extractor = ElectricityDataModelExtractor()
unity_manager = UnityMetadataManager(CATALOG_NAME, SCHEMA_NAME)

2. Extract metadata from PDF
pdf_metadata = extractor.extract_table_metadata_from_pdf(PDF_FILE_PATH)

3. Generate review data
review_data = unity_manager.generate_review_data(pdf_metadata)

4. Apply approved changes
approved_tables = ['dispatchprice', 'bidperoffer_d']
results = unity_manager.apply_metadata_updates(review_data, approved_tables)

text

## Custom Mismatch Policies

Strict matching
unity_manager.set_mismatch_policy('column_name_matching', 'exact')
unity_manager.set_mismatch_policy('pdf_extra_columns', 'error')

Flexible matching
unity_manager.set_mismatch_policy('column_name_matching', 'fuzzy')
unity_manager.set_mismatch_policy('require_minimum_match', 0.3)

text

## Custom Table Definition

custom_table = {
"custom_market_data": {
"description": "Custom market data table",
"columns": [
("TRADING_DATE", "DATE", True, "Trading date"),
("MARKET_PRICE", "DECIMAL(15,5)", False, "Market price in $/MWh"),
("VOLUME", "DECIMAL(16,6)", False, "Trading volume in MWh"),
]
}
}

Add to existing definitions
aemo_table_definitions.update(custom_table)

text

## Selective Table Updates

Update only specific tables
priority_tables = ['dispatchprice', 'setenergygensetdetail']
results = unity_manager.apply_metadata_updates(review_data, priority_tables)

Review results
print(f"Updated: {results['successful_updates']}")
print(f"Failed: {results['failed_updates']}")

