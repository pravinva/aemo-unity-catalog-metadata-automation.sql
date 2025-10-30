# Troubleshooting Guide

## Common Issues

### Table Not Found
**Error**: "Table not found in Unity Catalog"

**Solution**:
- Verify table exists: `SHOW TABLES IN catalog.schema`
- Create tables using Step 4 in the notebook
- Check table name spelling and case sensitivity

### Low Column Match Rate
**Error**: "Low column match rate: X%"

**Solution**:
- Verify PDF format matches AEMO Electricity Data Model structure
- Adjust `column_name_matching` policy to `'fuzzy'`
- Review column names in both PDF and Unity Catalog

### Permission Denied
**Error**: "Permission denied" or "Access denied"

**Solution**:
- Ensure you have `ALTER` permissions on target tables
- Verify Unity Catalog admin or table owner role
- Contact workspace administrator for permission grants

### PDF Extraction Failure
**Error**: "Error extracting metadata from PDF"

**Solution**:
- Verify PDF file path is correct and accessible
- Ensure PDF follows AEMO Electricity Data Model format
- Check PDF is not corrupted or password-protected
- Confirm PDF file exists: `dbutils.fs.ls("/path/to/pdf/")`

### Changes Not Applied
**Error**: "Failed to update" or changes not reflected

**Solution**:
- Check execution results for specific error messages
- Verify table is not locked by another process
- Ensure SQL syntax in ALTER statements is valid
- Confirm you have commit permissions

## Debug Mode

Enable detailed logging:

import logging
logging.basicConfig(level=logging.DEBUG)


## Support

For unresolved issues, open a GitHub issue with:
- Error message
- Steps to reproduce
- Databricks Runtime version
- Relevant code snippets

