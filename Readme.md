# README.md

# OFSAA FCCM Data Validation System

Automated validation tool for OFSAA FCCM data files with comprehensive error reporting and fix recommendations.

## Features

✅ **XML Template-Based Validation**
- Automatically loads table definitions from XML templates
- Supports multiple FCCM tables
- Easy to add new tables

✅ **Comprehensive Validation**
- Mandatory field checks
- Data type validation
- Length validation
- Format validation (email, phone, dates)

✅ **Detailed Reporting**
- JSON report with full error details
- HTML report with visual dashboard
- Excel report with multiple sheets
- Fix instructions text file

✅ **Smart Processing**
- Auto-detects table name from filename
- Auto-detects file encoding
- Batch processing support
- Progress tracking for large files

## Installation
```bash
# 1. Clone/download the project
cd ofsaa-validator

# 2. Install dependencies
pip install -r requirements.txt

# 3. Place XML templates in config/templates/
# 4. Place data files in data/input/
```

## Quick Start

### Single File Validation
```bash
# Auto-detect table from filename
python validate.py data/input/AccountAddress_20251015.dat

# Specify table explicitly
python validate.py data/input/myfile.dat --table ACCT_ADDR
```

### Batch Validation
```bash
# Validate all files in directory
python validate.py data/input --batch
```

## XML Template Format

Place your XML templates in `config/templates/`. Example:
```xml
<?xml version="1.0" encoding="UTF-8"?>
<Table name="ACCT_ADDR" description="Account Address Dimension">
    <FileFormat delimiter="~" encoding="UTF-8" dateFormat="YYYYMMDD"/>
    <Columns>
        <Column position="0" name="v_account_number" dataType="VARCHAR2" 
                length="50" nullable="false" description="Account Number"/>
        <Column position="1" name="n_addr_seq_id" dataType="NUMBER" 
                length="10" nullable="false" description="Address Sequence ID"/>
        <!-- Add more columns -->
    </Columns>
</Table>
```

## Output Files

After validation, check `data/output/{filename}/`:

1. **valid_records.csv** - Records ready for OFSAA load
2. **rejected_records.csv** - Records with errors
3. **validation_report.html** - Visual dashboard (open in browser)
4. **validation_report.json** - Detailed JSON report
5. **validation_report.xlsx** - Excel report with multiple sheets
6. **fix_instructions.txt** - Step-by-step fix guide

## Understanding the Reports

### Data Quality Score

- **95-100%**: ✓ Excellent - Ready for OFSAA load
- **85-94%**: ⚠ Good - Minor issues to review
- **70-84%**: ⚠ Fair - Significant issues to fix
- **<70%**: ✗ Poor - Major data quality problems

### Error Types

- **VALUE_MISSING**: Mandatory field is empty
- **INVALID_DATA_TYPE**: Wrong data type (e.g., text in number field)
- **LENGTH_EXCEEDED**: Value too long for field
- **INVALID_FORMAT**: Format doesn't match pattern (email, phone, date)

## Command Line Options
```
python validate.py <input> [options]

Arguments:
  input                 Input file or directory

Options:
  -t, --table TABLE     Specify table name (auto-detected if not provided)
  --templates DIR       Templates directory (default: config/templates)
  -b, --batch           Batch mode: validate all files in directory
  -h, --help            Show help message
```

## Examples

### Example 1: Validate AccountAddress file
```bash
python validate.py data/input/AccountAddress_20251015_DLY_01.dat
```

**Output:**
```
================================================================================
OFSAA FCCM Validation System
================================================================================

Loading 5 XML template(s)...
  ✓ Loaded ACCT_ADDR (14 columns)
  ✓ Loaded DIM_ACCOUNT (10 columns)
  ✓ Loaded DIM_CUSTOMER (10 columns)
  ✓ Loaded DIM_BRANCH (9 columns)
  ✓ Loaded FCT_ACCOUNT_BALANCE (8 columns)

Loaded 5 table definition(s)
Available tables: ACCT_ADDR, DIM_ACCOUNT, DIM_CUSTOMER, DIM_BRANCH, FCT_ACCOUNT_BALANCE

Auto-detected table: ACCT_ADDR

================================================================================
Validating: AccountAddress_20251015_DLY_01.dat
Table: ACCT_ADDR
================================================================================

Step 1/3: Parsing file
  Encoding: utf-8
  Records: 2389, Columns: 14
  ✓ Parsing successful

Step 2/3: Schema validation

  Validating 2389 records...
  Found 9 validation errors
  ✓ Validation complete

Step 3/3: Processing results

Generating reports...
  ✓ JSON report: data/output/AccountAddress_20251015_DLY_01/validation_report.json
  ✓ HTML report: data/output/AccountAddress_20251015_DLY_01/validation_report.html
  ✓ Excel report: data/output/AccountAddress_20251015_DLY_01/validation_report.xlsx
  ✓ Fix instructions: data/output/AccountAddress_20251015_DLY_01/fix_instructions.txt

================================================================================
VALIDATION SUMMARY
================================================================================
Total Records:     2,389
Valid Records:     2,380 (99.62%)
Rejected Records:  9
Total Errors:      9
Processing Time:   2.34s
================================================================================

✓ Data Quality: EXCELLENT - Ready for OFSAA load

✓ Validation completed successfully!

Output directory: data/output/AccountAddress_20251015_DLY_01/
```

### Example 2: Batch validate multiple files
```bash
python validate.py data/input --batch
```

### Example 3: Specify custom templates directory
```bash
python validate.py data/input/file.dat --templates /path/to/templates
```

## Troubleshooting

### Issue: "No table definitions loaded"
**Solution:** Ensure XML templates are in `config/templates/` directory

### Issue: "Cannot detect table name"
**Solution:** Use `--table` parameter to specify table explicitly
```bash
python validate.py myfile.dat --table ACCT_ADDR
```

### Issue: "Column count mismatch"
**Solution:** Check that your data file has the same number of columns as defined in XML template

### Issue: "Encoding error"
**Solution:** Tool auto-detects encoding, but you can convert file to UTF-8:
```bash
iconv -f ISO-8859-1 -t UTF-8 input.dat > output.dat
```

## Integration with ETL Pipeline

### Python Integration
```python
from src.orchestrator import ValidationOrchestrator

# Initialize
orchestrator = ValidationOrchestrator('config/templates')

# Validate
result = orchestrator.validate_file('data/input/myfile.dat', 'ACCT_ADDR')

# Check quality
if result['summary']['data_quality_score'] >= 95:
    print("✓ Ready for OFSAA load")
    # Proceed with OFSAA load
else:
    print("✗ Fix errors first")
    # Send notification
```

### Shell Script Integration
```bash
#!/bin/bash
# pre_load_validation.sh

FILE=$1
TABLE=$2

# Run validation
python validate.py "$FILE" --table "$TABLE"

# Check exit code
if [ $? -eq 0 ]; then
    echo "✓ Validation passed - proceeding with OFSAA load"
    # Your OFSAA load command here
else
    echo "✗ Validation failed - check reports"
    exit 1
fi
```

## Adding New Tables

1. Create XML template in `config/templates/`
2. Define table structure with all columns
3. Run validation - it automatically loads the new template

Example: `config/templates/DimProduct.xml`
```xml
<?xml version="1.0" encoding="UTF-8"?>
<Table name="DIM_PRODUCT" description="Product Dimension">
    <FileFormat delimiter="~" encoding="UTF-8" dateFormat="YYYYMMDD"/>
    <Columns>
        <Column position="0" name="v_product_code" dataType="VARCHAR2" 
                length="50" nullable="false"/>
        <Column position="1" name="v_product_name" dataType="VARCHAR2" 
                length="255" nullable="false"/>
        <!-- Add more columns -->
    </Columns>
</Table>
```

## Support

For issues or questions:
1. Check validation reports (HTML/Excel)
2. Review fix_instructions.txt
3. Check rejected_records.csv for specific errors

## License

Internal use only - Your Company Name