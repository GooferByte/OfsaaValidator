# src/orchestrator.py - COMPLETE WITH CORRECT INDENTATION

from typing import Dict, Tuple
from datetime import datetime
from pathlib import Path
import pandas as pd
import re

from src.xml_parser import XMLTemplateParser
from src.file_parser import FileParser
from src.validator import SchemaValidator
from src.report_generator import ReportGenerator

class ValidationOrchestrator:
    """Main orchestrator for validation workflow"""
    
    def __init__(self, templates_dir: str = "config/templates"):
        print(f"\n{'='*80}")
        print("OFSAA FCCM Validation System")
        print(f"{'='*80}\n")
        
        # Load all XML templates
        parser = XMLTemplateParser(templates_dir)
        self.table_definitions = parser.parse_all_templates()
        
        if not self.table_definitions:
            raise ValueError("No table definitions loaded!")
        
        print(f"\nLoaded {len(self.table_definitions)} table definition(s)")
        print(f"Available tables: {', '.join(self.table_definitions.keys())}\n")
        
        self.report_generator = ReportGenerator()
    
    def validate_file(self, file_path: str, table_name: str = None) -> Dict:
        """Validate a file"""
        
        start_time = datetime.now()
        
        # Auto-detect table name if not provided
        if not table_name:
            table_name = self._detect_table_name(file_path)
            print(f"Auto-detected table: {table_name}")
        
        # Validate table exists
        if table_name not in self.table_definitions:
            raise ValueError(
                f"Table '{table_name}' not found. "
                f"Available: {', '.join(self.table_definitions.keys())}"
            )
        
        table_def = self.table_definitions[table_name]
        
        print(f"\n{'='*80}")
        print(f"Validating: {Path(file_path).name}")
        print(f"Table: {table_name}")
        print(f"{'='*80}\n")
        
        # Step 1: Parse file
        print("Step 1/3: Parsing file")
        parser = FileParser(table_def)
        parse_result = parser.parse(file_path)
        
        if not parse_result.success:
            print("  ✗ Parsing failed")
            for error in parse_result.errors:
                print(f"    - {error}")
            return self._create_error_result(parse_result.errors)
        
        df = parse_result.dataframe
        print(f"  ✓ Parsing successful\n")
        
        # Reset index to ensure simple integer index
        df = df.reset_index(drop=True)
        
        # Step 2: Validate
        print("Step 2/3: Schema validation")
        validator = SchemaValidator(table_def)
        errors = validator.validate(df)
        print(f"  ✓ Validation complete\n")
        
        # Step 3: Separate valid/rejected
        print("Step 3/3: Processing results")
        valid_df, rejected_df = self._separate_records(df, errors)
        
        # Create summary
        summary = self._create_summary(len(df), len(valid_df), len(rejected_df), errors, start_time)
        
        # Create output directory with table name and filename
        filename_stem = Path(file_path).stem
        output_dir = f"data/output/{table_name}/{filename_stem}"
        output_paths = self._save_outputs(valid_df, rejected_df, output_dir)
        
        # Generate reports
        print("\nGenerating reports...")
        self.report_generator.generate_report(
            valid_df, rejected_df, errors, summary, parse_result.metadata, output_dir
        )
        
        # Print summary
        self._print_summary(summary)
        
        # Print output location
        print(f"📁 Output Location: {output_dir}/\n")
        
        return {
            'success': True,
            'summary': summary,
            'output_paths': output_paths,
            'output_dir': output_dir,
            'errors_count': len(errors)
        }
    
    def _detect_table_name(self, file_path: str) -> str:
        """Auto-detect table name from filename with better matching (case-insensitive)"""
        
        filename = Path(file_path).stem.upper()
        
        # Remove common suffixes and date patterns
        clean_filename = filename
        clean_filename = re.sub(r'_?\d{8}', '', clean_filename)  # Remove YYYYMMDD
        clean_filename = re.sub(r'_?(DLY|MLY|DAILY|MONTHLY|WEEKLY)_?\d*', '', clean_filename, flags=re.IGNORECASE)
        clean_filename = clean_filename.strip('_')
        
        print(f"  Cleaned filename: {clean_filename}")
        
        # Create case-insensitive mapping of table names
        table_mapping = {name.upper(): name for name in self.table_definitions.keys()}
        
        # First: Try EXACT match (case-insensitive)
        if clean_filename in table_mapping:
            return table_mapping[clean_filename]
        
        # Second: Try exact match with common variations
        variation_mappings = {
            'ACCOUNTADDRESS': 'AccountAddress',
            'ACCTADDR': 'AccountAddress',
            'DIMACCOUNT': 'Account',
            'DIMCUSTOMER': 'Customer',
            'DIMBRANCH': 'Branch',
            'DIMPRODUCT': 'Product',
            'FCTACCOUNTBALANCE': 'AccountBalance',
            'FCTBALANCE': 'AccountBalance',
            'ACCT_ADDR': 'AccountAddress',
            'DIM_ACCOUNT': 'Account',
            'DIM_CUSTOMER': 'Customer',
            'DIM_BRANCH': 'Branch',
            'DIM_PRODUCT': 'Product',
            'FCT_ACCOUNT_BALANCE': 'AccountBalance'
        }
        
        if clean_filename in variation_mappings:
            target_table = variation_mappings[clean_filename]
            # Check if this table exists (case-insensitive)
            for actual_name in self.table_definitions.keys():
                if actual_name.upper() == target_table.upper():
                    return actual_name
        
        # Third: Try longest match (prefer ACCOUNTADDRESS over ACCOUNT)
        sorted_tables = sorted(self.table_definitions.keys(), key=lambda x: len(x), reverse=True)
        
        for table_name in sorted_tables:
            table_upper = table_name.upper()
            
            # Check if table name is in the cleaned filename
            if table_upper in clean_filename:
                return table_name
            
            # Check if cleaned filename is in table name
            if clean_filename in table_upper:
                return table_name
        
        # Fourth: Try fuzzy matching with pattern keywords
        patterns = {
            'ADDRESS': ['AccountAddress', 'Address'],
            'ACCOUNT': ['Account', 'AccountAddress'],
            'CUSTOMER': ['Customer'],
            'BRANCH': ['Branch'],
            'BALANCE': ['AccountBalance', 'Balance'],
            'TRANSACTION': ['Transactions'],
            'PRODUCT': ['Product'],
            'COUNTRY': ['Country'],
            'PHONE': ['AccountPhone', 'Phone'],
            'EMAIL': ['AccountEmailAddress', 'Email']
        }
        
        for pattern, possible_tables in patterns.items():
            if pattern in clean_filename:
                # Return the first matching table that exists (case-insensitive)
                for target in possible_tables:
                    for actual_name in self.table_definitions.keys():
                        if actual_name.upper() == target.upper():
                            return actual_name
        
        # Return first table if only one exists
        if len(self.table_definitions) == 1:
            return list(self.table_definitions.keys())[0]
        
        # If all else fails, raise error with suggestions
        available = ', '.join(self.table_definitions.keys())
        raise ValueError(
            f"Cannot auto-detect table name from '{filename}'.\n"
            f"Available tables: {available}\n"
            f"Please specify table_name explicitly using --table parameter"
        )
    
    def _separate_records(self, df, errors):
        """Separate valid and rejected records"""
        
        rejected_indices = set()
        for error in errors:
            idx = error.row_number - 1
            if 0 <= idx < len(df):
                rejected_indices.add(idx)
        
        valid_mask = pd.Series([i not in rejected_indices for i in range(len(df))], index=df.index)
        rejected_mask = ~valid_mask
        
        valid_df = df[valid_mask].copy()
        rejected_df = df[rejected_mask].copy()
        
        if len(rejected_df) > 0:
            rejection_map = {}
            error_count_map = {}
            
            for idx in rejected_df.index:
                row_errors = [e for e in errors if e.row_number - 1 == idx]
                if row_errors:
                    reasons = [f"{e.column_name}: {e.error_message}" for e in row_errors]
                    rejection_map[idx] = " | ".join(reasons)
                    error_count_map[idx] = len(row_errors)
                else:
                    rejection_map[idx] = "Unknown"
                    error_count_map[idx] = 0
            
            rejected_df['rejection_reasons'] = rejected_df.index.map(rejection_map)
            rejected_df['error_count'] = rejected_df.index.map(error_count_map)
        
        return valid_df, rejected_df
    
    def _create_summary(self, total, valid, rejected, errors, start_time):
        """Create validation summary"""
        
        processing_time = (datetime.now() - start_time).total_seconds()
        quality_score = (valid / total * 100) if total > 0 else 0
        
        return {
            'total_records': total,
            'valid_records': valid,
            'rejected_records': rejected,
            'data_quality_score': round(quality_score, 2),
            'processing_time_seconds': round(processing_time, 2),
            'total_errors': len(errors)
        }
    
    def _save_outputs(self, valid_df, rejected_df, output_dir):
        """Save output files"""
        
        Path(output_dir).mkdir(parents=True, exist_ok=True)
        
        paths = {}
        
        if len(valid_df) > 0:
            path = f"{output_dir}/valid_records.csv"
            valid_df.to_csv(path, index=False)
            paths['valid'] = path
        
        if len(rejected_df) > 0:
            path = f"{output_dir}/rejected_records.csv"
            rejected_df.to_csv(path, index=False)
            paths['rejected'] = path
        
        return paths
    
    def _print_summary(self, summary):
        """Print summary"""
        
        print(f"\n{'='*80}")
        print("VALIDATION SUMMARY")
        print(f"{'='*80}")
        print(f"Total Records:     {summary['total_records']:,}")
        print(f"Valid Records:     {summary['valid_records']:,} ({summary['data_quality_score']}%)")
        print(f"Rejected Records:  {summary['rejected_records']:,}")
        print(f"Total Errors:      {summary['total_errors']:,}")
        print(f"Processing Time:   {summary['processing_time_seconds']}s")
        print(f"{'='*80}\n")
        
        if summary['data_quality_score'] >= 95:
            print("✓ Data Quality: EXCELLENT - Ready for OFSAA load\n")
        elif summary['data_quality_score'] >= 85:
            print("⚠ Data Quality: GOOD - Review rejected records\n")
        elif summary['data_quality_score'] >= 70:
            print("⚠ Data Quality: FAIR - Significant issues to fix\n")
        else:
            print("✗ Data Quality: POOR - Major issues detected\n")
    
    def _create_error_result(self, errors):
        """Create error result"""
        return {
            'success': False,
            'summary': {
                'total_records': 0,
                'valid_records': 0,
                'rejected_records': 0,
                'data_quality_score': 0,
                'processing_time_seconds': 0,
                'total_errors': 0
            },
            'output_paths': {},
            'errors': errors
        }