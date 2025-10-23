# src/validator.py - COMPLETE FIXED VERSION WITH REQUIREMENT SUPPORT

import pandas as pd
import re
from typing import List, Dict, Any
from datetime import datetime
from dataclasses import dataclass

@dataclass
class ValidationError:
    """Validation error details"""
    row_number: int
    column_name: str
    error_type: str
    error_message: str
    actual_value: Any
    expected_value: str = ""
    fix_recommendation: str = ""
    
    def to_dict(self):
        return {
            'row': self.row_number,
            'column': self.column_name,
            'error_type': self.error_type,
            'message': self.error_message,
            'actual_value': str(self.actual_value) if self.actual_value is not None else 'NULL',
            'expected_value': self.expected_value,
            'fix_recommendation': self.fix_recommendation
        }

class SchemaValidator:
    """Schema-level validation engine with fix recommendations"""
    
    def __init__(self, table_definition):
        self.table_def = table_definition
        self.columns = {col.name: col for col in table_definition.columns}
        
    def validate(self, df: pd.DataFrame) -> List[ValidationError]:
        """Validate DataFrame against schema rules"""
        
        print(f"\n  Validating {len(df)} records...")
        
        all_errors = []
        
        # Use enumerate to ensure integer row numbers
        for row_number, (idx, row) in enumerate(df.iterrows(), start=1):
            row_errors = self._validate_row(row, row_number)
            all_errors.extend(row_errors)
            
            # Print progress every 1000 rows
            if row_number % 1000 == 0:
                print(f"    Processed {row_number} records...")
        
        print(f"  Found {len(all_errors)} validation errors")
        
        return all_errors
    
    def _validate_row(self, row: pd.Series, row_number: int) -> List[ValidationError]:
        """Validate a single row"""
        errors = []
        
        for col_name, col_config in self.columns.items():
            value = row.get(col_name)
            
            # 1. Mandatory check - Check BOTH requirement attribute AND nullable
            # Priority: requirement="M" takes precedence
            # A field is mandatory if: requirement="M" OR nullable=false
            is_mandatory = (hasattr(col_config, 'requirement') and col_config.requirement == 'M') or (not col_config.nullable)
            
            if is_mandatory:
                error = self._check_mandatory(value, col_config, row_number)
                if error:
                    errors.append(error)
                    continue
            
            # Skip other checks if value is empty (and field is optional)
            if value is None or (isinstance(value, str) and value.strip() == ''):
                continue
            
            # 2. Data type check
            error = self._check_data_type(value, col_config, row_number)
            if error:
                errors.append(error)
            
            # 3. Length check
            error = self._check_length(value, col_config, row_number)
            if error:
                errors.append(error)
            
            # 4. Format check
            error = self._check_format(value, col_config, row_number)
            if error:
                errors.append(error)
        
        return errors
    
    def _check_mandatory(self, value: Any, col_config, row_number: int) -> ValidationError:
        """Check mandatory field with fix recommendation"""
        
        if value is None or (isinstance(value, str) and value.strip() == ''):
            
            # Generate fix recommendation
            fix_rec = self._get_mandatory_fix_recommendation(col_config.name)
            
            # Determine why field is mandatory
            requirement_info = ""
            if hasattr(col_config, 'requirement') and col_config.requirement == 'M':
                requirement_info = " (Requirement: M)"
            elif not col_config.nullable:
                requirement_info = " (Nullable: false)"
            
            return ValidationError(
                row_number=row_number,
                column_name=col_config.name,
                error_type='VALUE_MISSING',
                error_message=f"{col_config.name} [Value Missing]{requirement_info}",
                actual_value=value,
                expected_value='Non-null value',
                fix_recommendation=fix_rec
            )
        
        return None
    
    def _check_data_type(self, value: Any, col_config, row_number: int) -> ValidationError:
        """Validate data type with fix recommendation"""
        
        data_type = col_config.data_type
        col_name = col_config.name
        
        try:
            if data_type in ['NUMBER', 'INTEGER', 'NUMERIC']:
                float(str(value).replace(',', ''))
                
            elif data_type == 'DATE':
                self._parse_date(str(value))
                
            elif data_type in ['VARCHAR', 'VARCHAR2', 'CHAR']:
                str(value)
            
            return None
            
        except (ValueError, TypeError):
            fix_rec = self._get_datatype_fix_recommendation(value, data_type)
            
            return ValidationError(
                row_number=row_number,
                column_name=col_name,
                error_type='INVALID_DATA_TYPE',
                error_message=f"Invalid {data_type} format",
                actual_value=value,
                expected_value=f"Valid {data_type}",
                fix_recommendation=fix_rec
            )
    
    def _check_length(self, value: Any, col_config, row_number: int) -> ValidationError:
        """Validate length with fix recommendation"""
        
        max_length = col_config.length
        col_name = col_config.name
        actual_length = len(str(value))
        
        if max_length and actual_length > max_length:
            return ValidationError(
                row_number=row_number,
                column_name=col_name,
                error_type='LENGTH_EXCEEDED',
                error_message=f"Length exceeds maximum {max_length} characters",
                actual_value=value,
                expected_value=f"Max {max_length} characters",
                fix_recommendation=f"Truncate to {max_length} characters: '{str(value)[:max_length]}...'"
            )
        
        return None
    
    def _check_format(self, value: Any, col_config, row_number: int) -> ValidationError:
        """Check format patterns with fix recommendation"""
        
        col_name = col_config.name
        value_str = str(value).strip()
        
        # Email validation
        if 'email' in col_name.lower() and value_str:
            email_pattern = r'^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$'
            if not re.match(email_pattern, value_str):
                return ValidationError(
                    row_number=row_number,
                    column_name=col_name,
                    error_type='INVALID_FORMAT',
                    error_message="Invalid email format",
                    actual_value=value,
                    expected_value='valid@email.com',
                    fix_recommendation='Provide valid email address (e.g., user@example.com)'
                )
        
        # Phone validation
        if 'phone' in col_name.lower() and value_str:
            phone_clean = re.sub(r'[\s\-\(\)\+]', '', value_str)
            if not phone_clean.isdigit():
                return ValidationError(
                    row_number=row_number,
                    column_name=col_name,
                    error_type='INVALID_FORMAT',
                    error_message="Invalid phone format",
                    actual_value=value,
                    expected_value='Valid phone number',
                    fix_recommendation='Remove non-numeric characters or provide valid phone number'
                )
        
        return None
    
    def _parse_date(self, date_str: str) -> datetime:
        """Parse date string"""
        formats = ['%Y%m%d', '%Y-%m-%d', '%d/%m/%Y', '%m/%d/%Y']
        
        for fmt in formats:
            try:
                return datetime.strptime(date_str, fmt)
            except ValueError:
                continue
        
        raise ValueError(f"Unable to parse date: {date_str}")
    
    def _get_mandatory_fix_recommendation(self, col_name: str) -> str:
        """Generate fix recommendation for mandatory fields"""
        
        recommendations = {
            'country': 'Add 2-letter country code (e.g., AO for Angola, PT for Portugal)',
            'branch': 'Provide valid branch code from your branch master data',
            'currency': 'Add 3-letter currency code (e.g., AOA, USD, EUR)',
            'account': 'Provide valid account number',
            'customer': 'Provide valid customer ID',
            'date': 'Add date in YYYYMMDD format (e.g., 20251015)',
            'status': 'Provide valid status code (e.g., ACTIVE, CLOSED)',
            'type': 'Provide valid type code',
            'address': 'Provide valid address information',
            'seq': 'Provide valid sequence ID'
        }
        
        col_lower = col_name.lower()
        
        for key, rec in recommendations.items():
            if key in col_lower:
                return rec
        
        return f'Populate {col_name} with valid value'
    
    def _get_datatype_fix_recommendation(self, value: Any, data_type: str) -> str:
        """Generate fix recommendation for data type errors"""
        
        if data_type in ['NUMBER', 'INTEGER', 'NUMERIC']:
            return f"Remove non-numeric characters. Current value '{value}' contains invalid characters"
        elif data_type == 'DATE':
            return f"Convert to YYYYMMDD format. Current value '{value}' is not a valid date"
        else:
            return f"Ensure value is valid {data_type}"