# src/file_parser.py - COMPLETE FIXED VERSION

import pandas as pd
import chardet
from typing import Dict, List
from dataclasses import dataclass

@dataclass
class ParseResult:
    """Result of file parsing"""
    success: bool
    dataframe: pd.DataFrame = None
    errors: List[str] = None
    metadata: Dict = None

class FileParser:
    """Parse and validate file structure"""
    
    def __init__(self, table_definition):
        self.table_def = table_definition
        
    def parse(self, file_path: str) -> ParseResult:
        """Parse file and return DataFrame"""
        
        try:
            # Detect encoding
            encoding = self._detect_encoding(file_path)
            print(f"  Encoding: {encoding}")
            
            # Read file
            df = pd.read_csv(
                file_path,
                sep=self.table_def.delimiter,
                encoding=encoding,
                header=None,
                dtype=str,
                na_filter=False
            )
            
            # Set column names from table definition
            expected_cols = len(self.table_def.columns)
            actual_cols = len(df.columns)
            
            print(f"  Records: {len(df)}, Columns: {actual_cols}")
            
            # Validate column count
            if actual_cols != expected_cols:
                return ParseResult(
                    success=False,
                    errors=[f"Column count mismatch. Expected: {expected_cols}, Found: {actual_cols}"]
                )
            
            # Assign column names
            df.columns = [col.name for col in self.table_def.columns]
            
            # Create metadata
            metadata = {
                'file_path': file_path,
                'table_name': self.table_def.name,  # FIXED: Changed from table_name to name
                'encoding_used': encoding,
                'expected_columns': expected_cols,
                'actual_columns': actual_cols,
                'total_records': len(df)
            }
            
            return ParseResult(
                success=True,
                dataframe=df,
                errors=[],
                metadata=metadata
            )
            
        except Exception as e:
            return ParseResult(
                success=False,
                errors=[f"Error parsing file: {str(e)}"]
            )
    
    def _detect_encoding(self, file_path: str) -> str:
        """Detect file encoding"""
        
        try:
            with open(file_path, 'rb') as f:
                raw_data = f.read(100000)  # Read first 100KB
                result = chardet.detect(raw_data)
                detected_encoding = result['encoding']
                
                # Fallback to UTF-8 if detection fails
                if not detected_encoding:
                    return 'utf-8'
                
                # Handle common encoding variations
                encoding_map = {
                    'ascii': 'utf-8',
                    'ISO-8859-1': 'latin-1',
                    'Windows-1252': 'cp1252'
                }
                
                return encoding_map.get(detected_encoding, detected_encoding)
                
        except Exception:
            # Default to UTF-8 if detection fails
            return 'utf-8'