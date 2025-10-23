# src/file_parser.py

import pandas as pd
import chardet
from typing import Tuple, Dict, List
from dataclasses import dataclass

@dataclass
class ParseResult:
    """Result of file parsing"""
    success: bool
    dataframe: pd.DataFrame
    metadata: Dict
    errors: List[str]

class FileParser:
    """Intelligent file parser with auto-detection"""
    
    def __init__(self, table_definition):
        self.table_def = table_definition
        
    def parse(self, file_path: str) -> ParseResult:
        """Parse file and return DataFrame"""
        
        errors = []
        metadata = {
            'file_path': file_path,
            'table_name': self.table_def.table_name
        }
        
        try:
            # Detect encoding
            detected_encoding = self._detect_encoding(file_path)
            encoding_to_use = detected_encoding or self.table_def.encoding
            metadata['detected_encoding'] = detected_encoding
            metadata['encoding_used'] = encoding_to_use
            
            print(f"  Encoding: {encoding_to_use}")
            
            # Get column names
            column_names = [col.name for col in sorted(self.table_def.columns, key=lambda x: x.position)]
            metadata['expected_columns'] = len(column_names)
            
            # Parse file
            df = pd.read_csv(
                file_path,
                sep=self.table_def.delimiter,
                encoding=encoding_to_use,
                names=column_names,
                dtype=str,
                keep_default_na=False,
                on_bad_lines='skip'
            )
            
            metadata['actual_columns'] = len(df.columns)
            metadata['total_records'] = len(df)
            
            print(f"  Records: {len(df)}, Columns: {len(df.columns)}")
            
            # Validate column count
            if len(df.columns) != len(column_names):
                errors.append(
                    f"Column count mismatch: Expected {len(column_names)}, found {len(df.columns)}"
                )
            
            # Replace empty strings with None
            df = df.replace('', None)
            
            return ParseResult(
                success=len(errors) == 0,
                dataframe=df,
                metadata=metadata,
                errors=errors
            )
            
        except Exception as e:
            errors.append(f"Failed to parse file: {str(e)}")
            return ParseResult(
                success=False,
                dataframe=pd.DataFrame(),
                metadata=metadata,
                errors=errors
            )
    
    def _detect_encoding(self, file_path: str) -> str:
        """Detect file encoding"""
        try:
            with open(file_path, 'rb') as f:
                raw_data = f.read(100000)
                result = chardet.detect(raw_data)
                return result['encoding']
        except Exception:
            return None