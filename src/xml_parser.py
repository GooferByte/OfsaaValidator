# src/xml_parser.py - COMPLETE FIXED VERSION

import xml.etree.ElementTree as ET
from dataclasses import dataclass
from typing import List, Dict
from pathlib import Path

@dataclass
class ColumnDefinition:
    """Column definition from XML template"""
    position: int
    name: str
    data_type: str
    length: int
    nullable: bool
    requirement: str = "O"  # M=Mandatory, O=Optional
    description: str = ""

@dataclass
class TableDefinition:
    """Table definition from XML template"""
    name: str
    description: str
    columns: List[ColumnDefinition]
    delimiter: str = "~"
    encoding: str = "UTF-8"
    date_format: str = "YYYYMMDD"

class XMLTemplateParser:
    """Parse XML templates for table definitions"""
    
    def __init__(self, templates_dir: str):
        self.templates_dir = Path(templates_dir)
        
    def parse_all_templates(self) -> Dict[str, TableDefinition]:
        """Parse all XML templates in the directory"""
        
        templates = {}
        
        if not self.templates_dir.exists():
            raise ValueError(f"Templates directory not found: {self.templates_dir}")
        
        xml_files = list(self.templates_dir.glob("*.xml"))
        
        if not xml_files:
            raise ValueError(f"No XML templates found in: {self.templates_dir}")
        
        print(f"Loading {len(xml_files)} XML template(s)...")
        
        for xml_file in xml_files:
            try:
                table_def = self.parse_template(str(xml_file))
                templates[table_def.name] = table_def
                print(f"  ✓ Loaded {table_def.name} ({len(table_def.columns)} columns)")
            except Exception as e:
                print(f"  ✗ Failed to load {xml_file.name}: {e}")
        
        return templates
    
    def parse_template(self, xml_path: str) -> TableDefinition:
        """Parse single XML template file"""
        
        try:
            # Read file content and strip any leading whitespace
            with open(xml_path, 'r', encoding='utf-8') as f:
                content = f.read().lstrip()
            
            # Parse from string
            root = ET.fromstring(content)
            
            table_name = root.get('name')
            if not table_name:
                raise ValueError(f"Table name not found in {xml_path}")
            
            table_desc = root.get('description', '')
            
            # Parse file format
            file_format = root.find('FileFormat')
            delimiter = file_format.get('delimiter', '~') if file_format is not None else '~'
            encoding = file_format.get('encoding', 'UTF-8') if file_format is not None else 'UTF-8'
            date_format = file_format.get('dateFormat', 'YYYYMMDD') if file_format is not None else 'YYYYMMDD'
            
            # Parse columns - Try both 'Columns' and 'columns' (case insensitive)
            columns = []
            columns_elem = root.find('Columns')
            if columns_elem is None:
                columns_elem = root.find('columns')
            
            if columns_elem is not None:
                # Try both 'Column' and 'column'
                column_list = columns_elem.findall('Column')
                if not column_list:
                    column_list = columns_elem.findall('column')
                
                for col in column_list:
                    # Parse nullable
                    nullable_str = col.get('nullable', 'true').lower()
                    nullable = nullable_str in ['true', 'yes', '1']
                    
                    # Parse length
                    length_str = col.get('length', '0')
                    try:
                        length = int(length_str)
                    except ValueError:
                        length = 0
                    
                    # Parse requirement attribute (M=Mandatory, O=Optional)
                    requirement = col.get('requirement', 'O').upper()
                    
                    # Parse position
                    position_str = col.get('position', '0')
                    try:
                        position = int(position_str)
                    except ValueError:
                        position = 0
                    
                    column_def = ColumnDefinition(
                        position=position,
                        name=col.get('name', ''),
                        data_type=col.get('dataType', 'VARCHAR2'),
                        length=length,
                        nullable=nullable,
                        requirement=requirement,
                        description=col.get('description', '')
                    )
                    columns.append(column_def)
            
            if not columns:
                print(f"  Warning: No columns found in {xml_path}")
            
            # Sort by position
            columns.sort(key=lambda x: x.position)
            
            return TableDefinition(
                name=table_name,
                description=table_desc,
                columns=columns,
                delimiter=delimiter,
                encoding=encoding,
                date_format=date_format
            )
            
        except ET.ParseError as e:
            raise ValueError(f"Invalid XML: {e}")
        except Exception as e:
            raise ValueError(f"Error parsing template: {e}")