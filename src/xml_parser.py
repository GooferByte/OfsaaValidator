# src/xml_parser.py

import xml.etree.ElementTree as ET
from typing import Dict, List
from dataclasses import dataclass
from pathlib import Path

@dataclass
class ColumnDefinition:
    """Column definition from XML template"""
    position: int
    name: str
    data_type: str
    length: int
    nullable: bool
    description: str = ""

@dataclass
class TableDefinition:
    """Complete table definition from XML"""
    table_name: str
    description: str
    delimiter: str
    encoding: str
    date_format: str
    columns: List[ColumnDefinition]

class XMLTemplateParser:
    """Parse OFSAA XML templates to extract table definitions"""
    
    def __init__(self, templates_dir: str = "config/templates"):
        self.templates_dir = Path(templates_dir)
        
    def parse_template(self, xml_path: str) -> TableDefinition:
        """
        Parse single XML template file
        
        Args:
            xml_path: Path to XML template file
            
        Returns:
            TableDefinition object
        """
        
        tree = ET.parse(xml_path)
        root = tree.getroot()
        
        # Extract table information
        table_name = self._get_table_name(root, xml_path)
        description = root.get('description', f'{table_name} dimension/fact table')
        
        # Extract file format
        file_format = self._extract_file_format(root)
        
        # Extract columns
        columns = self._extract_columns(root)
        
        return TableDefinition(
            table_name=table_name,
            description=description,
            delimiter=file_format.get('delimiter', '~'),
            encoding=file_format.get('encoding', 'UTF-8'),
            date_format=file_format.get('date_format', 'YYYYMMDD'),
            columns=columns
        )
    
    def parse_all_templates(self) -> Dict[str, TableDefinition]:
        """
        Parse all XML templates in templates directory
        
        Returns:
            Dictionary mapping table names to TableDefinition objects
        """
        
        if not self.templates_dir.exists():
            raise FileNotFoundError(f"Templates directory not found: {self.templates_dir}")
        
        templates = {}
        xml_files = list(self.templates_dir.glob("*.xml"))
        
        if not xml_files:
            raise FileNotFoundError(f"No XML templates found in {self.templates_dir}")
        
        print(f"Loading {len(xml_files)} XML template(s)...")
        
        for xml_file in xml_files:
            try:
                table_def = self.parse_template(str(xml_file))
                templates[table_def.table_name] = table_def
                print(f"  ✓ Loaded {table_def.table_name} ({len(table_def.columns)} columns)")
            except Exception as e:
                print(f"  ✗ Failed to load {xml_file.name}: {str(e)}")
        
        return templates
    
    def _get_table_name(self, root: ET.Element, xml_path: str) -> str:
        """Extract table name from XML or filename"""
        
        # Try to get from XML attributes
        table_name = root.get('name') or root.get('tableName') or root.get('table')
        
        if table_name:
            return table_name.upper()
        
        # Try from tag name
        if 'Table' in root.tag:
            table_name = root.tag.replace('Table', '')
            if table_name:
                return table_name.upper()
        
        # Fallback to filename
        filename = Path(xml_path).stem
        
        # Convert common patterns
        mappings = {
            'AccountAddress': 'ACCT_ADDR',
            'DimAccount': 'DIM_ACCOUNT',
            'DimCustomer': 'DIM_CUSTOMER',
            'DimBranch': 'DIM_BRANCH',
            'FctAccountBalance': 'FCT_ACCOUNT_BALANCE'
        }
        
        return mappings.get(filename, filename.upper())
    
    def _extract_file_format(self, root: ET.Element) -> Dict:
        """Extract file format configuration"""
        
        file_format = {}
        
        # Look for FileFormat or Format element
        format_elem = root.find('.//FileFormat') or root.find('.//Format')
        
        if format_elem is not None:
            file_format['delimiter'] = format_elem.get('delimiter', '~')
            file_format['encoding'] = format_elem.get('encoding', 'UTF-8')
            file_format['date_format'] = format_elem.get('dateFormat', 'YYYYMMDD')
        else:
            # Default values
            file_format['delimiter'] = '~'
            file_format['encoding'] = 'UTF-8'
            file_format['date_format'] = 'YYYYMMDD'
        
        return file_format
    
    def _extract_columns(self, root: ET.Element) -> List[ColumnDefinition]:
        """Extract column definitions from XML"""
        
        columns = []
        
        # Try different XML structures
        # Structure 1: <Columns><Column>...</Column></Columns>
        columns_elem = root.find('.//Columns')
        if columns_elem is not None:
            column_elems = columns_elem.findall('Column')
        else:
            # Structure 2: Direct <Column> elements
            column_elems = root.findall('.//Column')
        
        if not column_elems:
            # Structure 3: <Field> elements
            column_elems = root.findall('.//Field')
        
        for idx, col_elem in enumerate(column_elems):
            column = self._parse_column_element(col_elem, idx)
            columns.append(column)
        
        # Sort by position
        columns.sort(key=lambda x: x.position)
        
        return columns
    
    def _parse_column_element(self, elem: ET.Element, default_position: int) -> ColumnDefinition:
        """Parse single column element"""
        
        # Extract attributes with fallbacks
        name = (elem.get('name') or elem.get('columnName') or 
                elem.get('fieldName') or elem.find('Name'))
        
        if isinstance(name, ET.Element):
            name = name.text
        
        data_type = (elem.get('dataType') or elem.get('type') or 
                    elem.find('DataType'))
        
        if isinstance(data_type, ET.Element):
            data_type = data_type.text
        
        length = (elem.get('length') or elem.get('size') or 
                 elem.get('maxLength') or elem.find('Length'))
        
        if isinstance(length, ET.Element):
            length = length.text
        
        nullable = (elem.get('nullable') or elem.get('required') or 
                   elem.get('mandatory') or 'true')
        
        # Handle nullable logic
        if isinstance(nullable, str):
            nullable = nullable.lower()
            if nullable in ['false', 'n', 'no', '0', 'required', 'mandatory']:
                nullable = False
            else:
                nullable = True
        
        position = elem.get('position') or elem.get('order') or default_position
        
        description = (elem.get('description') or elem.find('Description') or 
                      elem.get('comment') or name)
        
        if isinstance(description, ET.Element):
            description = description.text
        
        return ColumnDefinition(
            position=int(position) if position else default_position,
            name=str(name) if name else f'column_{default_position}',
            data_type=str(data_type).upper() if data_type else 'VARCHAR2',
            length=int(length) if length else 255,
            nullable=bool(nullable),
            description=str(description) if description else ''
        )
    
    def to_json(self, table_def: TableDefinition) -> Dict:
        """Convert TableDefinition to JSON-compatible dict"""
        
        return {
            "table_name": table_def.table_name,
            "description": table_def.description,
            "file_format": {
                "delimiter": table_def.delimiter,
                "encoding": table_def.encoding,
                "date_format": table_def.date_format
            },
            "columns": [
                {
                    "position": col.position,
                    "name": col.name,
                    "data_type": col.data_type,
                    "length": col.length,
                    "nullable": col.nullable,
                    "description": col.description
                }
                for col in table_def.columns
            ]
        }