"""
Schema conversion from SQL Server to PostgreSQL
"""

import logging
from typing import Dict, Any, List
import re


class SchemaConverter:
    """Converts SQL Server schema to PostgreSQL"""
    
    def __init__(self, conversion_rules: Dict[str, Any]):
        self.rules = conversion_rules
        self.logger = logging.getLogger(__name__)
        self.data_type_mapping = self._load_data_type_mapping()
    
    def _load_data_type_mapping(self) -> Dict[str, str]:
        """Load data type mapping rules"""
        return {
            'int': 'INTEGER',
            'bigint': 'BIGINT',
            'smallint': 'SMALLINT',
            'tinyint': 'SMALLINT',
            'bit': 'BOOLEAN',
            'decimal': 'DECIMAL',
            'numeric': 'NUMERIC',
            'money': 'DECIMAL(15,2)',
            'smallmoney': 'DECIMAL(10,4)',
            'float': 'DOUBLE PRECISION',
            'real': 'REAL',
            'datetime': 'TIMESTAMP',
            'datetime2': 'TIMESTAMP',
            'smalldatetime': 'TIMESTAMP',
            'date': 'DATE',
            'time': 'TIME',
            'datetimeoffset': 'TIMESTAMP WITH TIME ZONE',
            'char': 'CHAR',
            'varchar': 'VARCHAR',
            'nchar': 'CHAR',
            'nvarchar': 'VARCHAR',
            'text': 'TEXT',
            'ntext': 'TEXT',
            'binary': 'BYTEA',
            'varbinary': 'BYTEA',
            'image': 'BYTEA',
            'uniqueidentifier': 'UUID',
            'xml': 'XML'
        }
    
    def convert(self, metadata: Dict[str, Any]) -> Dict[str, Any]:
        """Convert complete schema metadata"""
        converted = {
            'tables': self._convert_tables(metadata['tables']),
            'views': self._convert_views(metadata['views']),
            'indexes': self._convert_indexes(metadata['indexes']),
            'constraints': self._convert_constraints(metadata['constraints'])
        }
        
        return converted
    
    def _convert_tables(self, tables: Dict[str, Any]) -> Dict[str, Any]:
        """Convert table definitions"""
        converted_tables = {}
        
        for table_key, table_info in tables.items():
            converted_table = {
                'schema': table_info['schema'],
                'name': table_info['name'],
                'columns': []
            }
            
            for column in table_info['columns']:
                converted_column = self._convert_column(column)
                converted_table['columns'].append(converted_column)
            
            converted_tables[table_key] = converted_table
            
        return converted_tables
    
    def _convert_column(self, column: Dict[str, Any]) -> Dict[str, Any]:
        """Convert individual column definition"""
        sql_server_type = column['data_type'].lower()
        
        # Map data type
        if sql_server_type in self.data_type_mapping:
            pg_type = self.data_type_mapping[sql_server_type]
        else:
            pg_type = 'TEXT'  # Default fallback
            self.logger.warning(f"Unknown data type: {sql_server_type}, using TEXT")
        
        # Handle length specifications
        if column['max_length'] and sql_server_type in ['varchar', 'nvarchar', 'char', 'nchar']:
            if column['max_length'] == -1:  # MAX length
                pg_type = 'TEXT'
            else:
                pg_type = f"{pg_type}({column['max_length']})"
        
        # Handle precision and scale for numeric types
        if column['precision'] and sql_server_type in ['decimal', 'numeric']:
            if column['scale']:
                pg_type = f"{pg_type}({column['precision']},{column['scale']})"
            else:
                pg_type = f"{pg_type}({column['precision']})"
        
        # Convert default values
        default_value = self._convert_default_value(column['default_value'])
        
        return {
            'name': column['name'],
            'data_type': pg_type,
            'is_nullable': column['is_nullable'],
            'default_value': default_value,
            'ordinal_position': column['ordinal_position']
        }
    
    def _convert_default_value(self, default_value: str) -> str:
        """Convert SQL Server default values to PostgreSQL"""
        if not default_value:
            return None
        
        # Remove parentheses
        default_value = default_value.strip('()')
        
        # Common conversions
        conversions = {
            'getdate()': 'CURRENT_TIMESTAMP',
            'getutcdate()': 'CURRENT_TIMESTAMP',
            'newid()': 'gen_random_uuid()',
            'user_name()': 'CURRENT_USER',
            'system_user': 'CURRENT_USER'
        }
        
        for sql_func, pg_func in conversions.items():
            if default_value.lower() == sql_func:
                return pg_func
        
        return default_value
    
    def _convert_views(self, views: Dict[str, Any]) -> Dict[str, Any]:
        """Convert view definitions"""
        converted_views = {}
        
        for view_key, view_info in views.items():
            # Basic T-SQL to PostgreSQL conversion
            pg_definition = self._convert_tsql_to_pgsql(view_info['definition'])
            
            converted_views[view_key] = {
                'schema': view_info['schema'],
                'name': view_info['name'],
                'definition': pg_definition
            }
        
        return converted_views
    
    def _convert_tsql_to_pgsql(self, tsql: str) -> str:
        """Convert T-SQL syntax to PostgreSQL"""
        # Basic conversions (this would be much more comprehensive in production)
        conversions = [
            (r'\[(\w+)\]', r'"\1"'),  # Bracket identifiers to quotes
            (r'ISNULL\(([^,]+),([^)]+)\)', r'COALESCE(\1,\2)'),  # ISNULL to COALESCE
            (r'LEN\(', r'LENGTH('),  # LEN to LENGTH
            (r'DATEPART\(', r'EXTRACT('),  # DATEPART to EXTRACT
            (r'GETDATE\(\)', r'CURRENT_TIMESTAMP'),  # GETDATE to CURRENT_TIMESTAMP
        ]
        
        result = tsql
        for pattern, replacement in conversions:
            result = re.sub(pattern, replacement, result, flags=re.IGNORECASE)
        
        return result
    
    def _convert_indexes(self, indexes: Dict[str, Any]) -> Dict[str, Any]:
        """Convert index definitions"""
        converted_indexes = {}
        
        for index_key, index_info in indexes.items():
            # Skip primary key indexes (handled as constraints)
            if index_info['is_primary_key']:
                continue
            
            converted_indexes[index_key] = {
                'name': index_info['name'],
                'table': index_info['table'],
                'is_unique': index_info['is_unique'],
                'columns': index_info['columns']
            }
        
        return converted_indexes
    
    def _convert_constraints(self, constraints: Dict[str, Any]) -> Dict[str, Any]:
        """Convert constraint definitions"""
        # Implementation for constraint conversion
        return {}