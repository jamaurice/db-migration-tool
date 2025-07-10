"""
SQL Server metadata and data extraction
"""
import pyodbc
import logging
from typing import Dict, Any, Generator, List
import pandas as pd


class SqlServerExtractor:
    """Extracts metadata and data from SQL Server"""
    
    def __init__(self, connection_config: Dict[str, Any]):
        self.config = connection_config
        self.logger = logging.getLogger(__name__)
        self.connection = None
    
    def connect(self):
        """Establish connection to SQL Server"""
        if self.connection is None:
            connection_string = (
                f"DRIVER={{ODBC Driver 17 for SQL Server}};"
                f"SERVER={self.config['server']};"
                f"DATABASE={self.config['database']};"
                f"UID={self.config['username']};"
                f"PWD={self.config['password']}"
            )
            self.connection = pyodbc.connect(connection_string)
        return self.connection
    
    def extract_metadata(self) -> Dict[str, Any]:
        """Extract complete database metadata"""
        conn = self.connect()
        cursor = conn.cursor()
        
        metadata = {
            'tables': self._extract_tables(cursor),
            'views': self._extract_views(cursor),
            'indexes': self._extract_indexes(cursor),
            'constraints': self._extract_constraints(cursor),
            'triggers': self._extract_triggers(cursor),
            'procedures': self._extract_procedures(cursor),
            'functions': self._extract_functions(cursor)
        }
        
        return metadata
    
    def _extract_tables(self, cursor) -> Dict[str, Any]:
        """Extract table definitions"""
        query = """
        SELECT 
            t.TABLE_SCHEMA,
            t.TABLE_NAME,
            c.COLUMN_NAME,
            c.DATA_TYPE,
            c.CHARACTER_MAXIMUM_LENGTH,
            c.NUMERIC_PRECISION,
            c.NUMERIC_SCALE,
            c.IS_NULLABLE,
            c.COLUMN_DEFAULT,
            c.ORDINAL_POSITION
        FROM INFORMATION_SCHEMA.TABLES t
        INNER JOIN INFORMATION_SCHEMA.COLUMNS c 
            ON t.TABLE_NAME = c.TABLE_NAME 
            AND t.TABLE_SCHEMA = c.TABLE_SCHEMA
        WHERE t.TABLE_TYPE = 'BASE TABLE'
        ORDER BY t.TABLE_SCHEMA, t.TABLE_NAME, c.ORDINAL_POSITION
        """
        
        cursor.execute(query)
        rows = cursor.fetchall()
        
        tables = {}
        for row in rows:
            schema_table = f"{row.TABLE_SCHEMA}.{row.TABLE_NAME}"
            if schema_table not in tables:
                tables[schema_table] = {
                    'schema': row.TABLE_SCHEMA,
                    'name': row.TABLE_NAME,
                    'columns': []
                }
            
            column_info = {
                'name': row.COLUMN_NAME,
                'data_type': row.DATA_TYPE,
                'max_length': row.CHARACTER_MAXIMUM_LENGTH,
                'precision': row.NUMERIC_PRECISION,
                'scale': row.NUMERIC_SCALE,
                'is_nullable': row.IS_NULLABLE == 'YES',
                'default_value': row.COLUMN_DEFAULT,
                'ordinal_position': row.ORDINAL_POSITION
            }
            tables[schema_table]['columns'].append(column_info)
        
        return tables
    
    def _extract_views(self, cursor) -> Dict[str, Any]:
        """Extract view definitions"""
        query = """
        SELECT 
            TABLE_SCHEMA,
            TABLE_NAME,
            VIEW_DEFINITION
        FROM INFORMATION_SCHEMA.VIEWS
        """
        
        cursor.execute(query)
        rows = cursor.fetchall()
        
        views = {}
        for row in rows:
            schema_view = f"{row.TABLE_SCHEMA}.{row.TABLE_NAME}"
            views[schema_view] = {
                'schema': row.TABLE_SCHEMA,
                'name': row.TABLE_NAME,
                'definition': row.VIEW_DEFINITION
            }
        
        return views
    
    def _extract_indexes(self, cursor) -> Dict[str, Any]:
        """Extract index definitions"""
        query = """
        SELECT 
            i.name AS index_name,
            t.name AS table_name,
            i.type_desc,
            i.is_unique,
            i.is_primary_key,
            c.name AS column_name,
            ic.key_ordinal,
            ic.is_descending_key
        FROM sys.indexes i
        INNER JOIN sys.tables t ON i.object_id = t.object_id
        INNER JOIN sys.index_columns ic ON i.object_id = ic.object_id AND i.index_id = ic.index_id
        INNER JOIN sys.columns c ON ic.object_id = c.object_id AND ic.column_id = c.column_id
        WHERE i.type > 0
        ORDER BY t.name, i.name, ic.key_ordinal
        """
        
        cursor.execute(query)
        rows = cursor.fetchall()
        
        indexes = {}
        for row in rows:
            index_key = f"{row.table_name}.{row.index_name}"
            if index_key not in indexes:
                indexes[index_key] = {
                    'name': row.index_name,
                    'table': row.table_name,
                    'type': row.type_desc,
                    'is_unique': row.is_unique,
                    'is_primary_key': row.is_primary_key,
                    'columns': []
                }
            
            indexes[index_key]['columns'].append({
                'name': row.column_name,
                'ordinal': row.key_ordinal,
                'descending': row.is_descending_key
            })
        
        return indexes
    
    def _extract_constraints(self, cursor) -> Dict[str, Any]:
        """Extract constraint definitions"""
        # Implementation for extracting constraints (FK, UK, CK)
        return {}
    
    def _extract_triggers(self, cursor) -> Dict[str, Any]:
        """Extract trigger definitions"""
        # Implementation for extracting triggers
        return {}
    
    def _extract_procedures(self, cursor) -> Dict[str, Any]:
        """Extract stored procedure definitions"""
        # Implementation for extracting stored procedures
        return {}
    
    def _extract_functions(self, cursor) -> Dict[str, Any]:
        """Extract function definitions"""
        # Implementation for extracting functions
        return {}
    
    def extract_data(self, table_name: str, batch_size: int = 10000) -> Generator[pd.DataFrame, None, None]:
        """Extract data from a table in batches"""
        conn = self.connect()
        
        # Get total row count
        count_query = f"SELECT COUNT(*) FROM {table_name}"
        total_rows = pd.read_sql_query(count_query, conn).iloc[0, 0]
        
        # Extract data in batches
        offset = 0
        while offset < total_rows:
            query = f"""
            SELECT * FROM {table_name}
            ORDER BY (SELECT NULL)
            OFFSET {offset} ROWS
            FETCH NEXT {batch_size} ROWS ONLY
            """
            
            batch_df = pd.read_sql_query(query, conn)
            yield batch_df
            
            offset += batch_size
            self.logger.debug(f"Extracted {min(offset, total_rows)}/{total_rows} rows from {table_name}")
