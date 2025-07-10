"""
PostgreSQL data and schema loading
"""

import psycopg2
import logging
from typing import Dict, Any, List
import pandas as pd
from sqlalchemy import create_engine


class PostgreSQLLoader:
    """Loads schema and data into PostgreSQL"""
    
    def __init__(self, connection_config: Dict[str, Any]):
        self.config = connection_config
        self.logger = logging.getLogger(__name__)
        self.connection = None
        self.engine = None
    
    def connect(self):
        """Establish connection to PostgreSQL"""
        if self.connection is None:
            connection_string = (
                f"host={self.config['host']} "
                f"port={self.config.get('port', 5432)} "
                f"dbname={self.config['database']} "
                f"user={self.config['username']} "
                f"password={self.config['password']}"
            )
            self.connection = psycopg2.connect(connection_string)
            
            # Also create SQLAlchemy engine for pandas
            engine_string = (
                f"postgresql://{self.config['username']}:{self.config['password']}"
                f"@{self.config['host']}:{self.config.get('port', 5432)}"
                f"/{self.config['database']}"
            )
            self.engine = create_engine(engine_string)
        
        return self.connection
    
    def create_schema(self, schema_metadata: Dict[str, Any]):
        """Create tables and schemas in PostgreSQL"""
        conn = self.connect()
        cursor = conn.cursor()
        
        try:
            # Create tables
            for table_key, table_info in schema_metadata['tables'].items():
                ddl = self._generate_table_ddl(table_info)
                self.logger.info(f"Creating table: {table_key}")
                cursor.execute(ddl)
            
            # Create views
            for view_key, view_info in schema_metadata['views'].items():
                ddl = self._generate_view_ddl(view_info)
                self.logger.info(f"Creating view: {view_key}")
                cursor.execute(ddl)
            
            conn.commit()
            
        except Exception as e:
            conn.rollback()
            raise e
    
    def _generate_table_ddl(self, table_info: Dict[str, Any]) -> str:
        """Generate CREATE TABLE DDL"""
        schema = table_info['schema']
        table_name = table_info['name']
        columns = table_info['columns']
        
        # Create schema if not exists
        ddl_parts = [f"CREATE SCHEMA IF NOT EXISTS {schema};"]
        
        # Create table
        column_definitions = []
        for column in columns:
            col_def = f'"{column["name"]}" {column["data_type"]}'
            
            if not column['is_nullable']:
                col_def += ' NOT NULL'
            
            if column['default_value']:
                col_def += f' DEFAULT {column["default_value"]}'
            
            column_definitions.append(col_def)
        
        table_ddl = f"""
        CREATE TABLE {schema}.{table_name} (
            {','.join(column_definitions)}
        );
        """
        
        ddl_parts.append(table_ddl)
        return '\n'.join(ddl_parts)
    
    def _generate_view_ddl(self, view_info: Dict[str, Any]) -> str:
        """Generate CREATE VIEW DDL"""
        schema = view_info['schema']
        view_name = view_info['name']
        definition = view_info['definition']
        
        return f"CREATE VIEW {schema}.{view_name} AS {definition};"
    
    def create_indexes_constraints(self, schema_metadata: Dict[str, Any]):
        """Create indexes and constraints"""
        conn = self.connect()
        cursor = conn.cursor()
        
        try:
            # Create indexes
            for index_key, index_info in schema_metadata['indexes'].items():
                ddl = self._generate_index_ddl(index_info)
                self.logger.info(f"Creating index: {index_key}")
                cursor.execute(ddl)
            
            conn.commit()
            
        except Exception as e:
            conn.rollback()
            raise e
    
    def _generate_index_ddl(self, index_info: Dict[str, Any]) -> str:
        """Generate CREATE INDEX DDL"""
        unique_clause = "UNIQUE " if index_info['is_unique'] else ""
        column_list = ', '.join([col['name'] for col in index_info['columns']])
        
        return f"""
        CREATE {unique_clause}INDEX {index_info['name']} 
        ON {index_info['table']} ({column_list});
        """
    
    def load_data(self, table_name: str, data: pd.DataFrame):
        """Load data into PostgreSQL table"""
        if self.engine is None:
            self.connect()
        
        # Use pandas to_sql for efficient bulk loading
        data.to_sql(
            table_name, 
            self.engine, 
            if_exists='append', 
            index=False, 
            method='multi'
        )