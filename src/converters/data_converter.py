"""
Data type conversion and transformation
"""

import logging
import pandas as pd
from typing import Dict, Any, List
import uuid
from datetime import datetime


class DataConverter:
    """Converts data between SQL Server and PostgreSQL formats"""
    
    def __init__(self, data_mapping: Dict[str, Any]):
        self.mapping = data_mapping
        self.logger = logging.getLogger(__name__)
    
    def convert_batch(self, data_batch: pd.DataFrame, table_name: str) -> pd.DataFrame:
        """Convert a batch of data"""
        converted_batch = data_batch.copy()
        
        # Apply column-specific conversions
        for column in converted_batch.columns:
            converted_batch[column] = self._convert_column_data(
                converted_batch[column], 
                table_name, 
                column
            )
        
        return converted_batch
    
    def _convert_column_data(self, series: pd.Series, table_name: str, column_name: str) -> pd.Series:
        """Convert data for a specific column"""
        # Get column metadata (would be passed from schema info)
        # For now, we'll infer from the data
        
        # Handle common conversions
        if series.dtype == 'object':
            # String data - handle encoding issues
            series = series.astype(str)
        
        elif 'datetime' in str(series.dtype):
            # Ensure proper timestamp format
            series = pd.to_datetime(series)
        
        elif series.dtype == 'bool':
            # Convert bit to boolean
            series = series.astype(bool)
        
        return series