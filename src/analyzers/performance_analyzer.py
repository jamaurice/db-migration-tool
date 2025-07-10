"""
Performance analysis and optimization recommendations
"""

import logging
from typing import Dict, Any, List


class PerformanceAnalyzer:
    """Analyzes performance implications and generates recommendations"""
    
    def __init__(self):
        self.logger = logging.getLogger(__name__)
    
    def analyze_schema(self, metadata: Dict[str, Any]) -> Dict[str, Any]:
        """Analyze schema for performance implications"""
        recommendations = {
            'indexing': self._analyze_indexing(metadata),
            'partitioning': self._analyze_partitioning(metadata),
            'data_types': self._analyze_data_types(metadata),
            'constraints': self._analyze_constraints(metadata)
        }
        
        return recommendations
    
    def _analyze_indexing(self, metadata: Dict[str, Any]) -> List[Dict[str, Any]]:
        """Analyze indexing strategy"""
        recommendations = []
        
        for table_key, table_info in metadata['tables'].items():
            # Check for missing primary keys
            has_pk = any(col.get('is_primary_key', False) for col in table_info.get('columns', []))
            if not has_pk:
                recommendations.append({
                    'type': 'missing_primary_key',
                    'table': table_key,
                    'severity': 'high',
                    'description': f'Table {table_key} lacks a primary key',
                    'recommendation': 'Add a primary key or unique identifier column'
                })
        
        return recommendations
    
    def _analyze_partitioning(self, metadata: Dict[str, Any]) -> List[Dict[str, Any]]:
        """Analyze partitioning opportunities"""
        recommendations = []
        
        # Look for large tables that could benefit from partitioning
        for table_key, table_info in metadata['tables'].items():
            # Check for date columns that could be used for partitioning
            date_columns = [
                col for col in table_info.get('columns', [])
                if col['data_type'].lower() in ['datetime', 'datetime2', 'date']
            ]
            
            if date_columns:
                recommendations.append({
                    'type': 'partitioning_opportunity',
                    'table': table_key,
                    'severity': 'medium',
                    'description': f'Table {table_key} has date columns suitable for partitioning',
                    'recommendation': f'Consider partitioning by {date_columns[0]["name"]}'
                })
        
        return recommendations
    
    def _analyze_data_types(self, metadata: Dict[str, Any]) -> List[Dict[str, Any]]:
        """Analyze data type efficiency"""
        recommendations = []
        
        for table_key, table_info in metadata['tables'].items():
            for column in table_info.get('columns', []):
                # Check for oversized varchar columns
                if (column['data_type'].lower() in ['varchar', 'nvarchar'] and 
                    column.get('max_length', 0) > 1000):
                    recommendations.append({
                        'type': 'oversized_varchar',
                        'table': table_key,
                        'column': column['name'],
                        'severity': 'low',
                        'description': f'Column {column["name"]} has large varchar size',
                        'recommendation': 'Consider using TEXT type or reducing size'
                    })
        
        return recommendations
    
    def _analyze_constraints(self, metadata: Dict[str, Any]) -> List[Dict[str, Any]]:
        """Analyze constraint usage"""
        recommendations = []
        
        # This would analyze foreign key relationships, check constraints, etc.
        return recommendations
    
    def generate_recommendations(self, converted_schema: Dict[str, Any]) -> Dict[str, Any]:
        """Generate post-migration performance recommendations"""
        return {
            'vacuum_analyze': 'Run VACUUM ANALYZE on all tables after migration',
            'statistics': 'Update table statistics for optimal query planning',
            'configuration': self._get_config_recommendations()
        }
    
    def _get_config_recommendations(self) -> List[str]:
        """Get PostgreSQL configuration recommendations"""
        return [
            'Consider increasing shared_buffers to 25% of RAM',
            'Set effective_cache_size to 50-75% of RAM',
            'Tune checkpoint_segments for write-heavy workloads',
            'Consider enabling synchronous_commit = off for better performance (with trade-offs)'
        ]
