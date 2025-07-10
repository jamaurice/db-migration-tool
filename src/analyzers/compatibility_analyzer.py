"""
Compatibility analysis between SQL Server and PostgreSQL
"""

import logging
from typing import Dict, Any, List
import re


class CompatibilityAnalyzer:
    """Analyzes compatibility issues and migration challenges"""
    
    def __init__(self):
        self.logger = logging.getLogger(__name__)
        self.incompatible_features = self._load_incompatible_features()
    
    def _load_incompatible_features(self) -> Dict[str, str]:
        """Load known incompatible features"""
        return {
            'IDENTITY': 'Use SERIAL or GENERATED columns instead',
            'ROWVERSION': 'No direct equivalent, consider using timestamps',
            'CURSOR': 'Limited cursor support, consider alternatives',
            'TRY...CATCH': 'Use EXCEPTION blocks instead',
            'MERGE': 'Use INSERT...ON CONFLICT or separate statements',
            'PIVOT': 'Use crosstab() function or manual pivoting',
            'UNPIVOT': 'Use custom functions or restructure data'
        }
    
    def analyze(self, metadata: Dict[str, Any]) -> Dict[str, Any]:
        """Perform compatibility analysis"""
        issues = {
            'data_types': self._analyze_data_types(metadata),
            'features': self._analyze_features(metadata),
            'syntax': self._analyze_syntax(metadata),
            'functions': self._analyze_functions(metadata)
        }
        
        return issues
    
    def _analyze_data_types(self, metadata: Dict[str, Any]) -> List[Dict[str, Any]]:
        """Analyze data type compatibility"""
        issues = []
        
        for table_key, table_info in metadata['tables'].items():
            for column in table_info.get('columns', []):
                data_type = column['data_type'].lower()
                
                # Check for problematic data types
                if data_type == 'uniqueidentifier':
                    issues.append({
                        'type': 'data_type_conversion',
                        'severity': 'medium',
                        'table': table_key,
                        'column': column['name'],
                        'issue': 'UNIQUEIDENTIFIER will be converted to UUID',
                        'action_required': 'Verify UUID extension is enabled'
                    })
                
                elif data_type in ['text', 'ntext']:
                    issues.append({
                        'type': 'deprecated_type',
                        'severity': 'low',
                        'table': table_key,
                        'column': column['name'],
                        'issue': f'{data_type.upper()} is deprecated in SQL Server',
                        'action_required': 'Will be converted to TEXT'
                    })
        
        return issues
    
    def _analyze_features(self, metadata: Dict[str, Any]) -> List[Dict[str, Any]]:
        """Analyze feature compatibility"""
        issues = []
        
        # Check stored procedures and functions
        for proc_key, proc_info in metadata.get('procedures', {}).items():
            definition = proc_info.get('definition', '')
            
            for feature, solution in self.incompatible_features.items():
                if feature.lower() in definition.lower():
                    issues.append({
                        'type': 'incompatible_feature',
                        'severity': 'high',
                        'object': proc_key,
                        'feature': feature,
                        'issue': f'Uses incompatible feature: {feature}',
                        'solution': solution
                    })
        
        return issues
    
    def _analyze_syntax(self, metadata: Dict[str, Any]) -> List[Dict[str, Any]]:
        """Analyze syntax compatibility"""
        issues = []
        
        # Check view definitions for syntax issues
        for view_key, view_info in metadata.get('views', {}).items():
            definition = view_info.get('definition', '')
            
            # Check for bracket identifiers
            if '[' in definition and ']' in definition:
                issues.append({
                    'type': 'syntax_difference',
                    'severity': 'low',
                    'object': view_key,
                    'issue': 'Uses bracket identifiers',
                    'solution': 'Will be converted to quoted identifiers'
                })
            
            # Check for SQL Server specific functions
            sql_server_functions = ['ISNULL', 'LEN', 'DATEPART', 'DATEDIFF']
            for func in sql_server_functions:
                if re.search(f'\\b{func}\\b', definition, re.IGNORECASE):
                    issues.append({
                        'type': 'function_conversion',
                        'severity': 'medium',
                        'object': view_key,
                        'function': func,
                        'issue': f'Uses SQL Server specific function: {func}',
                        'solution': 'Will be converted to PostgreSQL equivalent'
                    })
        
        return issues
    
    def _analyze_functions(self, metadata: Dict[str, Any]) -> List[Dict[str, Any]]:
        """Analyze function compatibility"""
        issues = []
        
        # This would analyze user-defined functions for compatibility
        return issues