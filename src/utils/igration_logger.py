"""
Migration-specific logging and audit trail
"""

import json
import logging
from datetime import datetime
from pathlib import Path
from typing import Dict, Any


class MigrationLogger:
    """Logs migration progress and maintains audit trail"""
    
    def __init__(self, config: Dict[str, Any]):
        self.config = config
        self.logger = logging.getLogger(__name__)
        self.audit_file = Path('logs') / 'migration_audit.json'
        self.audit_data = []
    
    def log_phase(self, phase: str, status: str, data: Any = None):
        """Log migration phase completion"""
        entry = {
            'timestamp': datetime.now().isoformat(),
            'phase': phase,
            'status': status,
            'data_summary': self._summarize_data(data) if data else None
        }
        
        self.audit_data.append(entry)
        self._write_audit_log()
        
        self.logger.info(f"Phase {phase}: {status}")
    
    def log_error(self, phase: str, error: str):
        """Log migration error"""
        entry = {
            'timestamp': datetime.now().isoformat(),
            'phase': phase,
            'status': 'error',
            'error': error
        }
        
        self.audit_data.append(entry)
        self._write_audit_log()
        
        self.logger.error(f"Phase {phase} failed: {error}")
    
    def log_recommendations(self, recommendations: Dict[str, Any]):
        """Log performance recommendations"""
        entry = {
            'timestamp': datetime.now().isoformat(),
            'type': 'recommendations',
            'data': recommendations
        }
        
        self.audit_data.append(entry)
        self._write_audit_log()
    
    def _summarize_data(self, data: Any) -> Dict[str, Any]:
        """Create summary of data for logging"""
        if isinstance(data, dict):
            if 'tables' in data:
                return {
                    'table_count': len(data['tables']),
                    'view_count': len(data.get('views', {})),
                    'index_count': len(data.get('indexes', {}))
                }
        
        return {'type': type(data).__name__}
    
    def _write_audit_log(self):
        """Write audit log to file"""
        with open(self.audit_file, 'w') as f:
            json.dump(self.audit_data, f, indent=2)