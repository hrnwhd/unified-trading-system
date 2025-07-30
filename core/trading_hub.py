# ===== TRADING HUB - MAIN SYSTEM COORDINATOR =====
# Central coordinator that manages all system components
# Handles communication between data, trading, and monitoring systems

import json
import time
import threading
from datetime import datetime, timedelta
from pathlib import Path
import logging

class TradingHub:
    """Central coordinator for the unified trading system"""
    
    def __init__(self, config, status, loggers):
        self.config = config
        self.status = status
        self.logger = loggers.get('trading', logging.getLogger(__name__))
        
        # Data paths
        self.data_dir = Path("data")
        self.data_dir.mkdir(exist_ok=True)
        
        # Unified data file
        self.market_data_file = self.data_dir / "market_data.json"
        self.bot_state_file = self.data_dir / "bot_state.json"
        self.emergency_file = self.data_dir / "emergency.json"
        
        # Component communication
        self.component_status = {}
        self.data_lock = threading.Lock()
        self.shutdown_event = threading.Event()
        
        # Market data cache
        self.market_data = {
            "last_updated": None,
            "system_status": "initializing",
            "data_sources": {
                "economic_calendar": {"status": "waiting", "data": []},
                "sentiment": {"status": "waiting", "data": {}},
                "correlation": {"status": "waiting", "data": {}},
                "cot": {"status": "waiting", "data": {}}
            }
        }
        
        self.logger.info("🎯 Trading Hub initialized")
    
    def run(self):
        """Main hub coordination loop"""
        try:
            self.logger.info("🚀 Trading Hub starting...")
            
            # Initialize market data file
            self.initialize_market_data()
            
            # Main coordination loop
            while not self.shutdown_event.is_set():
                try:
                    # Coordinate data flow
                    self.coordinate_data_flow()
                    
                    # Monitor component health
                    self.monitor_components()
                    
                    # Update system status
                    self.update_system_status()
                    
                    # Sleep for next cycle
                    time.sleep(10)  # Check every 10 seconds
                    
                except Exception as e:
                    self.logger.error(f"❌ Error in hub coordination: {e}")
                    time.sleep(5)
            
        except Exception as e:
            self.logger.error(f"❌ Fatal error in Trading Hub: {e}")
        
        finally:
            self.logger.info("🛑 Trading Hub stopped")
    
    def initialize_market_data(self):
        """Initialize the unified market data structure"""
        try:
            # Create initial market data structure
            initial_data = {
                "metadata": {
                    "system_name": "Unified Trading System",
                    "version": self.config.get('system.version', '1.0'),
                    "created": datetime.now().isoformat(),
                    "account_number": self.config.get('mt5.account_number'),
                    "magic_number": self.config.get('mt5.magic_number')
                },
                "last_updated": datetime.now().isoformat(),
                "system_status": "running",
                "data_freshness": {
                    "economic_calendar": {"fresh": False, "last_update": None},
                    "sentiment": {"fresh": False, "last_update": None},
                    "correlation": {"fresh": False, "last_update": None},
                    "cot": {"fresh": False, "last_update": None}
                },
                "data_sources": {
                    "economic_calendar": {
                        "status": "waiting",
                        "next_update": None,
                        "events": [],
                        "error": None
                    },
                    "sentiment": {
                        "status": "waiting", 
                        "next_update": None,
                        "pairs": {},
                        "threshold": self.config.get('data_collection.sentiment_threshold', 60),
                        "error": None
                    },
                    "correlation": {
                        "status": "waiting",
                        "next_update": None,
                        "matrix": {},
                        "warnings": [],
                        "error": None
                    },
                    "cot": {
                        "status": "waiting",
                        "next_update": None,
                        "financial": {},
                        "commodity": {},
                        "error": None
                    }
                },
                "trading_signals": {
                    "generated": [],
                    "executed": [],
                    "last_analysis": None
                },
                "risk_status": {
                    "emergency_stop": False,
                    "warnings": [],
                    "last_check": None
                }
            }
            
            # Save to file
            with self.data_lock:
                with open(self.market_data_file, 'w') as f:
                    json.dump(initial_data, f, indent=2)
            
            self.market_data = initial_data
            self.logger.info("✅ Market data file initialized")
            
        except Exception as e:
            self.logger.error(f"❌ Error initializing market data: {e}")
    
    def coordinate_data_flow(self):
        """Coordinate data flow between components"""
        try:
            # Load current market data
            self.load_market_data()
            
            # Check data freshness
            self.check_data_freshness()
            
            # Coordinate updates if needed
            self.coordinate_updates()
            
        except Exception as e:
            self.logger.error(f"❌ Error coordinating data flow: {e}")
    
    def load_market_data(self):
        """Load current market data from file"""
        try:
            if self.market_data_file.exists():
                with open(self.market_data_file, 'r') as f:
                    self.market_data = json.load(f)
            
        except Exception as e:
            self.logger.error(f"❌ Error loading market data: {e}")
    
    def check_data_freshness(self):
        """Check if data sources are fresh or stale"""
        try:
            current_time = datetime.now()
            freshness_limits = {
                'sentiment': self.config.get('data_collection.sentiment_interval_minutes', 30),
                'correlation': self.config.get('data_collection.correlation_interval_minutes', 30),
                'economic_calendar': self.config.get('data_collection.economic_calendar_interval_minutes', 60),
                'cot': 24 * 60 * 7  # 1 week for COT data
            }
            
            for source, limit_minutes in freshness_limits.items():
                freshness_data = self.market_data.get('data_freshness', {}).get(source, {})
                last_update_str = freshness_data.get('last_update')
                
                if last_update_str:
                    last_update = datetime.fromisoformat(last_update_str)
                    age_minutes = (current_time - last_update).total_seconds() / 60
                    
                    is_fresh = age_minutes < limit_minutes
                    freshness_data['fresh'] = is_fresh
                    freshness_data['age_minutes'] = round(age_minutes, 1)
                else:
                    freshness_data['fresh'] = False
                    freshness_data['age_minutes'] = None
                
                # Update in market data
                if 'data_freshness' not in self.market_data:
                    self.market_data['data_freshness'] = {}
                self.market_data['data_freshness'][source] = freshness_data
            
        except Exception as e:
            self.logger.error(f"❌ Error checking data freshness: {e}")
    
    def coordinate_updates(self):
        """Coordinate data updates based on schedules and freshness"""
        try:
            # This will be implemented when data managers are integrated
            # For now, just log the coordination status
            
            stale_sources = []
            for source, freshness in self.market_data.get('data_freshness', {}).items():
                if not freshness.get('fresh', False):
                    stale_sources.append(source)
            
            if stale_sources:
                self.logger.debug(f"📊 Stale data sources: {', '.join(stale_sources)}")
            
        except Exception as e:
            self.logger.error(f"❌ Error coordinating updates: {e}")
    
    def monitor_components(self):
        """Monitor health of all system components"""
        try:
            # Check if critical files exist and are recent
            critical_files = {
                'market_data': self.market_data_file,
                'emergency': self.emergency_file
            }
            
            for file_type, file_path in critical_files.items():
                if file_path.exists():
                    # Check file age
                    file_age = datetime.now() - datetime.fromtimestamp(file_path.stat().st_mtime)
                    
                    if file_age.total_seconds() > 300:  # 5 minutes
                        self.logger.warning(f"⚠️ {file_type} file is {file_age} old")
                else:
                    self.logger.warning(f"⚠️ Missing {file_type} file: {file_path}")
            
        except Exception as e:
            self.logger.error(f"❌ Error monitoring components: {e}")
    
    def update_system_status(self):
        """Update overall system status"""
        try:
            # Update market data with current timestamp
            self.market_data['last_updated'] = datetime.now().isoformat()
            
            # Determine system status
            fresh_sources = sum(1 for source in self.market_data.get('data_freshness', {}).values() 
                              if source.get('fresh', False))
            total_sources = len(self.market_data.get('data_freshness', {}))
            
            if fresh_sources == total_sources and total_sources > 0:
                system_status = "optimal"
            elif fresh_sources > 0:
                system_status = "partial"
            else:
                system_status = "degraded"
            
            self.market_data['system_status'] = system_status
            
            # Save updated market data
            with self.data_lock:
                with open(self.market_data_file, 'w') as f:
                    json.dump(self.market_data, f, indent=2)
            
        except Exception as e:
            self.logger.error(f"❌ Error updating system status: {e}")
    
    def update_data_source(self, source_name, data, status="fresh"):
        """Update data for a specific source"""
        try:
            with self.data_lock:
                current_time = datetime.now().isoformat()
                
                # Update the data source
                if source_name in self.market_data['data_sources']:
                    self.market_data['data_sources'][source_name].update({
                        'status': status,
                        'last_update': current_time,
                        'error': None
                    })
                    
                    # Update the actual data based on source type
                    if source_name == 'economic_calendar':
                        self.market_data['data_sources'][source_name]['events'] = data
                    elif source_name == 'sentiment':
                        self.market_data['data_sources'][source_name]['pairs'] = data
                    elif source_name == 'correlation':
                        self.market_data['data_sources'][source_name]['matrix'] = data.get('matrix', {})
                        self.market_data['data_sources'][source_name]['warnings'] = data.get('warnings', [])
                    elif source_name == 'cot':
                        self.market_data['data_sources'][source_name]['financial'] = data.get('financial', {})
                        self.market_data['data_sources'][source_name]['commodity'] = data.get('commodity', {})
                
                # Update freshness
                if 'data_freshness' not in self.market_data:
                    self.market_data['data_freshness'] = {}
                
                self.market_data['data_freshness'][source_name] = {
                    'fresh': True,
                    'last_update': current_time,
                    'age_minutes': 0
                }
                
                # Update overall timestamp
                self.market_data['last_updated'] = current_time
                
                # Save to file
                with open(self.market_data_file, 'w') as f:
                    json.dump(self.market_data, f, indent=2)
                
                self.logger.info(f"✅ Updated {source_name} data")
                
        except Exception as e:
            self.logger.error(f"❌ Error updating {source_name} data: {e}")
    
    def update_data_source_error(self, source_name, error_message):
        """Update data source with error status"""
        try:
            with self.data_lock:
                current_time = datetime.now().isoformat()
                
                if source_name in self.market_data['data_sources']:
                    self.market_data['data_sources'][source_name].update({
                        'status': 'error',
                        'last_update': current_time,
                        'error': error_message
                    })
                
                # Save to file
                with open(self.market_data_file, 'w') as f:
                    json.dump(self.market_data, f, indent=2)
                
                self.logger.error(f"❌ {source_name} error: {error_message}")
                
        except Exception as e:
            self.logger.error(f"❌ Error updating {source_name} error: {e}")
    
    def get_market_data(self):
        """Get current market data (thread-safe)"""
        try:
            with self.data_lock:
                return self.market_data.copy()
        except Exception as e:
            self.logger.error(f"❌ Error getting market data: {e}")
            return {}
    
    def get_trading_signals(self):
        """Get current trading signals"""
        try:
            market_data = self.get_market_data()
            return market_data.get('trading_signals', {})
        except Exception as e:
            self.logger.error(f"❌ Error getting trading signals: {e}")
            return {}
    
    def update_trading_signals(self, signals):
        """Update trading signals"""
        try:
            with self.data_lock:
                self.market_data['trading_signals']['generated'] = signals
                self.market_data['trading_signals']['last_analysis'] = datetime.now().isoformat()
                
                # Save to file
                with open(self.market_data_file, 'w') as f:
                    json.dump(self.market_data, f, indent=2)
                
                self.logger.info(f"✅ Updated trading signals: {len(signals)} signals")
                
        except Exception as e:
            self.logger.error(f"❌ Error updating trading signals: {e}")
    
    def update_risk_status(self, emergency_stop=False, warnings=None):
        """Update risk monitoring status"""
        try:
            if warnings is None:
                warnings = []
                
            with self.data_lock:
                self.market_data['risk_status'] = {
                    'emergency_stop': emergency_stop,
                    'warnings': warnings,
                    'last_check': datetime.now().isoformat()
                }
                
                # Save to file
                with open(self.market_data_file, 'w') as f:
                    json.dump(self.market_data, f, indent=2)
                
                if emergency_stop:
                    self.logger.critical("🚨 EMERGENCY STOP ACTIVATED")
                elif warnings:
                    self.logger.warning(f"⚠️ Risk warnings: {len(warnings)}")
                
        except Exception as e:
            self.logger.error(f"❌ Error updating risk status: {e}")
    
    def get_system_summary(self):
        """Get comprehensive system summary"""
        try:
            market_data = self.get_market_data()
            
            # Count fresh data sources
            freshness = market_data.get('data_freshness', {})
            fresh_count = sum(1 for f in freshness.values() if f.get('fresh', False))
            total_count = len(freshness)
            
            # Get component status from main status manager
            component_status = self.status.get_status_summary()
            
            summary = {
                'system_status': market_data.get('system_status', 'unknown'),
                'uptime': component_status.get('uptime_formatted', 'unknown'),
                'data_freshness': f"{fresh_count}/{total_count} sources fresh",
                'emergency_stop': market_data.get('risk_status', {}).get('emergency_stop', False),
                'warnings_count': len(market_data.get('risk_status', {}).get('warnings', [])),
                'last_updated': market_data.get('last_updated'),
                'components': component_status.get('components', {}),
                'trading_enabled': self.config.get('trading.enabled', False),
                'pairs_monitored': len(self.config.pairs.get('monitored_pairs', []))
            }
            
            return summary
            
        except Exception as e:
            self.logger.error(f"❌ Error getting system summary: {e}")
            return {'error': str(e)}
    
    def request_shutdown(self):
        """Request graceful shutdown"""
        self.logger.info("🛑 Shutdown requested for Trading Hub")
        self.shutdown_event.set()
    
    def emergency_shutdown(self, reason="Unknown"):
        """Emergency shutdown of entire system"""
        try:
            self.logger.critical(f"🚨 EMERGENCY SHUTDOWN: {reason}")
            
            # Update emergency status
            emergency_data = {
                'timestamp': datetime.now().isoformat(),
                'emergency_active': True,
                'reason': reason,
                'initiated_by': 'trading_hub'
            }
            
            with open(self.emergency_file, 'w') as f:
                json.dump(emergency_data, f, indent=2)
            
            # Signal shutdown
            self.shutdown_event.set()
            
        except Exception as e:
            self.logger.error(f"❌ Error during emergency shutdown: {e}")
    
    def create_status_report(self):
        """Create detailed status report for external monitoring"""
        try:
            market_data = self.get_market_data()
            system_summary = self.get_system_summary()
            
            report = {
                'report_timestamp': datetime.now().isoformat(),
                'system_summary': system_summary,
                'data_sources_detail': market_data.get('data_sources', {}),
                'data_freshness_detail': market_data.get('data_freshness', {}),
                'risk_status_detail': market_data.get('risk_status', {}),
                'configuration': {
                    'trading_enabled': self.config.get('trading.enabled'),
                    'martingale_enabled': self.config.get('martingale.enabled'),
                    'telegram_enabled': self.config.get('telegram.enabled'),
                    'monitored_pairs': self.config.pairs.get('monitored_pairs', [])
                }
            }
            
            return report
            
        except Exception as e:
            self.logger.error(f"❌ Error creating status report: {e}")
            return {'error': str(e)}