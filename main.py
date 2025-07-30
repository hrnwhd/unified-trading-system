#!/usr/bin/env python3
# ===== UNIFIED TRADING SYSTEM - MAIN ENTRY POINT =====
# Single command to start entire trading ecosystem
# Version: 1.0 - Core Foundation

import os
import sys
import json
import time
import threading
import logging
from datetime import datetime
from pathlib import Path

# Add core modules to path
sys.path.append(os.path.join(os.path.dirname(__file__), 'core'))
sys.path.append(os.path.join(os.path.dirname(__file__), 'scrapers'))
sys.path.append(os.path.join(os.path.dirname(__file__), 'interfaces'))

# ===== SETUP LOGGING =====
def setup_logging():
    """Setup centralized logging system"""
    # Create logs directory
    log_dir = Path("logs")
    log_dir.mkdir(exist_ok=True)
    
    # Configure main logger
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
        handlers=[
            logging.FileHandler(log_dir / 'system.log', encoding='utf-8'),
            logging.StreamHandler(sys.stdout)
        ]
    )
    
    # Create specialized loggers
    loggers = {
        'trading': logging.getLogger('trading'),
        'data': logging.getLogger('data'),
        'telegram': logging.getLogger('telegram'),
        'risk': logging.getLogger('risk')
    }
    
    # Add file handlers for specialized logs
    for name, logger in loggers.items():
        file_handler = logging.FileHandler(log_dir / f'{name}.log', encoding='utf-8')
        file_handler.setFormatter(logging.Formatter('%(asctime)s - %(levelname)s - %(message)s'))
        logger.addHandler(file_handler)
    
    return loggers

# ===== DIRECTORY STRUCTURE SETUP =====
def create_directory_structure():
    """Create the required directory structure"""
    directories = [
        "config",
        "core", 
        "scrapers",
        "interfaces",
        "data",
        "logs"
    ]
    
    for directory in directories:
        Path(directory).mkdir(exist_ok=True)
    
    print("✅ Directory structure created")

# ===== CONFIGURATION LOADER =====
class ConfigManager:
    """Centralized configuration management"""
    
    def __init__(self):
        self.config_dir = Path("config")
        self.config_dir.mkdir(exist_ok=True)
        self.settings = {}
        self.load_all_configs()
    
    def load_all_configs(self):
        """Load all configuration files"""
        try:
            # Load main settings
            self.settings = self.load_config("settings.json", self.get_default_settings())
            
            # Load pairs configuration
            self.pairs = self.load_config("pairs.json", self.get_default_pairs())
            
            # Load schedules
            self.schedules = self.load_config("schedules.json", self.get_default_schedules())
            
            print("✅ Configuration loaded successfully")
            
        except Exception as e:
            print(f"❌ Error loading configuration: {e}")
            raise
    
    def load_config(self, filename, default_config):
        """Load individual config file or create with defaults"""
        config_path = self.config_dir / filename
        
        if config_path.exists():
            try:
                with open(config_path, 'r') as f:
                    config = json.load(f)
                print(f"📁 Loaded {filename}")
                return config
            except Exception as e:
                print(f"⚠️ Error loading {filename}, using defaults: {e}")
                return default_config
        else:
            # Create default config file
            with open(config_path, 'w') as f:
                json.dump(default_config, f, indent=2)
            print(f"📝 Created default {filename}")
            return default_config
    
    def get_default_settings(self):
        """Default system settings"""
        return {
            "system": {
                "name": "Unified Trading System",
                "version": "1.0",
                "environment": "production",
                "debug_mode": False
            },
            "mt5": {
                "account_number": 42903786,
                "magic_number": 50515253,
                "server": "MetaTrader5",
                "timeout": 30
            },
            "trading": {
                "enabled": True,
                "max_positions_per_pair": 1,
                "emergency_stop_enabled": True,
                "risk_management": {
                    "max_drawdown_percent": 50,
                    "max_daily_loss_percent": 10,
                    "max_concurrent_trades": 20
                }
            },
            "martingale": {
                "enabled": True,
                "max_layers": 15,
                "multiplier": 2,
                "emergency_dd_percentage": 50,
                "profit_buffer_pips": 5,
                "min_profit_percentage": 1,
                "flirt_threshold_pips": 10
            },
            "data_collection": {
                "enabled": True,
                "sentiment_interval_minutes": 30,
                "correlation_interval_minutes": 30,
                "economic_calendar_interval_minutes": 60,
                "cot_update_day": "friday",
                "cot_update_time": "18:00",
                "data_retention_days": 30
            },
            "testing": {
                "reduce_intervals": False,
                "sentiment_test_interval": 5,
                "correlation_test_interval": 5,
                "calendar_test_interval": 10,
                "dry_run_mode": False
            },
            "telegram": {
                "enabled": False,
                "bot_token": "",
                "chat_id": "",
                "alerts_enabled": True,
                "status_updates_interval": 300
            },
            "web_dashboard": {
                "enabled": False,
                "port": 8080,
                "host": "localhost"
            },
            "logging": {
                "level": "INFO",
                "max_file_size_mb": 50,
                "backup_count": 5,
                "console_output": True
            }
        }
    
    def get_default_pairs(self):
        """Default trading pairs configuration"""
        return {
            "monitored_pairs": [
                "AUDUSD", "USDCAD", "XAUUSD", "EURUSD", "GBPUSD",
                "AUDCAD", "USDCHF", "GBPCAD", "AUDNZD", "NZDCAD", 
                "US500", "BTCUSD"
            ],
            "pair_settings": {
                "AUDUSD": {
                    "enabled": True,
                    "risk_profile": "Medium",
                    "trading_days": [0, 1, 2, 3, 4],
                    "trading_hours": {"start": 0, "end": 23}
                },
                "USDCAD": {
                    "enabled": True,
                    "risk_profile": "Low",
                    "trading_days": [0, 1, 2, 3, 4],
                    "trading_hours": {"start": 0, "end": 23}
                },
                "XAUUSD": {
                    "enabled": True,
                    "risk_profile": "High",
                    "trading_days": [0, 1, 2, 3, 4],
                    "trading_hours": {"start": 0, "end": 23}
                },
                "EURUSD": {
                    "enabled": True,
                    "risk_profile": "Medium",
                    "trading_days": [0, 1, 2, 3, 4],
                    "trading_hours": {"start": 0, "end": 23}
                },
                "GBPUSD": {
                    "enabled": True,
                    "risk_profile": "Medium",
                    "trading_days": [0, 1, 2, 3, 4],
                    "trading_hours": {"start": 0, "end": 23}
                },
                "AUDCAD": {
                    "enabled": True,
                    "risk_profile": "Low",
                    "trading_days": [0, 1, 2, 3, 4],
                    "trading_hours": {"start": 0, "end": 23}
                },
                "USDCHF": {
                    "enabled": True,
                    "risk_profile": "High",
                    "trading_days": [0, 1, 2, 3, 4],
                    "trading_hours": {"start": 0, "end": 23}
                },
                "GBPCAD": {
                    "enabled": True,
                    "risk_profile": "Low",
                    "trading_days": [0, 1, 2, 3, 4],
                    "trading_hours": {"start": 0, "end": 23}
                },
                "AUDNZD": {
                    "enabled": True,
                    "risk_profile": "Medium",
                    "trading_days": [0, 1, 2, 3, 4],
                    "trading_hours": {"start": 0, "end": 23}
                },
                "NZDCAD": {
                    "enabled": True,
                    "risk_profile": "Low",
                    "trading_days": [0, 1, 2, 3, 4],
                    "trading_hours": {"start": 0, "end": 23}
                },
                "US500": {
                    "enabled": True,
                    "risk_profile": "High",
                    "trading_days": [0, 1, 2, 3, 4],
                    "trading_hours": {"start": 0, "end": 23}
                },
                "BTCUSD": {
                    "enabled": True,
                    "risk_profile": "Medium",
                    "trading_days": [0, 1, 2, 3, 4],
                    "trading_hours": {"start": 0, "end": 23}
                }
            },
            "risk_profiles": {
                "Low": {
                    "adx_threshold": 25,
                    "min_timeframes": 3,
                    "rsi_overbought": 70,
                    "rsi_oversold": 30,
                    "ema_buffer_pct": 0.005,
                    "risk_reward_ratio_long": 1.5,
                    "risk_reward_ratio_short": 1.5,
                    "min_adx_strength": 25,
                    "max_adx_strength": 60,
                    "risk_per_trade_pct": 0.05,
                    "atr_multiplier": 1.5,
                    "min_volatility_pips": 5
                },
                "Medium": {
                    "adx_threshold": 25,
                    "min_timeframes": 2,
                    "rsi_overbought": 70,
                    "rsi_oversold": 30,
                    "ema_buffer_pct": 0.005,
                    "risk_reward_ratio_long": 1.3,
                    "risk_reward_ratio_short": 1.3,
                    "min_adx_strength": 25,
                    "max_adx_strength": 60,
                    "risk_per_trade_pct": 0.1,
                    "atr_multiplier": 1.5,
                    "min_volatility_pips": 5
                },
                "High": {
                    "adx_threshold": 20,
                    "min_timeframes": 1,
                    "rsi_overbought": 70,
                    "rsi_oversold": 30,
                    "ema_buffer_pct": 0.008,
                    "risk_reward_ratio_long": 1.1,
                    "risk_reward_ratio_short": 1.1,
                    "min_adx_strength": 20,
                    "max_adx_strength": 70,
                    "risk_per_trade_pct": 0.2,
                    "atr_multiplier": 2,
                    "min_volatility_pips": 3
                }
            }
        }
    
    def get_default_schedules(self):
        """Default update schedules"""
        return {
            "data_collection": {
                "sentiment": {
                    "interval_minutes": 30,
                    "enabled": True,
                    "retry_attempts": 3,
                    "timeout_seconds": 60
                },
                "correlation": {
                    "interval_minutes": 30,
                    "enabled": True,
                    "retry_attempts": 3,
                    "timeout_seconds": 60
                },
                "economic_calendar": {
                    "interval_minutes": 60,
                    "enabled": True,
                    "retry_attempts": 3,
                    "timeout_seconds": 60
                },
                "cot": {
                    "update_day": "friday",
                    "update_time": "18:00",
                    "enabled": True,
                    "historical_weeks": 6,
                    "retry_attempts": 3,
                    "timeout_seconds": 300
                }
            },
            "trading": {
                "analysis_interval_minutes": 5,
                "position_check_interval_seconds": 30,
                "risk_check_interval_seconds": 60
            },
            "monitoring": {
                "emergency_check_interval_seconds": 30,
                "status_update_interval_minutes": 5,
                "health_check_interval_minutes": 1
            }
        }
    
    def get(self, key_path, default=None):
        """Get configuration value using dot notation (e.g., 'mt5.account_number')"""
        try:
            keys = key_path.split('.')
            value = self.settings
            
            for key in keys:
                value = value[key]
            
            return value
        except (KeyError, TypeError):
            return default
    
    def update(self, key_path, value):
        """Update configuration value and save"""
        try:
            keys = key_path.split('.')
            config = self.settings
            
            # Navigate to parent
            for key in keys[:-1]:
                if key not in config:
                    config[key] = {}
                config = config[key]
            
            # Set value
            config[keys[-1]] = value
            
            # Save to file
            self.save_config("settings.json", self.settings)
            
            return True
        except Exception as e:
            print(f"Error updating config: {e}")
            return False
    
    def save_config(self, filename, config):
        """Save configuration to file"""
        config_path = self.config_dir / filename
        with open(config_path, 'w') as f:
            json.dump(config, f, indent=2)

# ===== SYSTEM STATUS MANAGER =====
class SystemStatus:
    """Track overall system health and status"""
    
    def __init__(self):
        self.components = {
            'data_manager': {'status': 'stopped', 'last_update': None, 'error': None},
            'trading_engine': {'status': 'stopped', 'last_update': None, 'error': None},
            'risk_monitor': {'status': 'stopped', 'last_update': None, 'error': None},
            'telegram_bot': {'status': 'stopped', 'last_update': None, 'error': None}
        }
        self.start_time = datetime.now()
        self.system_status = 'initializing'
    
    def update_component_status(self, component, status, error=None):
        """Update status of individual component"""
        if component in self.components:
            self.components[component]['status'] = status
            self.components[component]['last_update'] = datetime.now()
            self.components[component]['error'] = error
    
    def get_overall_status(self):
        """Get overall system status"""
        running_components = sum(1 for comp in self.components.values() if comp['status'] == 'running')
        total_components = len(self.components)
        
        if running_components == total_components:
            return 'healthy'
        elif running_components > 0:
            return 'partial'
        else:
            return 'stopped'
    
    def get_status_summary(self):
        """Get detailed status summary"""
        uptime = datetime.now() - self.start_time
        
        return {
            'system_status': self.get_overall_status(),
            'uptime_seconds': int(uptime.total_seconds()),
            'uptime_formatted': str(uptime).split('.')[0],
            'components': self.components.copy(),
            'timestamp': datetime.now().isoformat()
        }

# ===== MAIN TRADING SYSTEM CLASS =====
class TradingSystemManager:
    """Main system coordinator"""
    
    def __init__(self):
        self.config = ConfigManager()
        self.status = SystemStatus()
        self.loggers = setup_logging()
        self.logger = logging.getLogger(__name__)
        
        # Component references
        self.data_manager = None
        self.trading_engine = None
        self.risk_monitor = None
        self.telegram_bot = None
        
        # Control flags
        self.running = False
        self.shutdown_requested = False
        
        # Threads
        self.threads = {}
    
    def initialize_system(self):
        """Initialize all system components"""
        try:
            self.logger.info("🚀 Initializing Unified Trading System...")
            
            # Create directories
            create_directory_structure()
            
            # Display configuration
            self.display_startup_info()
            
            # Import core modules (will be created in next phases)
            self.import_modules()
            
            self.logger.info("✅ System initialization complete")
            return True
            
        except Exception as e:
            self.logger.error(f"❌ System initialization failed: {e}")
            return False
    
    def display_startup_info(self):
        """Display system startup information"""
        print("\n" + "="*60)
        print("🤖 UNIFIED TRADING SYSTEM STARTING")
        print("="*60)
        print(f"Version: {self.config.get('system.version')}")
        print(f"Environment: {self.config.get('system.environment')}")
        print(f"MT5 Account: {self.config.get('mt5.account_number')}")
        print(f"Magic Number: {self.config.get('mt5.magic_number')}")
        print(f"Monitored Pairs: {len(self.config.pairs['monitored_pairs'])}")
        print(f"Trading Enabled: {self.config.get('trading.enabled')}")
        print(f"Martingale Enabled: {self.config.get('martingale.enabled')}")
        print(f"Data Collection Enabled: {self.config.get('data_collection.enabled')}")
        print(f"Telegram Enabled: {self.config.get('telegram.enabled')}")
        print("="*60)
    
    def import_modules(self):
        """Import core system modules"""
        try:
            # These will be created in subsequent phases
            # For now, we'll create placeholder imports
            
            self.logger.info("📦 Loading core modules...")
            
            # Try to import modules (will fail gracefully if not yet created)
            try:
                from trading_hub import TradingHub
                self.trading_hub = TradingHub(self.config, self.status, self.loggers)
                self.logger.info("✅ Trading Hub loaded")
            except ImportError:
                self.logger.warning("⚠️ Trading Hub not found - will be created in Phase 2")
                self.trading_hub = None
            
            try:
                from data_manager import DataManager
                self.data_manager = DataManager(self.config, self.loggers['data'])
                self.logger.info("✅ Data Manager loaded")
            except ImportError:
                self.logger.warning("⚠️ Data Manager not found - will be created in Phase 2")
                self.data_manager = None
            
            try:
                from trading_engine import TradingEngine
                self.trading_engine = TradingEngine(self.config, self.loggers['trading'])
                self.logger.info("✅ Trading Engine loaded")
            except ImportError:
                self.logger.warning("⚠️ Trading Engine not found - will be created in Phase 3")
                self.trading_engine = None
            
            try:
                from risk_monitor import RiskMonitor
                self.risk_monitor = RiskMonitor(self.config, self.loggers['risk'])
                self.logger.info("✅ Risk Monitor loaded")
            except ImportError:
                self.logger.warning("⚠️ Risk Monitor not found - will be created in Phase 3")
                self.risk_monitor = None
            
            if self.config.get('telegram.enabled'):
                try:
                    from telegram_bot import TelegramBot
                    self.telegram_bot = TelegramBot(self.config, self.loggers['telegram'])
                    self.logger.info("✅ Telegram Bot loaded")
                except ImportError:
                    self.logger.warning("⚠️ Telegram Bot not found - will be created in Phase 4")
                    self.telegram_bot = None
            
        except Exception as e:
            self.logger.error(f"❌ Error importing modules: {e}")
    
    def start_system(self):
        """Start all system components"""
        try:
            self.logger.info("🚀 Starting system components...")
            self.running = True
            
            # Start Data Manager
            if self.data_manager:
                self.start_component('data_manager', self.data_manager.run)
            
            # Start Risk Monitor
            if self.risk_monitor:
                self.start_component('risk_monitor', self.risk_monitor.run)
            
            # Start Trading Engine (after data manager is running)
            if self.trading_engine:
                time.sleep(2)  # Allow data manager to initialize
                self.start_component('trading_engine', self.trading_engine.run)
            
            # Start Telegram Bot
            if self.telegram_bot:
                self.start_component('telegram_bot', self.telegram_bot.run)
            
            self.logger.info("✅ All components started")
            return True
            
        except Exception as e:
            self.logger.error(f"❌ Error starting system: {e}")
            return False
    
    def start_component(self, component_name, target_function):
        """Start individual component in separate thread"""
        try:
            thread = threading.Thread(
                target=self.component_wrapper,
                args=(component_name, target_function),
                name=f"{component_name}_thread",
                daemon=True
            )
            thread.start()
            self.threads[component_name] = thread
            
            self.status.update_component_status(component_name, 'running')
            self.logger.info(f"🔄 Started {component_name}")
            
        except Exception as e:
            self.logger.error(f"❌ Failed to start {component_name}: {e}")
            self.status.update_component_status(component_name, 'error', str(e))
    
    def component_wrapper(self, component_name, target_function):
        """Wrapper for component execution with error handling"""
        try:
            self.logger.info(f"🔄 {component_name} thread starting...")
            target_function()
            
        except Exception as e:
            self.logger.error(f"❌ {component_name} crashed: {e}")
            self.status.update_component_status(component_name, 'error', str(e))
        
        finally:
            self.status.update_component_status(component_name, 'stopped')
            self.logger.warning(f"🛑 {component_name} thread stopped")
    
    def monitor_system(self):
        """Main system monitoring loop"""
        try:
            self.logger.info("👀 System monitoring started")
            
            while self.running and not self.shutdown_requested:
                try:
                    # Check component health
                    self.check_component_health()
                    
                    # Log status periodically
                    if datetime.now().minute % 5 == 0:  # Every 5 minutes
                        self.log_system_status()
                    
                    # Check for shutdown signal
                    if self.check_shutdown_signal():
                        break
                    
                    time.sleep(10)  # Check every 10 seconds
                    
                except KeyboardInterrupt:
                    self.logger.info("🛑 Shutdown requested by user")
                    break
                
                except Exception as e:
                    self.logger.error(f"❌ Error in monitoring loop: {e}")
                    time.sleep(5)
            
        except Exception as e:
            self.logger.error(f"❌ Fatal error in system monitor: {e}")
        
        finally:
            self.shutdown_system()
    
    def check_component_health(self):
        """Check health of all components"""
        try:
            for component_name, thread in self.threads.items():
                if not thread.is_alive():
                    self.logger.warning(f"⚠️ {component_name} thread died")
                    self.status.update_component_status(component_name, 'stopped')
                    
                    # TODO: Add restart logic in future phases
                    
        except Exception as e:
            self.logger.error(f"❌ Error checking component health: {e}")
    
    def check_shutdown_signal(self):
        """Check for external shutdown signals"""
        try:
            # Check for shutdown file
            shutdown_file = Path("data/shutdown_signal.json")
            if shutdown_file.exists():
                self.logger.info("🛑 Shutdown signal file detected")
                shutdown_file.unlink()  # Remove the file
                return True
            
            return False
            
        except Exception as e:
            self.logger.error(f"❌ Error checking shutdown signal: {e}")
            return False
    
    def log_system_status(self):
        """Log current system status"""
        try:
            status_summary = self.status.get_status_summary()
            
            self.logger.info(f"📊 System Status: {status_summary['system_status'].upper()}")
            self.logger.info(f"⏰ Uptime: {status_summary['uptime_formatted']}")
            
            running_components = [name for name, comp in status_summary['components'].items() 
                                if comp['status'] == 'running']
            
            if running_components:
                self.logger.info(f"✅ Running: {', '.join(running_components)}")
            
            failed_components = [name for name, comp in status_summary['components'].items() 
                               if comp['status'] == 'error']
            
            if failed_components:
                self.logger.warning(f"❌ Failed: {', '.join(failed_components)}")
                
        except Exception as e:
            self.logger.error(f"❌ Error logging system status: {e}")
    
    def shutdown_system(self):
        """Gracefully shutdown all components"""
        try:
            self.logger.info("🔄 Shutting down system...")
            self.running = False
            
            # Signal all components to stop
            for component_name in self.threads.keys():
                self.status.update_component_status(component_name, 'stopping')
            
            # Wait for threads to complete (with timeout)
            for component_name, thread in self.threads.items():
                thread.join(timeout=30)  # 30 second timeout
                if thread.is_alive():
                    self.logger.warning(f"⚠️ {component_name} thread did not stop gracefully")
            
            self.logger.info("✅ System shutdown complete")
            
        except Exception as e:
            self.logger.error(f"❌ Error during shutdown: {e}")

# ===== COMMAND LINE INTERFACE =====
def parse_command_line():
    """Parse command line arguments"""
    import argparse
    
    parser = argparse.ArgumentParser(description='Unified Trading System')
    parser.add_argument('--config', help='Configuration file path')
    parser.add_argument('--debug', action='store_true', help='Enable debug mode')
    parser.add_argument('--dry-run', action='store_true', help='Run in simulation mode')
    parser.add_argument('--test-intervals', action='store_true', help='Use reduced intervals for testing')
    
    return parser.parse_args()

# ===== MAIN EXECUTION =====
def main():
    """Main entry point"""
    try:
        print("🚀 Starting Unified Trading System...")
        
        # Parse command line arguments
        args = parse_command_line()
        
        # Initialize system manager
        system_manager = TradingSystemManager()
        
        # Apply command line overrides
        if args.debug:
            system_manager.config.update('system.debug_mode', True)
            system_manager.config.update('logging.level', 'DEBUG')
        
        if args.dry_run:
            system_manager.config.update('testing.dry_run_mode', True)
            system_manager.config.update('trading.enabled', False)
        
        if args.test_intervals:
            system_manager.config.update('testing.reduce_intervals', True)
        
        # Initialize system
        if not system_manager.initialize_system():
            print("❌ System initialization failed")
            return 1
        
        # Start all components
        if not system_manager.start_system():
            print("❌ System startup failed")
            return 1
        
        print("\n✅ System started successfully!")
        print("📱 Use Ctrl+C to stop the system")
        print("📊 Check logs/ directory for detailed logs")
        print("⚙️ Check config/ directory for configuration files")
        print("📁 Check data/ directory for market data")
        
        # Run main monitoring loop
        system_manager.monitor_system()
        
        return 0
        
    except KeyboardInterrupt:
        print("\n🛑 System stopped by user")
        return 0
    
    except Exception as e:
        print(f"\n❌ Fatal system error: {e}")
        import traceback
        traceback.print_exc()
        return 1

if __name__ == "__main__":
    sys.exit(main())