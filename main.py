#!/usr/bin/env python3
# ===== UNIFIED TRADING SYSTEM - SPYDER COMPATIBLE =====
# Phase 2: Integrated Data Management System - SPYDER IDE COMPATIBLE
# Version: 1.1 - Data Integration Complete

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
    
    # Configure main logger with UTF-8 encoding
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
    
    # Add file handlers for specialized logs with UTF-8 encoding
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
    
    print("Directory structure verified")

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
            
            print("Configuration loaded successfully")
            
        except Exception as e:
            print(f"Error loading configuration: {e}")
            raise
    
    def load_config(self, filename, default_config):
        """Load individual config file or create with defaults"""
        config_path = self.config_dir / filename
        
        if config_path.exists():
            try:
                with open(config_path, 'r', encoding='utf-8') as f:
                    config = json.load(f)
                print(f"Loaded {filename}")
                return config
            except Exception as e:
                print(f"Error loading {filename}, using defaults: {e}")
                return default_config
        else:
            # Create default config file
            with open(config_path, 'w', encoding='utf-8') as f:
                json.dump(default_config, f, indent=2)
            print(f"Created default {filename}")
            return default_config
    
    def get_default_settings(self):
        """Default system settings"""
        return {
            "system": {
                "name": "Unified Trading System",
                "version": "1.1",
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
        with open(config_path, 'w', encoding='utf-8') as f:
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
            self.logger.info("Starting Unified Trading System...")
            
            # Create directories
            create_directory_structure()
            
            # Display configuration
            self.display_startup_info()
            
            # Import core modules
            self.import_modules()
            
            self.logger.info("System initialization complete")
            return True
            
        except Exception as e:
            self.logger.error(f"System initialization failed: {e}")
            return False
    
    def display_startup_info(self):
        """Display system startup information"""
        print("\n" + "="*60)
        print("UNIFIED TRADING SYSTEM - PHASE 2")
        print("="*60)
        print(f"Version: {self.config.get('system.version')}")
        print(f"Environment: {self.config.get('system.environment')}")
        print(f"MT5 Account: {self.config.get('mt5.account_number')}")
        print(f"Magic Number: {self.config.get('mt5.magic_number')}")
        print(f"Monitored Pairs: {len(self.config.pairs['monitored_pairs'])}")
        print(f"Trading Enabled: {self.config.get('trading.enabled')}")
        print(f"Data Collection Enabled: {self.config.get('data_collection.enabled')}")
        print(f"Martingale Enabled: {self.config.get('martingale.enabled')}")
        print(f"Telegram Enabled: {self.config.get('telegram.enabled')}")
        print("="*60)
    
    def import_modules(self):
        """Import core system modules"""
        try:
            self.logger.info("Loading core modules...")
            
            # Import Data Manager (Phase 2 - Now Available)
            try:
                from data_manager import DataManager
                self.data_manager = DataManager(self.config, self.loggers['data'])
                self.logger.info("Data Manager loaded")
            except ImportError as e:
                self.logger.error(f"Data Manager not found: {e}")
                self.data_manager = None
            
            # Try to import Trading Hub
            try:
                from trading_hub import TradingHub
                self.trading_hub = TradingHub(self.config, self.status, self.loggers)
                self.logger.info("Trading Hub loaded")
            except ImportError:
                self.logger.warning("Trading Hub not found - will be created in Phase 3")
                self.trading_hub = None
            
            # Try to import Trading Engine
            try:
                from trading_engine import TradingEngine
                self.trading_engine = TradingEngine(self.config, self.loggers['trading'])
                self.logger.info("Trading Engine loaded")
            except ImportError:
                self.logger.warning("Trading Engine not found - will be created in Phase 3")
                self.trading_engine = None
            
            # Try to import Risk Monitor
            try:
                from risk_monitor import RiskMonitor
                self.risk_monitor = RiskMonitor(self.config, self.loggers['risk'])
                self.logger.info("Risk Monitor loaded")
            except ImportError:
                self.logger.warning("Risk Monitor not found - will be created in Phase 3")
                self.risk_monitor = None
            
            # Try to import Telegram Bot
            if self.config.get('telegram.enabled'):
                try:
                    from telegram_bot import TelegramBot
                    self.telegram_bot = TelegramBot(self.config, self.loggers['telegram'])
                    self.logger.info("Telegram Bot loaded")
                except ImportError:
                    self.logger.warning("Telegram Bot not found - will be created in Phase 4")
                    self.telegram_bot = None
            
        except Exception as e:
            self.logger.error(f"Error importing modules: {e}")
    
    def start_system(self):
        """Start all system components"""
        try:
            self.logger.info("Starting system components...")
            self.running = True
            
            # Start Data Manager (Phase 2 - Priority)
            if self.data_manager:
                self.start_component('data_manager', self.data_manager.run)
                time.sleep(2)  # Allow data manager to initialize
            
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
            
            self.logger.info("All available components started")
            return True
            
        except Exception as e:
            self.logger.error(f"Error starting system: {e}")
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
            self.logger.info(f"Started {component_name}")
            
        except Exception as e:
            self.logger.error(f"Failed to start {component_name}: {e}")
            self.status.update_component_status(component_name, 'error', str(e))
    
    def component_wrapper(self, component_name, target_function):
        """Wrapper for component execution with error handling"""
        try:
            self.logger.info(f"{component_name} thread starting...")
            target_function()
            
        except Exception as e:
            self.logger.error(f"{component_name} crashed: {e}")
            self.status.update_component_status(component_name, 'error', str(e))
        
        finally:
            self.status.update_component_status(component_name, 'stopped')
            self.logger.warning(f"{component_name} thread stopped")
    
    def monitor_system(self):
        """Main system monitoring loop"""
        try:
            self.logger.info("System monitoring started")
            
            while self.running and not self.shutdown_requested:
                try:
                    # Check component health
                    self.check_component_health()
                    
                    # Log status periodically (every 5 minutes)
                    if datetime.now().minute % 5 == 0 and datetime.now().second < 30:
                        self.log_system_status()
                    
                    # Enhanced data manager monitoring (Phase 2)
                    if self.data_manager:
                        self.monitor_data_manager()
                    
                    # Check for shutdown signal
                    if self.check_shutdown_signal():
                        break
                    
                    time.sleep(10)  # Check every 10 seconds
                    
                except KeyboardInterrupt:
                    self.logger.info("Shutdown requested by user")
                    break
                
                except Exception as e:
                    self.logger.error(f"Error in monitoring loop: {e}")
                    time.sleep(5)
            
        except Exception as e:
            self.logger.error(f"Fatal error in system monitor: {e}")
        
        finally:
            self.shutdown_system()
    
    def monitor_data_manager(self):
        """Enhanced monitoring for data manager (Phase 2)"""
        try:
            # Get data manager health every 30 seconds
            if datetime.now().second % 30 == 0:
                health = self.data_manager.get_system_health()
                
                # Check if any data sources are having issues
                if health.get('health_score', 0) < 50:
                    self.logger.warning(f"Data health degraded: {health.get('health_score', 0)}%")
                
                # Log fresh vs stale data sources
                fresh_count = health.get('fresh_sources', 0)
                total_count = health.get('total_sources', 0)
                
                if fresh_count < total_count:
                    stale_count = total_count - fresh_count
                    self.logger.info(f"Data Status: {fresh_count}/{total_count} sources fresh ({stale_count} stale)")
                
        except Exception as e:
            self.logger.error(f"Error monitoring data manager: {e}")
    
    def check_component_health(self):
        """Check health of all components"""
        try:
            for component_name, thread in self.threads.items():
                if not thread.is_alive():
                    self.logger.warning(f"{component_name} thread died")
                    self.status.update_component_status(component_name, 'stopped')
                    
        except Exception as e:
            self.logger.error(f"Error checking component health: {e}")
    
    def check_shutdown_signal(self):
        """Check for external shutdown signals"""
        try:
            # Check for shutdown file
            shutdown_file = Path("data/shutdown_signal.json")
            if shutdown_file.exists():
                self.logger.info("Shutdown signal file detected")
                shutdown_file.unlink()  # Remove the file
                return True
            
            return False
            
        except Exception as e:
            self.logger.error(f"Error checking shutdown signal: {e}")
            return False
    
    def log_system_status(self):
        """Log current system status"""
        try:
            status_summary = self.status.get_status_summary()
            
            self.logger.info(f"System Status: {status_summary['system_status'].upper()}")
            self.logger.info(f"Uptime: {status_summary['uptime_formatted']}")
            
            running_components = [name for name, comp in status_summary['components'].items() 
                                if comp['status'] == 'running']
            
            if running_components:
                self.logger.info(f"Running: {', '.join(running_components)}")
            
            failed_components = [name for name, comp in status_summary['components'].items() 
                               if comp['status'] == 'error']
            
            if failed_components:
                self.logger.warning(f"Failed: {', '.join(failed_components)}")
            
            # Enhanced data manager status logging (Phase 2)
            if self.data_manager:
                try:
                    health = self.data_manager.get_system_health()
                    self.logger.info(f"Data Health: {health.get('health_score', 0)}% ({health.get('fresh_sources', 0)}/{health.get('total_sources', 0)} sources fresh)")
                except Exception as e:
                    self.logger.warning(f"Could not get data manager health: {e}")
                
        except Exception as e:
            self.logger.error(f"Error logging system status: {e}")
    
    def shutdown_system(self):
        """Gracefully shutdown all components"""
        try:
            self.logger.info("Shutting down system...")
            self.running = False
            
            # Signal all components to stop
            for component_name in self.threads.keys():
                self.status.update_component_status(component_name, 'stopping')
            
            # Request shutdown for data manager (Phase 2)
            if self.data_manager:
                try:
                    self.data_manager.request_shutdown()
                    self.logger.info("Data Manager shutdown requested")
                except Exception as e:
                    self.logger.warning(f"Error requesting data manager shutdown: {e}")
            
            # Wait for threads to complete (with timeout)
            for component_name, thread in self.threads.items():
                thread.join(timeout=30)  # 30 second timeout
                if thread.is_alive():
                    self.logger.warning(f"{component_name} thread did not stop gracefully")
            
            # Cleanup data manager (Phase 2)
            if self.data_manager:
                try:
                    self.data_manager.cleanup()
                    self.logger.info("Data Manager cleanup completed")
                except Exception as e:
                    self.logger.warning(f"Error cleaning up data manager: {e}")
            
            self.logger.info("System shutdown complete")
            
        except Exception as e:
            self.logger.error(f"Error during shutdown: {e}")

# ===== QUICK TEST FUNCTION FOR SPYDER =====
def test_system_quick():
    """Quick test function that can be run in Spyder"""
    print("="*60)
    print("QUICK SYSTEM TEST")
    print("="*60)
    
    try:
        # Test 1: Create system manager
        print("Test 1: Creating system manager...")
        system_manager = TradingSystemManager()
        print("   OK: System manager created")
        
        # Test 2: Check data manager
        if system_manager.data_manager:
            print("Test 2: Data manager check...")
            scrapers = list(system_manager.data_manager.scrapers.keys())
            print(f"   OK: Data manager loaded with {len(scrapers)} scrapers: {scrapers}")
            
            # Test 3: Check health
            health = system_manager.data_manager.get_system_health()
            print(f"   OK: System health: {health.get('health_score', 0)}%")
            
            # Test 4: Test force update
            print("Test 3: Testing force update...")
            success = system_manager.data_manager.force_update('economic_calendar')
            print(f"   Force update result: {'SUCCESS' if success else 'FAILED'}")
            
            # Test 5: Check market data file
            market_data_file = Path("data/market_data.json")
            if market_data_file.exists():
                print("Test 4: Market data file check...")
                with open(market_data_file, 'r', encoding='utf-8') as f:
                    market_data = json.load(f)
                
                system_status = market_data.get('system_status', 'unknown')
                print(f"   OK: Market data file exists, status: {system_status}")
                
                # Show data sources
                data_sources = market_data.get('data_sources', {})
                for source_name, source_data in data_sources.items():
                    status = source_data.get('status', 'unknown')
                    print(f"   {source_name}: {status}")
                
                print("\n" + "="*60)
                print("TEST RESULTS: SUCCESS")
                print("="*60)
                print("System is ready for operation!")
                print("\nYou can now:")
                print("1. Run individual data updates")
                print("2. Start the full system")
                print("3. Monitor data collection")
                
                return True
            else:
                print("   ERROR: Market data file not created")
                return False
        else:
            print("   ERROR: Data manager not available")
            return False
            
    except Exception as e:
        print(f"TEST ERROR: {e}")
        import traceback
        traceback.print_exc()
        return False

# ===== DATA COLLECTION TEST FUNCTIONS =====
def test_individual_scrapers():
    """Test each scraper individually"""
    print("="*60)
    print("INDIVIDUAL SCRAPER TESTS")
    print("="*60)
    
    try:
        # Create system manager
        system_manager = TradingSystemManager()
        
        if not system_manager.data_manager:
            print("ERROR: Data manager not available")
            return False
        
        data_manager = system_manager.data_manager
        
        # Test each scraper
        scrapers_to_test = ['economic_calendar', 'sentiment', 'correlation', 'cot']
        results = {}
        
        for scraper_name in scrapers_to_test:
            print(f"\nTesting {scraper_name}...")
            try:
                success = data_manager.force_update(scraper_name)
                results[scraper_name] = success
                print(f"   Result: {'SUCCESS' if success else 'FAILED'}")
                
                # Wait a moment between tests
                time.sleep(2)
                
            except Exception as e:
                results[scraper_name] = False
                print(f"   ERROR: {e}")
        
        # Summary
        print("\n" + "="*60)
        print("SCRAPER TEST SUMMARY")
        print("="*60)
        
        successful = sum(1 for result in results.values() if result)
        total = len(results)
        
        for scraper_name, result in results.items():
            status = "PASS" if result else "FAIL"
            print(f"{scraper_name}: {status}")
        
        print(f"\nOverall: {successful}/{total} scrapers working")
        
        if successful > 0:
            print("\nSome scrapers are working! You can:")
            print("1. Check data/market_data.json for results")
            print("2. Run the full system with available scrapers")
            print("3. Check logs/data.log for detailed information")
        
        return successful > 0
        
    except Exception as e:
        print(f"ERROR: {e}")
        return False

def run_data_collection_test(duration_minutes=5):
    """Run data collection for a specified duration"""
    print("="*60)
    print(f"DATA COLLECTION TEST - {duration_minutes} MINUTES")
    print("="*60)
    
    try:
        # Create system manager with test intervals
        system_manager = TradingSystemManager()
        
        # Set fast intervals for testing
        system_manager.config.schedules['data_collection']['sentiment']['interval_minutes'] = 2
        system_manager.config.schedules['data_collection']['correlation']['interval_minutes'] = 2
        system_manager.config.schedules['data_collection']['economic_calendar']['interval_minutes'] = 3
        
        print("Starting data collection with fast test intervals...")
        print("   Sentiment: every 2 minutes")
        print("   Correlation: every 2 minutes") 
        print("   Economic Calendar: every 3 minutes")
        print("   COT: weekly (normal schedule)")
        
        # Initialize and start system
        if not system_manager.initialize_system():
            print("ERROR: System initialization failed")
            return False
        
        if not system_manager.start_system():
            print("ERROR: System startup failed")
            return False
        
        print(f"\nRunning for {duration_minutes} minutes...")
        print("Press Ctrl+C to stop early")
        
        # Run for specified duration
        start_time = time.time()
        end_time = start_time + (duration_minutes * 60)
        
        update_count = 0
        last_status_time = start_time
        
        try:
            while time.time() < end_time:
                # Show status every 30 seconds
                if time.time() - last_status_time >= 30:
                    elapsed = (time.time() - start_time) / 60
                    remaining = duration_minutes - elapsed
                    
                    if system_manager.data_manager:
                        health = system_manager.data_manager.get_system_health()
                        health_score = health.get('health_score', 0)
                        fresh_sources = health.get('fresh_sources', 0)
                        total_sources = health.get('total_sources', 0)
                        
                        print(f"   Status: {elapsed:.1f}/{duration_minutes} min | "
                              f"Health: {health_score}% | "
                              f"Fresh: {fresh_sources}/{total_sources}")
                    
                    last_status_time = time.time()
                
                time.sleep(1)
                
        except KeyboardInterrupt:
            print("\n   Stopped by user")
        
        # Final status
        print("\nFinal Results:")
        if system_manager.data_manager:
            health = system_manager.data_manager.get_system_health()
            market_data = system_manager.data_manager.get_market_data()
            
            print(f"   Health Score: {health.get('health_score', 0)}%")
            
            # Show data source status
            data_sources = market_data.get('data_sources', {})
            for source_name, source_data in data_sources.items():
                status = source_data.get('status', 'unknown')
                last_update = source_data.get('last_update', 'never')
                
                if last_update != 'never':
                    try:
                        update_time = datetime.fromisoformat(last_update)
                        age_minutes = (datetime.now() - update_time).total_seconds() / 60
                        age_str = f"{age_minutes:.1f}m ago"
                    except:
                        age_str = "unknown age"
                else:
                    age_str = "never updated"
                
                print(f"   {source_name}: {status} ({age_str})")
        
        # Shutdown
        system_manager.shutdown_system()
        
        print("\nData collection test completed!")
        print("Check the following files for results:")
        print("   - data/market_data.json (unified data)")
        print("   - logs/data.log (detailed logs)")
        print("   - logs/system.log (system logs)")
        
        return True
        
    except Exception as e:
        print(f"ERROR: {e}")
        import traceback
        traceback.print_exc()
        return False

# ===== COMMAND LINE INTERFACE =====
def parse_command_line():
    """Parse command line arguments"""
    import argparse
    
    parser = argparse.ArgumentParser(description='Unified Trading System - Phase 2')
    parser.add_argument('--config', help='Configuration file path')
    parser.add_argument('--debug', action='store_true', help='Enable debug mode')
    parser.add_argument('--dry-run', action='store_true', help='Run in simulation mode')
    parser.add_argument('--test-intervals', action='store_true', help='Use reduced intervals for testing')
    parser.add_argument('--data-only', action='store_true', help='Run data collection only (no trading)')
    parser.add_argument('--force-update', help='Force update specific data source (calendar|sentiment|correlation|cot|all)')
    parser.add_argument('--test-quick', action='store_true', help='Run quick system test')
    parser.add_argument('--test-scrapers', action='store_true', help='Test individual scrapers')
    parser.add_argument('--test-collection', type=int, metavar='MINUTES', help='Run data collection test for N minutes')
    
    return parser.parse_args()

# ===== MAIN EXECUTION =====
def main():
    """Main entry point"""
    try:
        print("Starting Unified Trading System - Phase 2...")
        
        # Parse command line arguments
        args = parse_command_line()
        
        # Handle test commands first
        if args.test_quick:
            return 0 if test_system_quick() else 1
        
        if args.test_scrapers:
            return 0 if test_individual_scrapers() else 1
        
        if args.test_collection:
            return 0 if run_data_collection_test(args.test_collection) else 1
        
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
            # Apply reduced intervals to schedules
            system_manager.config.schedules['data_collection']['sentiment']['interval_minutes'] = 5
            system_manager.config.schedules['data_collection']['correlation']['interval_minutes'] = 5
            system_manager.config.schedules['data_collection']['economic_calendar']['interval_minutes'] = 10
        
        if args.data_only:
            system_manager.config.update('trading.enabled', False)
            print("Data collection only mode enabled")
        
        # Initialize system
        if not system_manager.initialize_system():
            print("System initialization failed")
            return 1
        
        # Handle force update command
        if args.force_update:
            if system_manager.data_manager:
                print(f"Forcing update for: {args.force_update}")
                source = args.force_update if args.force_update != 'all' else None
                success = system_manager.data_manager.force_update(source)
                print(f"Force update {'succeeded' if success else 'failed'}")
                return 0 if success else 1
            else:
                print("Data Manager not available for force update")
                return 1
        
        # Start all components
        if not system_manager.start_system():
            print("System startup failed")
            return 1
        
        print("\nSystem started successfully!")
        print("Use Ctrl+C to stop the system")
        print("Check logs/ directory for detailed logs")
        print("Check config/ directory for configuration files")
        print("Check data/ directory for market data")
        
        # Phase 2 specific information
        if system_manager.data_manager:
            print("Phase 2: Data Collection System Active")
            print("   - Economic Calendar: Updates every hour")
            print("   - Sentiment Analysis: Updates every 30 minutes")
            print("   - Correlation Data: Updates every 30 minutes")
            print("   - COT Data: Updates weekly on Friday")
            print("   - Unified Data: Available in data/market_data.json")
        
        # Run main monitoring loop
        system_manager.monitor_system()
        
        return 0
        
    except KeyboardInterrupt:
        print("\nSystem stopped by user")
        return 0
    
    except Exception as e:
        print(f"\nFatal system error: {e}")
        import traceback
        traceback.print_exc()
        return 1

if __name__ == "__main__":
    sys.exit(main())