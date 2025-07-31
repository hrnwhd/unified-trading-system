#!/usr/bin/env python3
# ===== UNIFIED TRADING SYSTEM - MAIN ENTRY POINT (PHASE 3) =====
# Enhanced with intelligent trading engine integration
# Version: 1.3 - Trading Intelligence Integration

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
    
    print("✅ Directory structure verified")

# ===== ENHANCED CONFIGURATION LOADER =====
class EnhancedConfigManager:
    """Enhanced configuration management for Phase 3"""
    
    def __init__(self):
        self.config_dir = Path("config")
        self.config_dir.mkdir(exist_ok=True)
        self.settings = {}
        self.load_all_configs()
    
    def load_all_configs(self):
        """Load all configuration files"""
        try:
            # Load main settings
            self.settings = self.load_config("settings.json", self.get_enhanced_default_settings())
            
            # Load pairs configuration
            self.pairs = self.load_config("pairs.json", self.get_default_pairs())
            
            # Load schedules
            self.schedules = self.load_config("schedules.json", self.get_default_schedules())
            
            print("✅ Enhanced configuration loaded successfully")
            
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
    
    def get_enhanced_default_settings(self):
        """Enhanced default system settings for Phase 3"""
        return {
            "system": {
                "name": "Unified Trading System",
                "version": "1.3",
                "environment": "production",
                "debug_mode": False,
                "phase": 3
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
                "enhanced_engine": True,
                "risk_management": {
                    "max_drawdown_percent": 50,
                    "max_daily_loss_percent": 10,
                    "max_concurrent_trades": 20
                }
            },
            "data_integration": {
                "enabled": True,
                "sentiment_threshold": 70,
                "correlation_risk_threshold": 70,
                "economic_event_buffer_hours": 1,
                "cache_duration_seconds": 60,
                "fallback_on_error": True
            },
            "enhanced_risk_management": {
                "correlation_adjustment": True,
                "economic_event_adjustment": True,
                "sentiment_based_blocking": True,
                "dynamic_position_sizing": True,
                "risk_reduction_factors": {
                    "high_correlation": 0.8,
                    "major_events": 0.7,
                    "extreme_sentiment": 0.9
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
                "status_updates_interval": 300,
                "enhanced_notifications": True
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
                "console_output": True,
                "enhanced_logging": True
            }
        }
    
    def get_default_pairs(self):
        """Default trading pairs configuration (unchanged)"""
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
                    "trading_hours": {"start": 0, "end": 23},
                    "data_integration": True
                },
                "USDCAD": {
                    "enabled": True,
                    "risk_profile": "Low",
                    "trading_days": [0, 1, 2, 3, 4],
                    "trading_hours": {"start": 0, "end": 23},
                    "data_integration": True
                },
                "XAUUSD": {
                    "enabled": True,
                    "risk_profile": "High",
                    "trading_days": [0, 1, 2, 3, 4],
                    "trading_hours": {"start": 0, "end": 23},
                    "data_integration": True
                },
                "EURUSD": {
                    "enabled": True,
                    "risk_profile": "Medium",
                    "trading_days": [0, 1, 2, 3, 4],
                    "trading_hours": {"start": 0, "end": 23},
                    "data_integration": True
                },
                "GBPUSD": {
                    "enabled": True,
                    "risk_profile": "Medium",
                    "trading_days": [0, 1, 2, 3, 4],
                    "trading_hours": {"start": 0, "end": 23},
                    "data_integration": True
                },
                "AUDCAD": {
                    "enabled": True,
                    "risk_profile": "Low",
                    "trading_days": [0, 1, 2, 3, 4],
                    "trading_hours": {"start": 0, "end": 23},
                    "data_integration": True
                },
                "USDCHF": {
                    "enabled": True,
                    "risk_profile": "High",
                    "trading_days": [0, 1, 2, 3, 4],
                    "trading_hours": {"start": 0, "end": 23},
                    "data_integration": True
                },
                "GBPCAD": {
                    "enabled": True,
                    "risk_profile": "Low",
                    "trading_days": [0, 1, 2, 3, 4],
                    "trading_hours": {"start": 0, "end": 23},
                    "data_integration": True
                },
                "AUDNZD": {
                    "enabled": True,
                    "risk_profile": "Medium",
                    "trading_days": [0, 1, 2, 3, 4],
                    "trading_hours": {"start": 0, "end": 23},
                    "data_integration": True
                },
                "NZDCAD": {
                    "enabled": True,
                    "risk_profile": "Low",
                    "trading_days": [0, 1, 2, 3, 4],
                    "trading_hours": {"start": 0, "end": 23},
                    "data_integration": True
                },
                "US500": {
                    "enabled": True,
                    "risk_profile": "High",
                    "trading_days": [0, 1, 2, 3, 4],
                    "trading_hours": {"start": 0, "end": 23},
                    "data_integration": True
                },
                "BTCUSD": {
                    "enabled": True,
                    "risk_profile": "Medium",
                    "trading_days": [0, 1, 2, 3, 4],
                    "trading_hours": {"start": 0, "end": 23},
                    "data_integration": True
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
        """Default update schedules (unchanged)"""
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
                "risk_check_interval_seconds": 60,
                "enhanced_decision_interval_seconds": 60
            },
            "monitoring": {
                "emergency_check_interval_seconds": 30,
                "status_update_interval_minutes": 5,
                "health_check_interval_minutes": 1,
                "data_connectivity_check_minutes": 10
            }
        }
    
    def get(self, key_path, default=None):
        """Get configuration value using dot notation"""
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

# ===== ENHANCED SYSTEM STATUS MANAGER =====
class EnhancedSystemStatus:
    """Enhanced system health tracking for Phase 3"""
    
    def __init__(self):
        self.components = {
            'data_manager': {'status': 'stopped', 'last_update': None, 'error': None},
            'enhanced_trading_engine': {'status': 'stopped', 'last_update': None, 'error': None},
            'risk_monitor': {'status': 'stopped', 'last_update': None, 'error': None},
            'telegram_bot': {'status': 'stopped', 'last_update': None, 'error': None},
            'trading_hub': {'status': 'stopped', 'last_update': None, 'error': None}
        }
        self.start_time = datetime.now()
        self.system_status = 'initializing'
        self.data_integration_status = 'unknown'
        self.enhanced_features_active = False
    
    def update_component_status(self, component, status, error=None):
        """Update status of individual component"""
        if component in self.components:
            self.components[component]['status'] = status
            self.components[component]['last_update'] = datetime.now()
            self.components[component]['error'] = error
            
            # Update enhanced features status
            if component == 'enhanced_trading_engine' and status == 'running':
                self.enhanced_features_active = True
    
    def update_data_integration_status(self, status):
        """Update data integration status"""
        self.data_integration_status = status
    
    def get_overall_status(self):
        """Get overall system status"""
        running_components = sum(1 for comp in self.components.values() if comp['status'] == 'running')
        total_components = len(self.components)
        
        if running_components == total_components:
            return 'optimal'
        elif running_components > total_components * 0.7:
            return 'healthy'
        elif running_components > 0:
            return 'partial'
        else:
            return 'stopped'
    
    def get_enhanced_status_summary(self):
        """Get detailed status summary for Phase 3"""
        uptime = datetime.now() - self.start_time
        
        return {
            'system_status': self.get_overall_status(),
            'uptime_seconds': int(uptime.total_seconds()),
            'uptime_formatted': str(uptime).split('.')[0],
            'components': self.components.copy(),
            'timestamp': datetime.now().isoformat(),
            'enhanced_features_active': self.enhanced_features_active,
            'data_integration_status': self.data_integration_status,
            'phase': 3
        }

# ===== ENHANCED TRADING SYSTEM MANAGER =====
class EnhancedTradingSystemManager:
    """Enhanced system coordinator for Phase 3"""
    
    def __init__(self):
        self.config = EnhancedConfigManager()
        self.status = EnhancedSystemStatus()
        self.loggers = setup_logging()
        self.logger = logging.getLogger(__name__)
        
        # Component references
        self.data_manager = None
        self.enhanced_trading_engine = None
        self.risk_monitor = None
        self.telegram_bot = None
        self.trading_hub = None
        
        # Control flags
        self.running = False
        self.shutdown_requested = False
        
        # Threads
        self.threads = {}
    
    def initialize_system(self):
        """Initialize all system components for Phase 3"""
        try:
            self.logger.info("🚀 Initializing Enhanced Trading System (Phase 3)...")
            
            # Create directories
            create_directory_structure()
            
            # Display enhanced startup info
            self.display_enhanced_startup_info()
            
            # Import enhanced modules
            self.import_enhanced_modules()
            
            self.logger.info("✅ Enhanced system initialization complete")
            return True
            
        except Exception as e:
            self.logger.error(f"❌ Enhanced system initialization failed: {e}")
            return False
    
    def display_enhanced_startup_info(self):
        """Display enhanced system startup information"""
        print("\n" + "="*70)
        print("🤖 ENHANCED UNIFIED TRADING SYSTEM - PHASE 3")
        print("="*70)
        print(f"Version: {self.config.get('system.version')}")
        print(f"Phase: {self.config.get('system.phase')}")
        print(f"Environment: {self.config.get('system.environment')}")
        print(f"MT5 Account: {self.config.get('mt5.account_number')}")
        print(f"Magic Number: {self.config.get('mt5.magic_number')}")
        print(f"Monitored Pairs: {len(self.config.pairs['monitored_pairs'])}")
        print(f"Enhanced Trading: {self.config.get('trading.enhanced_engine')}")
        print(f"Data Integration: {self.config.get('data_integration.enabled')}")
        print(f"Sentiment Threshold: {self.config.get('data_integration.sentiment_threshold')}%")
        print(f"Correlation Risk: {self.config.get('data_integration.correlation_risk_threshold')}%")
        print(f"Economic Buffer: {self.config.get('data_integration.economic_event_buffer_hours')}h")
        print(f"Telegram: {self.config.get('telegram.enabled')}")
        print("="*70)
    
    def import_enhanced_modules(self):
        """Import enhanced system modules for Phase 3"""
        try:
            self.logger.info("📦 Loading enhanced modules...")
            
            # Trading Hub
            try:
                from trading_hub import TradingHub
                self.trading_hub = TradingHub(self.config, self.status, self.loggers)
                self.logger.info("✅ Trading Hub loaded")
            except ImportError:
                self.logger.warning("⚠️ Trading Hub not found")
                self.trading_hub = None
            
            # Data Manager
            try:
                from data_manager import DataManager
                self.data_manager = DataManager(self.config, self.loggers['data'])
                self.logger.info("✅ Data Manager loaded")
            except ImportError:
                self.logger.warning("⚠️ Data Manager not found")
                self.data_manager = None
            
            # Enhanced Trading Engine (NEW)
            try:
                from trading_engine import run_enhanced_robot
                self.enhanced_trading_engine = run_enhanced_robot
                self.logger.info("✅ Enhanced Trading Engine loaded")
                self.status.enhanced_features_active = True
            except ImportError:
                # Fallback to original trading engine
                try:
                    from trading_engine import run_simplified_robot
                    self.enhanced_trading_engine = run_simplified_robot
                    self.logger.warning("⚠️ Using fallback trading engine (not enhanced)")
                except ImportError:
                    self.logger.warning("⚠️ No trading engine found")
                    self.enhanced_trading_engine = None
            
            # Risk Monitor
            try:
                from risk_monitor import run_emergency_monitor
                self.risk_monitor = run_emergency_monitor
                self.logger.info("✅ Risk Monitor loaded")
            except ImportError:
                self.logger.warning("⚠️ Risk Monitor not found")
                self.risk_monitor = None
            
            # Telegram Bot (if enabled)
            if self.config.get('telegram.enabled'):
                try:
                    from telegram_bot import TelegramBot
                    self.telegram_bot = TelegramBot(self.config, self.loggers['telegram'])
                    self.logger.info("✅ Telegram Bot loaded")
                except ImportError:
                    self.logger.warning("⚠️ Telegram Bot not found")
                    self.telegram_bot = None
            
        except Exception as e:
            self.logger.error(f"❌ Error importing enhanced modules: {e}")
    
    def start_system(self):
        """Start all enhanced system components"""
        try:
            self.logger.info("🚀 Starting enhanced system components...")
            self.running = True
            
            # Start Data Manager first (Phase 2)
            if self.data_manager:
                self.start_component('data_manager', self.data_manager.run)
                time.sleep(3)  # Allow data manager to initialize
            
            # Start Trading Hub
            if self.trading_hub:
                self.start_component('trading_hub', self.trading_hub.run)
                time.sleep(2)
            
            # Start Risk Monitor
            if self.risk_monitor:
                self.start_component('risk_monitor', self.risk_monitor)
                time.sleep(2)
            
            # Start Enhanced Trading Engine (Phase 3)
            if self.enhanced_trading_engine:
                self.start_component('enhanced_trading_engine', self.enhanced_trading_engine)
                time.sleep(2)
            
            # Start Telegram Bot (if enabled)
            if self.telegram_bot:
                self.start_component('telegram_bot', self.telegram_bot.run)
            
            self.logger.info("✅ All enhanced components started")
            return True
            
        except Exception as e:
            self.logger.error(f"❌ Error starting enhanced system: {e}")
            return False
    
    def start_component(self, component_name, target_function):
        """Start individual component in separate thread"""
        try:
            thread = threading.Thread(
                target=self.enhanced_component_wrapper,
                args=(component_name, target_function),
                name=f"{component_name}_thread",
                daemon=True
            )
            thread.start()
            self.threads[component_name] = thread
            
            self.status.update_component_status(component_name, 'running')
            self.logger.info(f"🔄 Started enhanced {component_name}")
            
        except Exception as e:
            self.logger.error(f"❌ Failed to start {component_name}: {e}")
            self.status.update_component_status(component_name, 'error', str(e))
    
    def enhanced_component_wrapper(self, component_name, target_function):
        """Enhanced wrapper for component execution with better error handling"""
        try:
            self.logger.info(f"🔄 {component_name} enhanced thread starting...")
            target_function()
            
        except Exception as e:
            self.logger.error(f"❌ {component_name} crashed: {e}")
            self.status.update_component_status(component_name, 'error', str(e))
            
            # Enhanced error recovery for critical components
            if component_name in ['enhanced_trading_engine', 'data_manager']:
                self.logger.info(f"🔄 Attempting to restart critical component: {component_name}")
                time.sleep(30)  # Wait before restart attempt
                try:
                    target_function()
                except Exception as restart_error:
                    self.logger.error(f"❌ Restart failed for {component_name}: {restart_error}")
        
        finally:
            self.status.update_component_status(component_name, 'stopped')
            self.logger.warning(f"🛑 {component_name} enhanced thread stopped")
    
    def monitor_enhanced_system(self):
        """Enhanced system monitoring loop"""
        try:
            self.logger.info("👀 Enhanced system monitoring started")
            
            while self.running and not self.shutdown_requested:
                try:
                    # Check component health
                    self.check_enhanced_component_health()
                    
                    # Check data integration status
                    self.check_data_integration_health()
                    
                    # Log enhanced status periodically
                    if datetime.now().minute % 5 == 0:  # Every 5 minutes
                        self.log_enhanced_system_status()
                    
                    # Check for shutdown signal
                    if self.check_shutdown_signal():
                        break
                    
                    time.sleep(10)  # Check every 10 seconds
                    
                except KeyboardInterrupt:
                    self.logger.info("🛑 Enhanced shutdown requested by user")
                    break
                
                except Exception as e:
                    self.logger.error(f"❌ Error in enhanced monitoring loop: {e}")
                    time.sleep(5)
            
        except Exception as e:
            self.logger.error(f"❌ Fatal error in enhanced system monitor: {e}")
        
        finally:
            self.shutdown_enhanced_system()
    
    def check_enhanced_component_health(self):
        """Enhanced component health checking"""
        try:
            for component_name, thread in self.threads.items():
                if not thread.is_alive():
                    self.logger.warning(f"⚠️ {component_name} enhanced thread died")
                    self.status.update_component_status(component_name, 'stopped')
                    
                    # Enhanced restart logic for critical components
                    if component_name in ['enhanced_trading_engine', 'data_manager']:
                        self.logger.info(f"🔄 Critical component down, attempting restart: {component_name}")
                        # TODO: Add restart logic in future versions
                        
        except Exception as e:
            self.logger.error(f"❌ Error checking enhanced component health: {e}")
    
    def check_data_integration_health(self):
        """Check health of data integration systems"""
        try:
            data_dir = Path("data")
            market_data_file = data_dir / "market_data.json"
            
            if market_data_file.exists():
                # Check data freshness
                with open(market_data_file, 'r') as f:
                    market_data = json.load(f)
                
                last_updated = market_data.get('last_updated')
                if last_updated:
                    last_update_time = datetime.fromisoformat(last_updated)
                    age_minutes = (datetime.now() - last_update_time).total_seconds() / 60
                    
                    if age_minutes < 10:
                        self.status.update_data_integration_status('fresh')
                    elif age_minutes < 60:
                        self.status.update_data_integration_status('stale')
                    else:
                        self.status.update_data_integration_status('old')
                else:
                    self.status.update_data_integration_status('unknown')
            else:
                self.status.update_data_integration_status('missing')
                
        except Exception as e:
            self.logger.error(f"❌ Error checking data integration health: {e}")
            self.status.update_data_integration_status('error')
    
    def check_shutdown_signal(self):
        """Check for external shutdown signals"""
        try:
            # Check for shutdown file
            shutdown_file = Path("data/shutdown_signal.json")
            if shutdown_file.exists():
                self.logger.info("🛑 Enhanced shutdown signal file detected")
                shutdown_file.unlink()  # Remove the file
                return True
            
            return False
            
        except Exception as e:
            self.logger.error(f"❌ Error checking shutdown signal: {e}")
            return False
    
    def log_enhanced_system_status(self):
        """Log enhanced system status"""
        try:
            status_summary = self.status.get_enhanced_status_summary()
            
            self.logger.info(f"📊 Enhanced System Status: {status_summary['system_status'].upper()}")
            self.logger.info(f"⏰ Uptime: {status_summary['uptime_formatted']}")
            self.logger.info(f"🧠 Enhanced Features: {'ACTIVE' if status_summary['enhanced_features_active'] else 'INACTIVE'}")
            self.logger.info(f"📊 Data Integration: {status_summary['data_integration_status'].upper()}")
            
            running_components = [name for name, comp in status_summary['components'].items() 
                                if comp['status'] == 'running']
            
            if running_components:
                self.logger.info(f"✅ Running: {', '.join(running_components)}")
            
            failed_components = [name for name, comp in status_summary['components'].items() 
                               if comp['status'] == 'error']
            
            if failed_components:
                self.logger.warning(f"❌ Failed: {', '.join(failed_components)}")
                
        except Exception as e:
            self.logger.error(f"❌ Error logging enhanced system status: {e}")
    
    def shutdown_enhanced_system(self):
        """Enhanced graceful shutdown"""
        try:
            self.logger.info("🔄 Shutting down enhanced system...")
            self.running = False
            
            # Signal all components to stop
            for component_name in self.threads.keys():
                self.status.update_component_status(component_name, 'stopping')
            
            # Wait for threads to complete (with timeout)
            for component_name, thread in self.threads.items():
                thread.join(timeout=30)  # 30 second timeout
                if thread.is_alive():
                    self.logger.warning(f"⚠️ {component_name} enhanced thread did not stop gracefully")
            
            self.logger.info("✅ Enhanced system shutdown complete")
            
        except Exception as e:
            self.logger.error(f"❌ Error during enhanced shutdown: {e}")

# ===== ENHANCED COMMAND LINE INTERFACE =====
def parse_enhanced_command_line():
    """Parse enhanced command line arguments"""
    import argparse
    
    parser = argparse.ArgumentParser(description='Enhanced Unified Trading System (Phase 3)')
    parser.add_argument('--config', help='Configuration file path')
    parser.add_argument('--debug', action='store_true', help='Enable debug mode')
    parser.add_argument('--dry-run', action='store_true', help='Run in simulation mode')
    parser.add_argument('--test-intervals', action='store_true', help='Use reduced intervals for testing')
    parser.add_argument('--no-data-integration', action='store_true', help='Disable data integration features')
    parser.add_argument('--trading-only', action='store_true', help='Run only the enhanced trading engine')
    parser.add_argument('--data-only', action='store_true', help='Run only the data collection system')
    parser.add_argument('--phase', type=int, choices=[1, 2, 3], default=3, help='System phase to run')
    
    return parser.parse_args()

# ===== ENHANCED MAIN EXECUTION =====
def main():
    """Enhanced main entry point for Phase 3"""
    try:
        print("🚀 Starting Enhanced Unified Trading System (Phase 3)...")
        
        # Parse enhanced command line arguments
        args = parse_enhanced_command_line()
        
        # Handle special run modes
        if args.trading_only:
            return run_trading_only_mode(args)
        
        if args.data_only:
            return run_data_only_mode(args)
        
        # Initialize enhanced system manager
        system_manager = EnhancedTradingSystemManager()
        
        # Apply command line overrides
        if args.debug:
            system_manager.config.update('system.debug_mode', True)
            system_manager.config.update('logging.level', 'DEBUG')
        
        if args.dry_run:
            system_manager.config.update('testing.dry_run_mode', True)
            system_manager.config.update('trading.enabled', False)
        
        if args.test_intervals:
            system_manager.config.update('testing.reduce_intervals', True)
        
        if args.no_data_integration:
            system_manager.config.update('data_integration.enabled', False)
            system_manager.config.update('trading.enhanced_engine', False)
        
        if args.phase < 3:
            system_manager.config.update('system.phase', args.phase)
            system_manager.config.update('data_integration.enabled', False)
            system_manager.config.update('trading.enhanced_engine', False)
        
        # Initialize enhanced system
        if not system_manager.initialize_system():
            print("❌ Enhanced system initialization failed")
            return 1
        
        # Start all enhanced components
        if not system_manager.start_system():
            print("❌ Enhanced system startup failed")
            return 1
        
        print("\n✅ Enhanced system started successfully!")
        print("📱 Use Ctrl+C to stop the system")
        print("📊 Check logs/ directory for detailed logs")
        print("⚙️ Check config/ directory for configuration files")
        print("📁 Check data/ directory for market data")
        print("🧠 Enhanced trading intelligence ACTIVE")
        
        # Run enhanced monitoring loop
        system_manager.monitor_enhanced_system()
        
        return 0
        
    except KeyboardInterrupt:
        print("\n🛑 Enhanced system stopped by user")
        return 0
    
    except Exception as e:
        print(f"\n❌ Fatal enhanced system error: {e}")
        import traceback
        traceback.print_exc()
        return 1

def run_trading_only_mode(args):
    """Run only the enhanced trading engine"""
    try:
        print("🎯 Starting Trading-Only Mode...")
        
        # Import and run enhanced trading engine directly
        try:
            from trading_engine import run_enhanced_robot
            print("✅ Enhanced trading engine loaded")
            run_enhanced_robot()
        except ImportError:
            print("⚠️ Enhanced trading engine not found, using fallback")
            try:
                from trading_engine import run_simplified_robot
                run_simplified_robot()
            except ImportError:
                print("❌ No trading engine found")
                return 1
        
        return 0
        
    except KeyboardInterrupt:
        print("\n🛑 Trading-only mode stopped by user")
        return 0
    except Exception as e:
        print(f"❌ Error in trading-only mode: {e}")
        return 1

def run_data_only_mode(args):
    """Run only the data collection system"""
    try:
        print("📊 Starting Data-Only Mode...")
        
        # Initialize minimal system for data collection
        config = EnhancedConfigManager()
        loggers = setup_logging()
        
        try:
            from data_manager import DataManager
            data_manager = DataManager(config, loggers['data'])
            print("✅ Data manager loaded")
            
            print("🔄 Starting data collection...")
            data_manager.run()
            
        except ImportError:
            print("❌ Data manager not found")
            return 1
        
        return 0
        
    except KeyboardInterrupt:
        print("\n🛑 Data-only mode stopped by user")
        return 0
    except Exception as e:
        print(f"❌ Error in data-only mode: {e}")
        return 1

# ===== ENHANCED UTILITY FUNCTIONS =====
def check_system_prerequisites():
    """Check if all prerequisites are met for Phase 3"""
    try:
        print("🔍 Checking Phase 3 prerequisites...")
        
        # Check required directories
        required_dirs = ['config', 'core', 'scrapers', 'data', 'logs']
        for dir_name in required_dirs:
            if not Path(dir_name).exists():
                print(f"❌ Missing directory: {dir_name}")
                return False
            else:
                print(f"✅ Directory found: {dir_name}")
        
        # Check critical files
        critical_files = [
            'core/trading_engine.py',
            'core/data_manager.py',
            'config/settings.json'
        ]
        
        for file_path in critical_files:
            if not Path(file_path).exists():
                print(f"❌ Missing file: {file_path}")
                return False
            else:
                print(f"✅ File found: {file_path}")
        
        # Check data directory
        data_dir = Path("data")
        market_data_file = data_dir / "market_data.json"
        
        if market_data_file.exists():
            print("✅ Market data file exists")
            try:
                with open(market_data_file, 'r') as f:
                    data = json.load(f)
                    if 'data_sources' in data:
                        print("✅ Market data structure valid")
                    else:
                        print("⚠️ Market data structure incomplete")
            except Exception as e:
                print(f"⚠️ Market data file error: {e}")
        else:
            print("⚠️ Market data file not found (will be created)")
        
        print("✅ Prerequisites check complete")
        return True
        
    except Exception as e:
        print(f"❌ Error checking prerequisites: {e}")
        return False

def display_phase3_banner():
    """Display Phase 3 startup banner"""
    banner = """
╔══════════════════════════════════════════════════════════════════╗
║                                                                  ║
║          🤖 ENHANCED UNIFIED TRADING SYSTEM - PHASE 3             ║
║                                                                  ║
║  ✅ Data Collection System (Phase 2)                             ║
║  🧠 Enhanced Trading Intelligence (Phase 3)                      ║
║  📊 Real-time Market Data Integration                             ║
║  🎯 Sentiment-Based Direction Blocking                           ║
║  🔗 Correlation Risk Management                                   ║
║  📅 Economic Event Timing                                         ║
║  ⚖️ Dynamic Position Sizing                                       ║
║                                                                  ║
║  Ready for Phase 4: Telegram Bot & Remote Control               ║
║                                                                  ║
╚══════════════════════════════════════════════════════════════════╝
"""
    print(banner)

if __name__ == "__main__":
    # Display banner
    display_phase3_banner()
    
    # Check prerequisites
    if not check_system_prerequisites():
        print("\n❌ Prerequisites not met. Please run setup.py first.")
        sys.exit(1)
    
    # Run enhanced main
    sys.exit(main())