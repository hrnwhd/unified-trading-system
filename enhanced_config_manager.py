#!/usr/bin/env python3
# ===== ENHANCED CONFIGURATION MANAGER =====
# Manages all configuration switches and settings for Phase 3

import json
import logging
from pathlib import Path
from datetime import datetime
from typing import Dict, Any, Optional

logger = logging.getLogger(__name__)

class EnhancedConfigManager:
    """Manages enhanced trading system configuration with dynamic updates"""
    
    def __init__(self, config_file="enhanced_config.json"):
        self.config_file = Path(config_file)
        self.config = self._load_default_config()
        self.load_config()
        
    def _load_default_config(self) -> Dict[str, Any]:
        """Load default configuration"""
        return {
            "system": {
                "name": "Enhanced Trading System - Phase 3",
                "version": "3.0",
                "environment": "production",
                "debug_mode": False,
                "created": datetime.now().isoformat()
            },
            
            # ===== MASTER SWITCHES =====
            "master_switches": {
                "enhanced_features_enabled": True,
                "trading_enabled": True,
                "martingale_enabled": True,
                "preserve_existing_batches": True,  # Critical: Never interrupt running batches
                "emergency_data_fallback": True
            },
            
            # ===== INTELLIGENCE FEATURES =====
            "intelligence": {
                "sentiment_blocking": {
                    "enabled": True,
                    "extreme_threshold": 70,  # Block if sentiment > 70%
                    "confidence_boost_enabled": True,
                    "fallback_on_error": True
                },
                "correlation_risk": {
                    "enabled": True,
                    "high_correlation_threshold": 70,
                    "risk_reduction_factor": 0.8,  # 20% risk reduction
                    "warning_threshold": 3  # Warn if 3+ high correlations
                },
                "economic_timing": {
                    "enabled": True,
                    "buffer_hours": 1,  # Avoid trades 1h before events
                    "high_impact_only": True,
                    "risk_reduction_factor": 0.7  # 30% risk reduction
                },
                "dynamic_position_sizing": {
                    "enabled": True,
                    "confidence_based": True,
                    "market_condition_based": True,
                    "correlation_based": True
                },
                "cot_analysis": {
                    "enabled": False,  # Optional for future
                    "commercial_threshold": 60,
                    "speculative_threshold": 40
                }
            },
            
            # ===== RISK MANAGEMENT =====
            "risk_management": {
                "master_risk_level": 100,  # 100% = normal, 50% = conservative, 150% = aggressive
                "ta_weight": 70,  # Technical Analysis weight
                "data_weight": 30,  # External Data weight
                "preserve_original_ta": True,  # Always true - never override proven TA
                "enhanced_risk_enabled": True,
                "correlation_adjustment": True,
                "sentiment_adjustment": True,
                "economic_adjustment": True,
                "emergency_dd_percentage": 50,
                "max_drawdown_stop": True
            },
            
            # ===== MARTINGALE PROTECTION =====
            "martingale_protection": {
                "protect_existing_batches": True,  # Critical switch
                "intelligence_bypass_layer": 3,  # Bypass intelligence checks after layer 3
                "preserve_original_logic": True,  # Use proven martingale system
                "adaptive_tp_enabled": True,
                "emergency_exit_enabled": True,
                "max_layers": 15,
                "multiplier": 2.0,
                "profit_buffer_pips": 5
            },
            
            # ===== DATA SOURCES =====
            "data_sources": {
                "sentiment": {
                    "enabled": True,
                    "file_path": "sentiment_signals.json",
                    "freshness_limit_minutes": 60,
                    "fallback_mode": "allow_all_directions"
                },
                "correlation": {
                    "enabled": True,
                    "file_path": "correlation_data.json",
                    "freshness_limit_minutes": 60,
                    "fallback_mode": "no_warnings"
                },
                "economic_calendar": {
                    "enabled": True,
                    "file_path": "data/market_data.json",
                    "freshness_limit_minutes": 120,
                    "fallback_mode": "no_events"
                },
                "cot": {
                    "enabled": False,
                    "file_path": "cot_consolidated_data.json",
                    "freshness_limit_minutes": 10080  # 1 week
                }
            },
            
            # ===== TRADING PARAMETERS =====
            "trading": {
                "pairs": [
                    "AUDUSD", "USDCAD", "XAUUSD", "EURUSD", "GBPUSD",
                    "AUDCAD", "USDCHF", "GBPCAD", "AUDNZD", "NZDCAD",
                    "US500", "BTCUSD"
                ],
                "timeframe": "M5",
                "max_positions_per_pair": 1,
                "max_concurrent_trades": 20,
                "magic_number": 50515253,
                "account_number": 42903786
            },
            
            # ===== PAIR CONFIGURATIONS =====
            "pair_configs": {
                "AUDUSD": {"risk_profile": "Medium", "enabled": True},
                "USDCAD": {"risk_profile": "Low", "enabled": True},
                "XAUUSD": {"risk_profile": "High", "enabled": True},
                "EURUSD": {"risk_profile": "Medium", "enabled": True},
                "GBPUSD": {"risk_profile": "Medium", "enabled": True},
                "AUDCAD": {"risk_profile": "Low", "enabled": True},
                "USDCHF": {"risk_profile": "High", "enabled": True},
                "GBPCAD": {"risk_profile": "Low", "enabled": True},
                "AUDNZD": {"risk_profile": "Medium", "enabled": True},
                "NZDCAD": {"risk_profile": "Low", "enabled": True},
                "US500": {"risk_profile": "High", "enabled": True},
                "BTCUSD": {"risk_profile": "Medium", "enabled": True}
            },
            
            # ===== RISK PROFILES =====
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
            },
            
            # ===== LOGGING AND MONITORING =====
            "logging": {
                "level": "INFO",
                "max_file_size_mb": 50,
                "backup_count": 5,
                "console_output": True,
                "enhanced_logging": True,
                "decision_logging": True,
                "performance_logging": True
            },
            
            # ===== SCHEDULES =====
            "schedules": {
                "analysis_interval_minutes": 5,
                "position_check_interval_seconds": 30,
                "risk_check_interval_seconds": 60,
                "status_update_interval_minutes": 10,
                "data_freshness_check_minutes": 5
            }
        }
    
    def load_config(self) -> bool:
        """Load configuration from file"""
        try:
            if self.config_file.exists():
                with open(self.config_file, 'r') as f:
                    saved_config = json.load(f)
                
                # Merge with defaults (preserves new settings)
                self._merge_configs(self.config, saved_config)
                logger.info(f"✅ Configuration loaded from {self.config_file}")
                return True
            else:
                logger.info("📄 No config file found, using defaults")
                self.save_config()  # Create default config file
                return True
                
        except Exception as e:
            logger.error(f"❌ Error loading config: {e}")
            return False
    
    def save_config(self) -> bool:
        """Save current configuration to file"""
        try:
            # Update timestamp
            self.config["system"]["last_updated"] = datetime.now().isoformat()
            
            # Create backup
            if self.config_file.exists():
                backup_file = self.config_file.with_suffix('.json.backup')
                import shutil
                shutil.copy2(self.config_file, backup_file)
            
            # Save config
            with open(self.config_file, 'w') as f:
                json.dump(self.config, f, indent=2, default=str)
            
            logger.info(f"💾 Configuration saved to {self.config_file}")
            return True
            
        except Exception as e:
            logger.error(f"❌ Error saving config: {e}")
            return False
    
    def _merge_configs(self, default: Dict, saved: Dict) -> None:
        """Recursively merge configurations, preserving new defaults"""
        for key, value in saved.items():
            if key in default:
                if isinstance(value, dict) and isinstance(default[key], dict):
                    self._merge_configs(default[key], value)
                else:
                    default[key] = value
            else:
                # Add new key from saved config
                default[key] = value
    
    def get(self, key: str, default: Any = None) -> Any:
        """Get configuration value using dot notation (e.g., 'intelligence.sentiment_blocking.enabled')"""
        try:
            keys = key.split('.')
            value = self.config
            
            for k in keys:
                if isinstance(value, dict) and k in value:
                    value = value[k]
                else:
                    return default
            
            return value
        except Exception:
            return default
    
    def set(self, key: str, value: Any, save: bool = True) -> bool:
        """Set configuration value using dot notation"""
        try:
            keys = key.split('.')
            config_ref = self.config
            
            # Navigate to parent
            for k in keys[:-1]:
                if k not in config_ref:
                    config_ref[k] = {}
                elif not isinstance(config_ref[k], dict):
                    logger.error(f"❌ Cannot set {key}: {k} is not a dict")
                    return False
                config_ref = config_ref[k]
            
            # Set value
            old_value = config_ref.get(keys[-1])
            config_ref[keys[-1]] = value
            
            logger.info(f"🔧 Config updated: {key} = {value} (was {old_value})")
            
            if save:
                return self.save_config()
            return True
            
        except Exception as e:
            logger.error(f"❌ Error setting {key}: {e}")
            return False
    
    def is_enabled(self, feature_path: str) -> bool:
        """Check if a feature is enabled"""
        return bool(self.get(f"{feature_path}.enabled", False))
    
    def enable_feature(self, feature_path: str, save: bool = True) -> bool:
        """Enable a feature"""
        return self.set(f"{feature_path}.enabled", True, save)
    
    def disable_feature(self, feature_path: str, save: bool = True) -> bool:
        """Disable a feature"""
        return self.set(f"{feature_path}.enabled", False, save)
    
    # ===== PRESET CONFIGURATIONS =====
    
    def set_pure_ta_mode(self) -> bool:
        """Set to pure technical analysis mode"""
        logger.info("🔧 Setting Pure TA Mode...")
        
        updates = [
            ("master_switches.enhanced_features_enabled", False),
            ("intelligence.sentiment_blocking.enabled", False),
            ("intelligence.correlation_risk.enabled", False),
            ("intelligence.economic_timing.enabled", False),
            ("intelligence.dynamic_position_sizing.enabled", False),
            ("risk_management.ta_weight", 100),
            ("risk_management.data_weight", 0)
        ]
        
        success = True
        for key, value in updates:
            if not self.set(key, value, save=False):
                success = False
        
        if success:
            success = self.save_config()
            if success:
                logger.info("✅ Pure TA Mode activated - All intelligence disabled")
        
        return success
    
    def set_full_intelligence_mode(self) -> bool:
        """Enable all intelligence features"""
        logger.info("🧠 Setting Full Intelligence Mode...")
        
        updates = [
            ("master_switches.enhanced_features_enabled", True),
            ("intelligence.sentiment_blocking.enabled", True),
            ("intelligence.correlation_risk.enabled", True),
            ("intelligence.economic_timing.enabled", True),
            ("intelligence.dynamic_position_sizing.enabled", True),
            ("risk_management.ta_weight", 70),
            ("risk_management.data_weight", 30)
        ]
        
        success = True
        for key, value in updates:
            if not self.set(key, value, save=False):
                success = False
        
        if success:
            success = self.save_config()
            if success:
                logger.info("✅ Full Intelligence Mode activated")
        
        return success
    
    def set_conservative_mode(self) -> bool:
        """Set conservative risk settings"""
        logger.info("🛡️ Setting Conservative Mode...")
        
        updates = [
            ("risk_management.master_risk_level", 50),  # Half risk
            ("intelligence.sentiment_blocking.extreme_threshold", 60),  # Lower threshold
            ("intelligence.correlation_risk.risk_reduction_factor", 0.6),  # More reduction
            ("intelligence.economic_timing.risk_reduction_factor", 0.5),  # More reduction
            ("martingale_protection.intelligence_bypass_layer", 5)  # Higher bypass layer
        ]
        
        success = True
        for key, value in updates:
            if not self.set(key, value, save=False):
                success = False
        
        if success:
            success = self.save_config()
            if success:
                logger.info("✅ Conservative Mode activated - Reduced risk settings")
        
        return success
    
    def set_aggressive_mode(self) -> bool:
        """Set aggressive trading settings with higher risk"""
        logger.info("⚡ Setting Aggressive Mode...")
        
        updates = [
            ("risk_management.master_risk_level", 150),  # 1.5x risk
            ("intelligence.sentiment_blocking.extreme_threshold", 80),  # Higher threshold
            ("intelligence.correlation_risk.risk_reduction_factor", 0.9),  # Less reduction
            ("intelligence.economic_timing.risk_reduction_factor", 0.8),  # Less reduction
            ("martingale_protection.intelligence_bypass_layer", 2)  # Lower bypass layer
        ]
        
        success = True
        for key, value in updates:
            if not self.set(key, value, save=False):
                success = False
        
        if success:
            success = self.save_config()
            if success:
                logger.info("✅ Aggressive Mode activated - Increased risk settings")
        
        return success
    
    def set_martingale_protection_mode(self) -> bool:
        """Maximum protection for existing martingale batches"""
        logger.info("🔒 Setting Martingale Protection Mode...")
        
        updates = [
            ("martingale_protection.protect_existing_batches", True),
            ("martingale_protection.intelligence_bypass_layer", 2),  # Bypass after layer 2
            ("martingale_protection.preserve_original_logic", True),
            ("master_switches.preserve_existing_batches", True),
            ("intelligence.sentiment_blocking.enabled", False),  # Disable for existing batches
            ("intelligence.economic_timing.enabled", False)  # Disable for existing batches
        ]
        
        success = True
        for key, value in updates:
            if not self.set(key, value, save=False):
                success = False
        
        if success:
            success = self.save_config()
            if success:
                logger.info("✅ Martingale Protection Mode - Existing batches fully protected")
        
        return success
    
    # ===== VALIDATION AND UTILITIES =====
    
    def validate_config(self) -> Dict[str, list]:
        """Validate configuration and return issues"""
        issues = {
            'errors': [],
            'warnings': [],
            'info': []
        }
        
        try:
            # Check critical paths
            required_keys = [
                'master_switches.enhanced_features_enabled',
                'master_switches.trading_enabled',
                'risk_management.master_risk_level',
                'trading.pairs',
                'trading.magic_number'
            ]
            
            for key in required_keys:
                if self.get(key) is None:
                    issues['errors'].append(f"Missing required config: {key}")
            
            # Validate weights
            ta_weight = self.get('risk_management.ta_weight', 0)
            data_weight = self.get('risk_management.data_weight', 0)
            
            if ta_weight + data_weight != 100:
                issues['errors'].append(f"TA weight ({ta_weight}) + Data weight ({data_weight}) must equal 100")
            
            # Check risk level
            risk_level = self.get('risk_management.master_risk_level', 100)
            if not 0 <= risk_level <= 200:
                issues['warnings'].append(f"Master risk level ({risk_level}%) outside recommended range (0-200%)")
            
            # Check file paths
            data_files = [
                ('data_sources.sentiment.file_path', 'Sentiment file'),
                ('data_sources.correlation.file_path', 'Correlation file'),
                ('data_sources.economic_calendar.file_path', 'Economic calendar file')
            ]
            
            for path_key, description in data_files:
                file_path = self.get(path_key)
                if file_path and not Path(file_path).exists():
                    issues['warnings'].append(f"{description} not found: {file_path}")
            
            # Check enabled features vs available data
            if self.get('intelligence.sentiment_blocking.enabled') and not Path(self.get('data_sources.sentiment.file_path', '')).exists():
                issues['warnings'].append("Sentiment blocking enabled but sentiment file missing")
            
            if self.get('intelligence.correlation_risk.enabled') and not Path(self.get('data_sources.correlation.file_path', '')).exists():
                issues['warnings'].append("Correlation risk enabled but correlation file missing")
            
            # Info messages
            if not self.get('master_switches.enhanced_features_enabled'):
                issues['info'].append("Enhanced features disabled - Running in Pure TA mode")
            
            if self.get('martingale_protection.protect_existing_batches'):
                issues['info'].append("Martingale protection active - Existing batches protected")
            
        except Exception as e:
            issues['errors'].append(f"Validation error: {e}")
        
        return issues
    
    def get_status_summary(self) -> Dict[str, Any]:
        """Get comprehensive configuration status"""
        try:
            validation = self.validate_config()
            
            return {
                'system': {
                    'name': self.get('system.name'),
                    'version': self.get('system.version'),
                    'environment': self.get('system.environment'),
                    'last_updated': self.get('system.last_updated')
                },
                'features': {
                    'enhanced_features': self.get('master_switches.enhanced_features_enabled'),
                    'sentiment_blocking': self.get('intelligence.sentiment_blocking.enabled'),
                    'correlation_risk': self.get('intelligence.correlation_risk.enabled'),
                    'economic_timing': self.get('intelligence.economic_timing.enabled'),
                    'dynamic_sizing': self.get('intelligence.dynamic_position_sizing.enabled')
                },
                'risk_settings': {
                    'master_risk_level': self.get('risk_management.master_risk_level'),
                    'ta_weight': self.get('risk_management.ta_weight'),
                    'data_weight': self.get('risk_management.data_weight'),
                    'martingale_protection': self.get('martingale_protection.protect_existing_batches')
                },
                'trading': {
                    'pairs_count': len(self.get('trading.pairs', [])),
                    'enabled_pairs': len([p for p, c in self.get('pair_configs', {}).items() if c.get('enabled', True)]),
                    'magic_number': self.get('trading.magic_number'),
                    'timeframe': self.get('trading.timeframe')
                },
                'validation': {
                    'errors': len(validation['errors']),
                    'warnings': len(validation['warnings']),
                    'status': 'valid' if not validation['errors'] else 'invalid'
                }
            }
            
        except Exception as e:
            return {'error': str(e)}
    
    def export_config(self, export_path: Optional[str] = None) -> bool:
        """Export configuration to specified path"""
        try:
            if export_path is None:
                timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
                export_path = f"enhanced_config_export_{timestamp}.json"
            
            export_data = {
                'exported_at': datetime.now().isoformat(),
                'system_info': self.get_status_summary(),
                'configuration': self.config
            }
            
            with open(export_path, 'w') as f:
                json.dump(export_data, f, indent=2, default=str)
            
            logger.info(f"✅ Configuration exported to {export_path}")
            return True
            
        except Exception as e:
            logger.error(f"❌ Export failed: {e}")
            return False
    
    def import_config(self, import_path: str, merge: bool = True) -> bool:
        """Import configuration from file"""
        try:
            import_file = Path(import_path)
            if not import_file.exists():
                logger.error(f"❌ Import file not found: {import_path}")
                return False
            
            with open(import_file, 'r') as f:
                import_data = json.load(f)
            
            # Extract configuration
            if 'configuration' in import_data:
                imported_config = import_data['configuration']
            else:
                imported_config = import_data
            
            if merge:
                # Merge with current config
                self._merge_configs(self.config, imported_config)
                logger.info(f"✅ Configuration merged from {import_path}")
            else:
                # Replace entire config
                self.config = imported_config
                logger.info(f"✅ Configuration replaced from {import_path}")
            
            return self.save_config()
            
        except Exception as e:
            logger.error(f"❌ Import failed: {e}")
            return False

# ===== TESTING AND CLI INTERFACE =====
def main():
    """CLI interface for configuration management"""
    import sys
    
    config_manager = EnhancedConfigManager()
    
    if len(sys.argv) < 2:
        print("Enhanced Configuration Manager")
        print("Usage:")
        print("  python enhanced_config_manager.py status     - Show configuration status")
        print("  python enhanced_config_manager.py validate   - Validate configuration")
        print("  python enhanced_config_manager.py pure_ta    - Set Pure TA mode")
        print("  python enhanced_config_manager.py full_intel - Set Full Intelligence mode")
        print("  python enhanced_config_manager.py conservative - Set Conservative mode")
        print("  python enhanced_config_manager.py aggressive - Set Aggressive mode")
        print("  python enhanced_config_manager.py protect    - Set Martingale Protection mode")
        print("  python enhanced_config_manager.py export [path] - Export configuration")
        print("  python enhanced_config_manager.py import <path> - Import configuration")
        print("  python enhanced_config_manager.py get <key>  - Get configuration value")
        print("  python enhanced_config_manager.py set <key> <value> - Set configuration value")
        return
    
    command = sys.argv[1].lower()
    
    try:
        if command == 'status':
            status = config_manager.get_status_summary()
            print(json.dumps(status, indent=2))
            
        elif command == 'validate':
            validation = config_manager.validate_config()
            print("Configuration Validation:")
            if validation['errors']:
                print("ERRORS:")
                for error in validation['errors']:
                    print(f"  ❌ {error}")
            if validation['warnings']:
                print("WARNINGS:")
                for warning in validation['warnings']:
                    print(f"  ⚠️ {warning}")
            if validation['info']:
                print("INFO:")
                for info in validation['info']:
                    print(f"  ℹ️ {info}")
            
            if not validation['errors']:
                print("✅ Configuration is valid")
            
        elif command == 'pure_ta':
            if config_manager.set_pure_ta_mode():
                print("✅ Pure TA mode activated")
            else:
                print("❌ Failed to set Pure TA mode")
                
        elif command == 'full_intel':
            if config_manager.set_full_intelligence_mode():
                print("✅ Full Intelligence mode activated")
            else:
                print("❌ Failed to set Full Intelligence mode")
                
        elif command == 'conservative':
            if config_manager.set_conservative_mode():
                print("✅ Conservative mode activated")
            else:
                print("❌ Failed to set Conservative mode")
                
        elif command == 'aggressive':
            if config_manager.set_aggressive_mode():
                print("✅ Aggressive mode activated")
            else:
                print("❌ Failed to set Aggressive mode")
                
        elif command == 'protect':
            if config_manager.set_martingale_protection_mode():
                print("✅ Martingale Protection mode activated")
            else:
                print("❌ Failed to set Martingale Protection mode")
                
        elif command == 'export':
            export_path = sys.argv[2] if len(sys.argv) > 2 else None
            if config_manager.export_config(export_path):
                print(f"✅ Configuration exported")
            else:
                print("❌ Export failed")
                
        elif command == 'import':
            if len(sys.argv) < 3:
                print("❌ Import path required")
                return
            
            import_path = sys.argv[2]
            merge = len(sys.argv) < 4 or sys.argv[3].lower() != 'replace'
            
            if config_manager.import_config(import_path, merge):
                print(f"✅ Configuration {'merged' if merge else 'replaced'}")
            else:
                print("❌ Import failed")
                
        elif command == 'get':
            if len(sys.argv) < 3:
                print("❌ Configuration key required")
                return
            
            key = sys.argv[2]
            value = config_manager.get(key)
            print(f"{key} = {value}")
            
        elif command == 'set':
            if len(sys.argv) < 4:
                print("❌ Configuration key and value required")
                return
            
            key = sys.argv[2]
            value = sys.argv[3]
            
            # Try to parse value as JSON
            try:
                import json
                parsed_value = json.loads(value)
            except:
                # Use as string if not valid JSON
                parsed_value = value
            
            if config_manager.set(key, parsed_value):
                print(f"✅ {key} = {parsed_value}")
            else:
                print(f"❌ Failed to set {key}")
                
        else:
            print(f"❌ Unknown command: {command}")
            
    except Exception as e:
        print(f"❌ Error: {e}")

if __name__ == "__main__":
    main()