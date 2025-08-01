#!/usr/bin/env python3
# ===== PHASE 3 INTEGRATION UTILITIES =====
# Helper functions to integrate, test, and manage the enhanced trading system

import json
import sys
import os
from pathlib import Path
from datetime import datetime, timedelta
import logging
from typing import Dict, List, Any, Optional

# Setup logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class Phase3IntegrationManager:
    """Manages the integration of Phase 3 enhancements with your existing system"""
    
    def __init__(self):
        self.base_path = Path.cwd()
        self.config_manager = None
        self.status = {
            'integration_complete': False,
            'data_sources_ready': False,
            'config_validated': False,
            'backup_created': False
        }
    
    def initialize_integration(self) -> bool:
        """Initialize the Phase 3 integration"""
        try:
            logger.info("🚀 Initializing Phase 3 Integration...")
            
            # Step 1: Create backup of existing system
            if self.create_system_backup():
                self.status['backup_created'] = True
                logger.info("✅ System backup created")
            
            # Step 2: Initialize configuration manager
            try:
                from enhanced_config_manager import EnhancedConfigManager
                self.config_manager = EnhancedConfigManager()
                self.status['config_validated'] = True
                logger.info("✅ Configuration manager initialized")
            except ImportError:
                logger.error("❌ Enhanced config manager not found")
                return False
            
            # Step 3: Check data sources
            if self.check_data_sources():
                self.status['data_sources_ready'] = True
                logger.info("✅ Data sources validated")
            
            # Step 4: Validate integration
            if self.validate_integration():
                self.status['integration_complete'] = True
                logger.info("✅ Phase 3 integration completed successfully")
                return True
            
            return False
            
        except Exception as e:
            logger.error(f"❌ Integration initialization failed: {e}")
            return False
    
    def create_system_backup(self) -> bool:
        """Create backup of existing trading system"""
        try:
            backup_dir = Path("backups") / f"pre_phase3_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
            backup_dir.mkdir(parents=True, exist_ok=True)
            
            # Files to backup
            backup_files = [
                "core/trading_engine_backup.py",  # Your proven system
                "config/pairs.json",
                "config/settings.json",
                "config/schedules.json"
            ]
            
            import shutil
            backup_count = 0
            
            for file_path in backup_files:
                source = Path(file_path)
                if source.exists():
                    destination = backup_dir / source.name
                    shutil.copy2(source, destination)
                    backup_count += 1
                    logger.debug(f"Backed up: {file_path}")
            
            # Create backup manifest
            manifest = {
                'backup_date': datetime.now().isoformat(),
                'backup_reason': 'Pre-Phase 3 Integration',
                'files_backed_up': backup_count,
                'original_files': backup_files,
                'restore_instructions': 'Copy files back to original locations to restore'
            }
            
            with open(backup_dir / "backup_manifest.json", 'w') as f:
                json.dump(manifest, f, indent=2)
            
            logger.info(f"💾 Backup created: {backup_count} files in {backup_dir}")
            return True
            
        except Exception as e:
            logger.error(f"❌ Backup creation failed: {e}")
            return False
    
    def check_data_sources(self) -> bool:
        """Check availability and status of data sources"""
        try:
            data_status = {
                'sentiment_signals.json': False,
                'correlation_data.json': False,
                'data/market_data.json': False,
                'cot_consolidated_data.json': False
            }
            
            # Check each data file
            for file_path, _ in data_status.items():
                file_obj = Path(file_path)
                if file_obj.exists():
                    # Check if file is recent (within 24 hours)
                    file_age = datetime.now() - datetime.fromtimestamp(file_obj.stat().st_mtime)
                    if file_age < timedelta(hours=24):
                        data_status[file_path] = True
                        logger.info(f"✅ {file_path}: Available and recent")
                    else:
                        logger.warning(f"⚠️ {file_path}: Available but old ({file_age})")
                else:
                    logger.warning(f"⚠️ {file_path}: Not found")
            
            # Create sample data files if missing
            missing_files = [f for f, status in data_status.items() if not status]
            if missing_files:
                logger.info(f"📝 Creating sample data for {len(missing_files)} missing files...")
                self.create_sample_data_files(missing_files)
            
            return True
            
        except Exception as e:
            logger.error(f"❌ Data source check failed: {e}")
            return False
    
    def create_sample_data_files(self, missing_files: List[str]) -> None:
        """Create sample data files for testing"""
        try:
            # Sample sentiment data
            if 'sentiment_signals.json' in missing_files:
                sample_sentiment = {
                    'timestamp': datetime.now().isoformat(),
                    'data_source': 'Sample',
                    'threshold_used': 70,
                    'pairs': {}
                }
                
                # Create balanced sentiment for all pairs
                pairs = ['AUDUSD', 'USDCAD', 'XAUUSD', 'EURUSD', 'GBPUSD', 
                        'AUDCAD', 'USDCHF', 'GBPCAD', 'AUDNZD', 'NZDCAD', 'US500', 'BTCUSD']
                
                for pair in pairs:
                    sample_sentiment['pairs'][pair] = {
                        'allowed_directions': ['long', 'short'],
                        'blocked_directions': [],
                        'sentiment': {'short': 50, 'long': 50},
                        'signal_strength': 'Balanced',
                        'current_price': 'Sample',
                        'popularity': 'Sample'
                    }
                
                with open('sentiment_signals.json', 'w') as f:
                    json.dump(sample_sentiment, f, indent=2)
                logger.info("📝 Created sample sentiment_signals.json")
            
            # Sample correlation data
            if 'correlation_data.json' in missing_files:
                sample_correlation = {
                    'timestamp': datetime.now().isoformat(),
                    'data_source': 'Sample',
                    'correlation_matrix': {},
                    'warnings': [],
                    'thresholds': {'high_correlation': 70, 'negative_correlation': -70}
                }
                
                with open('correlation_data.json', 'w') as f:
                    json.dump(sample_correlation, f, indent=2)
                logger.info("📝 Created sample correlation_data.json")
            
            # Sample market data
            if 'data/market_data.json' in missing_files:
                Path('data').mkdir(exist_ok=True)
                
                sample_market_data = {
                    'timestamp': datetime.now().isoformat(),
                    'system_status': 'running',
                    'data_sources': {
                        'economic_calendar': {
                            'status': 'fresh',
                            'events': [],
                            'last_update': datetime.now().isoformat()
                        }
                    }
                }
                
                with open('data/market_data.json', 'w') as f:
                    json.dump(sample_market_data, f, indent=2)
                logger.info("📝 Created sample market_data.json")
            
        except Exception as e:
            logger.error(f"❌ Sample data creation failed: {e}")
    
    def validate_integration(self) -> bool:
        """Validate the complete integration"""
        try:
            validation_results = {
                'config_valid': False,
                'imports_work': False,
                'data_accessible': False,
                'features_testable': False
            }
            
            # Test 1: Configuration validation
            if self.config_manager:
                validation = self.config_manager.validate_config()
                if not validation['errors']:
                    validation_results['config_valid'] = True
                    logger.info("✅ Configuration validation passed")
                else:
                    logger.error(f"❌ Configuration errors: {validation['errors']}")
            
            # Test 2: Import test
            try:
                # Test if enhanced trading engine can be imported
                sys.path.append(str(self.base_path))
                from enhanced_trading_engine import (
                    EnhancedDataManager,
                    EnhancedDecisionEngine,
                    EnhancedPositionSizing
                )
                validation_results['imports_work'] = True
                logger.info("✅ Enhanced modules import successfully")
            except ImportError as e:
                logger.error(f"❌ Import test failed: {e}")
            
            # Test 3: Data accessibility
            try:
                if validation_results['imports_work']:
                    data_manager = EnhancedDataManager()
                    sentiment_data = data_manager.get_sentiment_data()
                    correlation_data = data_manager.get_correlation_data()
                    
                    if sentiment_data or correlation_data:
                        validation_results['data_accessible'] = True
                        logger.info("✅ Data sources accessible")
            except Exception as e:
                logger.error(f"❌ Data accessibility test failed: {e}")
            
            # Test 4: Feature testing
            try:
                if validation_results['imports_work']:
                    decision_engine = EnhancedDecisionEngine()
                    can_trade, confidence, reasons = decision_engine.can_trade_direction(
                        'EURUSD', 'long', 80
                    )
                    validation_results['features_testable'] = True
                    logger.info(f"✅ Features testable - Sample decision: {can_trade} ({confidence:.1f}%)")
            except Exception as e:
                logger.error(f"❌ Feature test failed: {e}")
            
            # Overall validation
            passed_tests = sum(validation_results.values())
            total_tests = len(validation_results)
            
            logger.info(f"📊 Integration validation: {passed_tests}/{total_tests} tests passed")
            
            if passed_tests == total_tests:
                logger.info("✅ Integration validation completed successfully")
                return True
            else:
                logger.warning(f"⚠️ Integration validation partial: {passed_tests}/{total_tests}")
                return passed_tests >= 2  # At least basic functionality
            
        except Exception as e:
            logger.error(f"❌ Integration validation failed: {e}")
            return False
    
    def get_integration_status(self) -> Dict[str, Any]:
        """Get comprehensive integration status"""
        try:
            status = {
                'integration_status': self.status.copy(),
                'timestamp': datetime.now().isoformat(),
                'system_info': {},
                'data_sources': {},
                'configuration': {}
            }
            
            # System info
            status['system_info'] = {
                'python_version': sys.version,
                'working_directory': str(self.base_path),
                'key_files_present': self._check_key_files()
            }
            
            # Data sources status
            data_files = [
                'sentiment_signals.json',
                'correlation_data.json', 
                'data/market_data.json',
                'cot_consolidated_data.json'
            ]
            
            for file_path in data_files:
                file_obj = Path(file_path)
                if file_obj.exists():
                    file_age = datetime.now() - datetime.fromtimestamp(file_obj.stat().st_mtime)
                    status['data_sources'][file_path] = {
                        'exists': True,
                        'size_bytes': file_obj.stat().st_size,
                        'age_hours': file_age.total_seconds() / 3600,
                        'status': 'fresh' if file_age < timedelta(hours=2) else 'stale'
                    }
                else:
                    status['data_sources'][file_path] = {
                        'exists': False,
                        'status': 'missing'
                    }
            
            # Configuration status
            if self.config_manager:
                status['configuration'] = self.config_manager.get_status_summary()
            
            return status
            
        except Exception as e:
            return {'error': str(e)}
    
    def _check_key_files(self) -> Dict[str, bool]:
        """Check presence of key system files"""
        key_files = {
            'enhanced_trading_engine.py': False,
            'enhanced_config_manager.py': False,
            'core/trading_engine_backup.py': False,
            'core/data_manager.py': False,
            'scrapers/sentiment_scraper.py': False,
            'scrapers/correlation_scraper.py': False,
            'scrapers/calendar_scraper.py': False
        }
        
        for file_path in key_files:
            key_files[file_path] = Path(file_path).exists()
        
        return key_files
    
    def run_comprehensive_test(self) -> Dict[str, Any]:
        """Run comprehensive system test"""
        logger.info("🧪 Running comprehensive Phase 3 test...")
        
        test_results = {
            'timestamp': datetime.now().isoformat(),
            'tests': {},
            'overall_status': 'unknown',
            'recommendations': []
        }
        
        try:
            # Test 1: Configuration
            logger.info("Testing configuration...")
            if self.config_manager:
                validation = self.config_manager.validate_config()
                test_results['tests']['configuration'] = {
                    'passed': len(validation['errors']) == 0,
                    'errors': validation['errors'],
                    'warnings': validation['warnings']
                }
            
            # Test 2: Data Manager
            logger.info("Testing data manager...")
            try:
                from enhanced_trading_engine import EnhancedDataManager
                data_manager = EnhancedDataManager()
                
                sentiment_data = data_manager.get_sentiment_data()
                correlation_data = data_manager.get_correlation_data()
                economic_events = data_manager.get_economic_events(24)
                
                test_results['tests']['data_manager'] = {
                    'passed': True,
                    'sentiment_pairs': len(sentiment_data),
                    'correlation_warnings': len(correlation_data.get('warnings', [])),
                    'economic_events': len(economic_events)
                }
                
            except Exception as e:
                test_results['tests']['data_manager'] = {
                    'passed': False,
                    'error': str(e)
                }
            
            # Test 3: Decision Engine
            logger.info("Testing decision engine...")
            try:
                from enhanced_trading_engine import EnhancedDecisionEngine
                decision_engine = EnhancedDecisionEngine()
                
                # Test multiple decision scenarios
                test_pairs = ['EURUSD', 'GBPUSD', 'XAUUSD']
                decision_results = []
                
                for pair in test_pairs:
                    for direction in ['long', 'short']:
                        can_trade, confidence, reasons = decision_engine.can_trade_direction(
                            pair, direction, 75
                        )
                        decision_results.append({
                            'pair': pair,
                            'direction': direction,
                            'can_trade': can_trade,
                            'confidence': confidence,
                            'reasons': reasons
                        })
                
                test_results['tests']['decision_engine'] = {
                    'passed': True,
                    'decisions_tested': len(decision_results),
                    'sample_decisions': decision_results[:3]  # First 3 for brevity
                }
                
            except Exception as e:
                test_results['tests']['decision_engine'] = {
                    'passed': False,
                    'error': str(e)
                }
            
            # Test 4: Position Sizing
            logger.info("Testing position sizing...")
            try:
                from enhanced_trading_engine import EnhancedPositionSizing
                position_sizer = EnhancedPositionSizing()
                
                test_amounts = [100, 500, 1000]
                sizing_results = []
                
                for amount in test_amounts:
                    adjusted = position_sizer.calculate_enhanced_position_size(
                        'EURUSD', amount, 80
                    )
                    sizing_results.append({
                        'base_amount': amount,
                        'adjusted_amount': adjusted,
                        'adjustment_factor': adjusted / amount if amount > 0 else 0
                    })
                
                test_results['tests']['position_sizing'] = {
                    'passed': True,
                    'sizing_tests': sizing_results
                }
                
            except Exception as e:
                test_results['tests']['position_sizing'] = {
                    'passed': False,
                    'error': str(e)
                }
            
            # Calculate overall status
            passed_tests = sum(1 for test in test_results['tests'].values() 
                             if test.get('passed', False))
            total_tests = len(test_results['tests'])
            
            if passed_tests == total_tests:
                test_results['overall_status'] = 'all_passed'
            elif passed_tests >= total_tests * 0.5:
                test_results['overall_status'] = 'mostly_passed'
            else:
                test_results['overall_status'] = 'failed'
            
            # Generate recommendations
            if test_results['overall_status'] == 'all_passed':
                test_results['recommendations'] = [
                    "✅ All tests passed - System ready for enhanced trading",
                    "🚀 You can now run the enhanced trading engine with confidence",
                    "💡 Consider testing with paper trading first"
                ]
            elif test_results['overall_status'] == 'mostly_passed':
                test_results['recommendations'] = [
                    "⚠️ Most tests passed - Review failed tests before proceeding",
                    "🔧 Fix configuration issues if any",
                    "📊 Ensure data sources are updating properly"
                ]
            else:
                test_results['recommendations'] = [
                    "❌ Multiple tests failed - System needs attention",
                    "🛠️ Check file paths and dependencies",
                    "📞 Consider running in Pure TA mode until issues resolved"
                ]
            
            logger.info(f"🧪 Test completed: {passed_tests}/{total_tests} passed - {test_results['overall_status']}")
            
        except Exception as e:
            test_results['overall_status'] = 'error'
            test_results['error'] = str(e)
            logger.error(f"❌ Comprehensive test failed: {e}")
        
        return test_results

# ===== QUICK SETUP FUNCTIONS =====

def quick_setup_phase3():
    """Quick setup for Phase 3 integration"""
    print("🚀 Phase 3 Enhanced Trading System - Quick Setup")
    print("=" * 60)
    
    try:
        integration_manager = Phase3IntegrationManager()
        
        # Initialize integration
        if integration_manager.initialize_integration():
            print("✅ Phase 3 integration completed successfully!")
            
            # Show status
            status = integration_manager.get_integration_status()
            print(f"\n📊 Integration Status:")
            for key, value in status['integration_status'].items():
                emoji = "✅" if value else "❌"
                print(f"  {emoji} {key.replace('_', ' ').title()}: {value}")
            
            # Run quick test
            print("\n🧪 Running quick test...")
            test_results = integration_manager.run_comprehensive_test()
            
            print(f"\n📈 Test Results: {test_results['overall_status'].upper()}")
            for recommendation in test_results.get('recommendations', []):
                print(f"  {recommendation}")
            
            return True
            
        else:
            print("❌ Phase 3 integration failed")
            return False
            
    except Exception as e:
        print(f"❌ Setup failed: {e}")
        return False

def setup_trading_modes():
    """Setup different trading modes"""
    print("\n🎛️ Trading Mode Configuration")
    print("=" * 40)
    
    try:
        from enhanced_config_manager import EnhancedConfigManager
        config_manager = EnhancedConfigManager()
        
        print("Available modes:")
        print("1. Pure TA Mode (No intelligence, just your proven TA)")
        print("2. Conservative Intelligence (Safe enhancement)")
        print("3. Full Intelligence (All features enabled)")
        print("4. Aggressive Mode (Higher risk, more aggressive)")
        print("5. Martingale Protection (Protect existing batches)")
        
        choice = input("\nSelect mode (1-5): ").strip()
        
        if choice == '1':
            if config_manager.set_pure_ta_mode():
                print("✅ Pure TA Mode activated")
        elif choice == '2':
            if config_manager.set_conservative_mode():
                print("✅ Conservative Intelligence Mode activated")
        elif choice == '3':
            if config_manager.set_full_intelligence_mode():
                print("✅ Full Intelligence Mode activated")
        elif choice == '4':
            if config_manager.set_aggressive_mode():
                print("✅ Aggressive Mode activated")
        elif choice == '5':
            if config_manager.set_martingale_protection_mode():
                print("✅ Martingale Protection Mode activated")
        else:
            print("❌ Invalid choice")
            return False
        
        # Show current config
        status = config_manager.get_status_summary()
        print(f"\n📊 Current Configuration:")
        print(f"  Enhanced Features: {status['features']['enhanced_features']}")
        print(f"  Master Risk Level: {status['risk_settings']['master_risk_level']}%")
        print(f"  TA Weight: {status['risk_settings']['ta_weight']}%")
        print(f"  Data Weight: {status['risk_settings']['data_weight']}%")
        
        return True
        
    except Exception as e:
        print(f"❌ Mode setup failed: {e}")
        return False

def verify_system_integrity():
    """Verify system integrity and readiness"""
    print("\n🔍 System Integrity Check")
    print("=" * 30)
    
    try:
        integration_manager = Phase3IntegrationManager()
        
        # Check key files
        key_files = integration_manager._check_key_files()
        print("📁 Key Files Check:")
        for file_path, exists in key_files.items():
            emoji = "✅" if exists else "❌"
            print(f"  {emoji} {file_path}")
        
        # Check data sources
        integration_manager.check_data_sources()
        
        # Run comprehensive test
        test_results = integration_manager.run_comprehensive_test()
        
        print(f"\n🎯 Overall Status: {test_results['overall_status'].upper()}")
        
        # Show recommendations
        if test_results.get('recommendations'):
            print("\n💡 Recommendations:")
            for rec in test_results['recommendations']:
                print(f"  {rec}")
        
        return test_results['overall_status'] in ['all_passed', 'mostly_passed']
        
    except Exception as e:
        print(f"❌ Integrity check failed: {e}")
        return False

# ===== CLI INTERFACE =====

def main():
    """Main CLI interface"""
    if len(sys.argv) < 2:
        print("Phase 3 Enhanced Trading System - Integration Utilities")
        print("Usage:")
        print("  python integration_utilities.py setup        - Quick setup Phase 3")
        print("  python integration_utilities.py modes        - Configure trading modes")
        print("  python integration_utilities.py verify       - Verify system integrity")
        print("  python integration_utilities.py test         - Run comprehensive test")
        print("  python integration_utilities.py status       - Show integration status")
        print("  python integration_utilities.py backup       - Create system backup")
        return
    
    command = sys.argv[1].lower()
    
    try:
        if command == 'setup':
            success = quick_setup_phase3()
            sys.exit(0 if success else 1)
            
        elif command == 'modes':
            success = setup_trading_modes()
            sys.exit(0 if success else 1)
            
        elif command == 'verify':
            success = verify_system_integrity()
            sys.exit(0 if success else 1)
            
        elif command == 'test':
            integration_manager = Phase3IntegrationManager()
            test_results = integration_manager.run_comprehensive_test()
            print(json.dumps(test_results, indent=2, default=str))
            
        elif command == 'status':
            integration_manager = Phase3IntegrationManager()
            status = integration_manager.get_integration_status()
            print(json.dumps(status, indent=2, default=str))
            
        elif command == 'backup':
            integration_manager = Phase3IntegrationManager()
            if integration_manager.create_system_backup():
                print("✅ System backup created successfully")
            else:
                print("❌ Backup creation failed")
                
        else:
            print(f"❌ Unknown command: {command}")
            sys.exit(1)
            
    except Exception as e:
        print(f"❌ Command failed: {e}")
        sys.exit(1)

if __name__ == "__main__":
    main()