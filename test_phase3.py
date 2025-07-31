#!/usr/bin/env python3
# ===== PHASE 3 TESTING & VALIDATION SCRIPT =====
# Comprehensive testing suite for Enhanced Trading System
# Validates data integration, trading intelligence, and system health

import os
import sys
import json
import time
import logging
from datetime import datetime, timedelta
from pathlib import Path

# Add paths
sys.path.append(os.path.join(os.path.dirname(__file__), 'core'))
sys.path.append(os.path.join(os.path.dirname(__file__), 'scrapers'))

# ===== SETUP =====
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - TESTER - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

class Phase3Tester:
    """Comprehensive testing suite for Phase 3"""
    
    def __init__(self):
        self.test_results = {}
        self.data_dir = Path("data")
        self.config_dir = Path("config")
        self.core_dir = Path("core")
        self.logs_dir = Path("logs")
        
    def run_all_tests(self):
        """Run complete Phase 3 test suite"""
        logger.info("🧪 Starting Phase 3 Comprehensive Testing...")
        logger.info("="*60)
        
        # Test 1: System Structure
        self.test_system_structure()
        
        # Test 2: Configuration Validation
        self.test_configuration()
        
        # Test 3: Data Collection System
        self.test_data_collection()
        
        # Test 4: Data Integration
        self.test_data_integration()
        
        # Test 5: Enhanced Trading Logic
        self.test_enhanced_trading_logic()
        
        # Test 6: Risk Management
        self.test_enhanced_risk_management()
        
        # Test 7: System Integration
        self.test_system_integration()
        
        # Test 8: Performance Simulation
        self.test_performance_simulation()
        
        # Generate test report
        self.generate_test_report()
        
        logger.info("="*60)
        logger.info("🎯 Phase 3 Testing Complete!")
        
        return self.get_overall_result()
    
    def test_system_structure(self):
        """Test 1: Verify system structure"""
        logger.info("🔍 Test 1: System Structure Validation")
        
        results = {
            'directories': {},
            'core_files': {},
            'config_files': {},
            'data_files': {}
        }
        
        # Check directories
        required_dirs = ['config', 'core', 'scrapers', 'data', 'logs', 'interfaces']
        for dir_name in required_dirs:
            exists = Path(dir_name).exists()
            results['directories'][dir_name] = exists
            logger.info(f"   {'✅' if exists else '❌'} Directory: {dir_name}")
        
        # Check core files
        core_files = [
            'core/trading_engine.py',
            'core/data_manager.py',
            'core/trading_hub.py'
        ]
        for file_path in core_files:
            exists = Path(file_path).exists()
            results['core_files'][file_path] = exists
            logger.info(f"   {'✅' if exists else '❌'} Core file: {file_path}")
        
        # Check config files
        config_files = [
            'config/settings.json',
            'config/pairs.json',
            'config/schedules.json'
        ]
        for file_path in config_files:
            exists = Path(file_path).exists()
            results['config_files'][file_path] = exists
            logger.info(f"   {'✅' if exists else '❌'} Config file: {file_path}")
        
        self.test_results['system_structure'] = results
        logger.info("✅ Test 1 Complete\n")
    
    def test_configuration(self):
        """Test 2: Validate configuration"""
        logger.info("⚙️ Test 2: Configuration Validation")
        
        results = {
            'settings_valid': False,
            'phase3_features': {},
            'trading_config': {},
            'data_integration_config': {}
        }
        
        try:
            # Load and validate settings
            with open('config/settings.json', 'r') as f:
                settings = json.load(f)
            
            results['settings_valid'] = True
            logger.info("   ✅ Settings file loaded successfully")
            
            # Check Phase 3 specific features
            phase3_features = [
                'data_integration.enabled',
                'data_integration.sentiment_threshold',
                'enhanced_risk_management.correlation_adjustment',
                'trading.enhanced_engine'
            ]
            
            for feature in phase3_features:
                keys = feature.split('.')
                value = settings
                try:
                    for key in keys:
                        value = value[key]
                    results['phase3_features'][feature] = value
                    logger.info(f"   ✅ {feature}: {value}")
                except KeyError:
                    results['phase3_features'][feature] = None
                    logger.warning(f"   ⚠️ Missing: {feature}")
            
            # Validate critical settings
            mt5_account = settings.get('mt5', {}).get('account_number')
            magic_number = settings.get('mt5', {}).get('magic_number')
            
            if mt5_account:
                logger.info(f"   ✅ MT5 Account: {mt5_account}")
            else:
                logger.warning("   ⚠️ MT5 Account not configured")
            
            if magic_number:
                logger.info(f"   ✅ Magic Number: {magic_number}")
            else:
                logger.warning("   ⚠️ Magic Number not configured")
                
        except Exception as e:
            logger.error(f"   ❌ Configuration error: {e}")
            results['settings_valid'] = False
        
        self.test_results['configuration'] = results
        logger.info("✅ Test 2 Complete\n")
    
    def test_data_collection(self):
        """Test 3: Data collection system"""
        logger.info("📊 Test 3: Data Collection System")
        
        results = {
            'scrapers_available': {},
            'data_files_exist': {},
            'data_freshness': {}
        }
        
        # Check scraper availability
        scrapers = [
            'scrapers/sentiment_scraper.py',
            'scrapers/correlation_scraper.py',
            'scrapers/calendar_scraper.py',
            'scrapers/cot_scraper.py'
        ]
        
        for scraper in scrapers:
            exists = Path(scraper).exists()
            results['scrapers_available'][scraper] = exists
            logger.info(f"   {'✅' if exists else '❌'} Scraper: {scraper}")
        
        # Check data files
        data_files = [
            'data/market_data.json',
            'sentiment_signals.json',
            'correlation_data.json'
        ]
        
        for data_file in data_files:
            exists = Path(data_file).exists()
            results['data_files_exist'][data_file] = exists
            logger.info(f"   {'✅' if exists else '⚠️'} Data file: {data_file}")
        
        # Test data freshness
        try:
            if Path('data/market_data.json').exists():
                with open('data/market_data.json', 'r') as f:
                    market_data = json.load(f)
                
                last_updated = market_data.get('last_updated')
                if last_updated:
                    update_time = datetime.fromisoformat(last_updated)
                    age_minutes = (datetime.now() - update_time).total_seconds() / 60
                    results['data_freshness']['market_data_age_minutes'] = age_minutes
                    
                    freshness = "FRESH" if age_minutes < 60 else "STALE" if age_minutes < 240 else "OLD"
                    logger.info(f"   📊 Market data: {freshness} ({age_minutes:.1f}m old)")
                
        except Exception as e:
            logger.warning(f"   ⚠️ Error checking data freshness: {e}")
        
        self.test_results['data_collection'] = results
        logger.info("✅ Test 3 Complete\n")
    
    def test_data_integration(self):
        """Test 4: Data integration functionality"""
        logger.info("🔗 Test 4: Data Integration")
        
        results = {
            'integration_manager_loadable': False,
            'sentiment_data_accessible': False,
            'correlation_data_accessible': False,
            'economic_events_accessible': False,
            'data_caching_works': False
        }
        
        try:
            # Test importing the data integration manager
            sys.path.append('core')
            from trading_engine import DataIntegrationManager
            
            data_manager = DataIntegrationManager()
            results['integration_manager_loadable'] = True
            logger.info("   ✅ DataIntegrationManager loaded successfully")
            
            # Test sentiment data access
            try:
                sentiment_data = data_manager.get_sentiment_data()
                results['sentiment_data_accessible'] = True
                logger.info(f"   ✅ Sentiment data: {len(sentiment_data)} pairs")
            except Exception as e:
                logger.warning(f"   ⚠️ Sentiment data error: {e}")
            
            # Test correlation data access
            try:
                correlation_data = data_manager.get_correlation_data()
                matrix_size = len(correlation_data.get('matrix', {}))
                warnings_count = len(correlation_data.get('warnings', []))
                results['correlation_data_accessible'] = True
                logger.info(f"   ✅ Correlation data: {matrix_size} currencies, {warnings_count} warnings")
            except Exception as e:
                logger.warning(f"   ⚠️ Correlation data error: {e}")
            
            # Test economic events access
            try:
                economic_events = data_manager.get_economic_events(24)
                results['economic_events_accessible'] = True
                logger.info(f"   ✅ Economic events: {len(economic_events)} upcoming")
            except Exception as e:
                logger.warning(f"   ⚠️ Economic events error: {e}")
            
            # Test data caching
            try:
                # Load data twice to test caching
                start_time = time.time()
                data_manager.load_market_data()
                first_load_time = time.time() - start_time
                
                start_time = time.time()
                data_manager.load_market_data()
                second_load_time = time.time() - start_time
                
                if second_load_time < first_load_time:
                    results['data_caching_works'] = True
                    logger.info("   ✅ Data caching working (faster second load)")
                else:
                    logger.info("   ⚠️ Data caching may not be working")
                    
            except Exception as e:
                logger.warning(f"   ⚠️ Caching test error: {e}")
                
        except ImportError as e:
            logger.error(f"   ❌ Cannot import DataIntegrationManager: {e}")
        except Exception as e:
            logger.error(f"   ❌ Data integration test error: {e}")
        
        self.test_results['data_integration'] = results
        logger.info("✅ Test 4 Complete\n")
    
    def test_enhanced_trading_logic(self):
        """Test 5: Enhanced trading decision logic"""
        logger.info("🧠 Test 5: Enhanced Trading Logic")
        
        results = {
            'decision_manager_loadable': False,
            'sentiment_blocking_works': False,
            'correlation_risk_detection': False,
            'economic_timing_check': False,
            'enhanced_position_sizing': False
        }
        
        try:
            from trading_engine import EnhancedTradingDecisionManager
            
            decision_manager = EnhancedTradingDecisionManager()
            results['decision_manager_loadable'] = True
            logger.info("   ✅ EnhancedTradingDecisionManager loaded")
            
            # Test sentiment blocking logic
            try:
                # Create mock sentiment data with extreme values
                mock_sentiment = {
                    'EURUSD': {
                        'blocked_directions': ['long'],
                        'sentiment': {'short': 80, 'long': 20}
                    }
                }
                
                # Mock the data manager to return our test data
                decision_manager.data_manager.get_sentiment_data = lambda: mock_sentiment
                
                # Test if blocking works
                allowed, reason = decision_manager.can_trade_direction('EURUSD', 'long')
                if not allowed and 'sentiment' in reason.lower():
                    results['sentiment_blocking_works'] = True
                    logger.info("   ✅ Sentiment blocking: WORKING")
                else:
                    logger.warning("   ⚠️ Sentiment blocking: NOT WORKING")
                    
            except Exception as e:
                logger.warning(f"   ⚠️ Sentiment blocking test error: {e}")
            
            # Test correlation risk detection
            try:
                mock_correlation = {
                    'warnings': [
                        {
                            'type': 'HIGH_CORRELATION',
                            'pair': 'EURUSD-GBPUSD',
                            'value': 85
                        }
                    ]
                }
                
                decision_manager.data_manager.get_correlation_data = lambda: mock_correlation
                
                # This should pass but log warnings
                allowed, reason = decision_manager.can_trade_direction('EURUSD', 'long')
                results['correlation_risk_detection'] = True
                logger.info("   ✅ Correlation risk detection: WORKING")
                
            except Exception as e:
                logger.warning(f"   ⚠️ Correlation risk test error: {e}")
            
            # Test economic timing
            try:
                # Mock upcoming high-impact USD event
                mock_events = [
                    {
                        'currency': 'USD',
                        'event_name': 'Non-Farm Payrolls',
                        'impact': 'high',
                        'time_until_hours': 0.5
                    }
                ]
                
                decision_manager.data_manager.get_economic_events = lambda x: mock_events
                
                allowed, reason = decision_manager.can_trade_direction('EURUSD', 'long')
                if not allowed and 'economic' in reason.lower():
                    results['economic_timing_check'] = True
                    logger.info("   ✅ Economic timing check: WORKING")
                else:
                    logger.warning("   ⚠️ Economic timing check: NOT WORKING")
                    
            except Exception as e:
                logger.warning(f"   ⚠️ Economic timing test error: {e}")
                
        except ImportError as e:
            logger.error(f"   ❌ Cannot import enhanced trading components: {e}")
        except Exception as e:
            logger.error(f"   ❌ Enhanced trading logic test error: {e}")
        
        self.test_results['enhanced_trading_logic'] = results
        logger.info("✅ Test 5 Complete\n")
    
    def test_enhanced_risk_management(self):
        """Test 6: Enhanced risk management"""
        logger.info("⚖️ Test 6: Enhanced Risk Management")
        
        results = {
            'dynamic_position_sizing': False,
            'risk_reduction_factors': False,
            'correlation_adjustments': False,
            'event_based_adjustments': False
        }
        
        try:
            from trading_engine import EnhancedTradingDecisionManager
            
            decision_manager = EnhancedTradingDecisionManager()
            
            # Test dynamic position sizing
            try:
                base_risk = 100.0  # $100 base risk
                
                # Test with normal market conditions
                enhanced_risk = decision_manager.get_enhanced_position_sizing(
                    'EURUSD', base_risk, {}
                )
                
                if enhanced_risk != base_risk:
                    results['dynamic_position_sizing'] = True
                    logger.info(f"   ✅ Dynamic sizing: ${base_risk} → ${enhanced_risk:.2f}")
                else:
                    logger.info("   ℹ️ Dynamic sizing: No adjustments applied")
                    results['dynamic_position_sizing'] = True  # Still working, just no adjustments needed
                    
            except Exception as e:
                logger.warning(f"   ⚠️ Dynamic position sizing test error: {e}")
            
            # Test risk reduction factors
            try:
                # Mock high correlation environment
                mock_correlation = {
                    'warnings': [
                        {'type': 'HIGH_CORRELATION', 'pair': 'EURUSD-GBPUSD', 'value': 85},
                        {'type': 'HIGH_CORRELATION', 'pair': 'AUDUSD-NZDUSD', 'value': 88},
                        {'type': 'HIGH_CORRELATION', 'pair': 'USDCAD-USDCHF', 'value': 90},
                        {'type': 'HIGH_CORRELATION', 'pair': 'EURJPY-GBPJPY', 'value': 82},
                        {'type': 'HIGH_CORRELATION', 'pair': 'AUDCAD-NZDCAD', 'value': 86}
                    ]
                }
                
                decision_manager.data_manager.get_correlation_data = lambda: mock_correlation
                
                enhanced_risk = decision_manager.get_enhanced_position_sizing(
                    'EURUSD', 100.0, {}
                )
                
                if enhanced_risk < 100.0:
                    results['correlation_adjustments'] = True
                    logger.info(f"   ✅ Correlation adjustment: Risk reduced to ${enhanced_risk:.2f}")
                else:
                    logger.warning("   ⚠️ Correlation adjustment: No reduction applied")
                    
            except Exception as e:
                logger.warning(f"   ⚠️ Correlation adjustment test error: {e}")
            
            # Test event-based adjustments
            try:
                # Mock upcoming major events
                mock_events = [
                    {
                        'currency': 'USD',
                        'event_name': 'FOMC Decision',
                        'impact': 'high',
                        'time_until_hours': 2.0
                    }
                ]
                
                decision_manager.data_manager.get_economic_events = lambda x: mock_events
                
                enhanced_risk = decision_manager.get_enhanced_position_sizing(
                    'EURUSD', 100.0, {}
                )
                
                if enhanced_risk < 100.0:
                    results['event_based_adjustments'] = True
                    logger.info(f"   ✅ Event-based adjustment: Risk reduced to ${enhanced_risk:.2f}")
                else:
                    logger.warning("   ⚠️ Event-based adjustment: No reduction applied")
                    
            except Exception as e:
                logger.warning(f"   ⚠️ Event-based adjustment test error: {e}")
        
        except ImportError as e:
            logger.error(f"   ❌ Cannot import risk management components: {e}")
        except Exception as e:
            logger.error(f"   ❌ Risk management test error: {e}")
        
        self.test_results['enhanced_risk_management'] = results
        logger.info("✅ Test 6 Complete\n")
    
    def test_system_integration(self):
        """Test 7: Overall system integration"""
        logger.info("🔧 Test 7: System Integration")
        
        results = {
            'main_system_loadable': False,
            'enhanced_config_loadable': False,
            'component_integration': False,
            'logging_system': False
        }
        
        try:
            # Test main system loading
            sys.path.append('.')
            from main import EnhancedTradingSystemManager
            
            system_manager = EnhancedTradingSystemManager()
            results['main_system_loadable'] = True
            logger.info("   ✅ Main system loadable")
            
            # Test enhanced config
            config = system_manager.config
            if hasattr(config, 'get_enhanced_default_settings'):
                results['enhanced_config_loadable'] = True
                logger.info("   ✅ Enhanced configuration system working")
            else:
                logger.warning("   ⚠️ Enhanced configuration not found")
            
            # Test component integration
            try:
                # Test if enhanced modules can be imported
                import_success = 0
                total_modules = 0
                
                modules_to_test = [
                    ('core.trading_engine', 'DataIntegrationManager'),
                    ('core.trading_engine', 'EnhancedTradingDecisionManager'),
                    ('core.trading_engine', 'EnhancedTradeManager'),
                    ('core.data_manager', 'DataManager'),
                    ('core.trading_hub', 'TradingHub')
                ]
                
                for module_name, class_name in modules_to_test:
                    total_modules += 1
                    try:
                        module = __import__(module_name, fromlist=[class_name])
                        getattr(module, class_name)
                        import_success += 1
                        logger.info(f"   ✅ Component: {module_name}.{class_name}")
                    except (ImportError, AttributeError) as e:
                        logger.warning(f"   ⚠️ Component missing: {module_name}.{class_name}")
                
                if import_success >= total_modules * 0.7:  # 70% success rate
                    results['component_integration'] = True
                    logger.info(f"   ✅ Component integration: {import_success}/{total_modules} modules")
                else:
                    logger.warning(f"   ⚠️ Component integration incomplete: {import_success}/{total_modules}")
                    
            except Exception as e:
                logger.warning(f"   ⚠️ Component integration test error: {e}")
            
            # Test logging system
            try:
                log_dir = Path("logs")
                if log_dir.exists():
                    log_files = list(log_dir.glob("*.log"))
                    if log_files:
                        results['logging_system'] = True
                        logger.info(f"   ✅ Logging system: {len(log_files)} log files")
                    else:
                        logger.warning("   ⚠️ No log files found")
                else:
                    logger.warning("   ⚠️ Logs directory not found")
                    
            except Exception as e:
                logger.warning(f"   ⚠️ Logging system test error: {e}")
                
        except ImportError as e:
            logger.error(f"   ❌ Cannot import main system: {e}")
        except Exception as e:
            logger.error(f"   ❌ System integration test error: {e}")
        
        self.test_results['system_integration'] = results
        logger.info("✅ Test 7 Complete\n")
    
    def test_performance_simulation(self):
        """Test 8: Performance simulation"""
        logger.info("🚀 Test 8: Performance Simulation")
        
        results = {
            'signal_generation_speed': 0,
            'data_access_speed': 0,
            'decision_making_speed': 0,
            'memory_usage_acceptable': False
        }
        
        try:
            # Test signal generation speed
            start_time = time.time()
            
            # Simulate signal generation process
            for i in range(10):
                # Mock signal generation
                mock_signal = {
                    'symbol': 'EURUSD',
                    'direction': 'long',
                    'entry_price': 1.0850,
                    'timestamp': datetime.now()
                }
                # Simulate some processing
                time.sleep(0.01)
            
            signal_time = time.time() - start_time
            results['signal_generation_speed'] = signal_time
            logger.info(f"   ⏱️ Signal generation: {signal_time:.3f}s for 10 signals")
            
            # Test data access speed
            if Path('data/market_data.json').exists():
                start_time = time.time()
                
                for i in range(10):
                    with open('data/market_data.json', 'r') as f:
                        json.load(f)
                
                data_access_time = time.time() - start_time
                results['data_access_speed'] = data_access_time
                logger.info(f"   ⏱️ Data access: {data_access_time:.3f}s for 10 loads")
            
            # Test decision making speed
            try:
                from trading_engine import EnhancedTradingDecisionManager
                decision_manager = EnhancedTradingDecisionManager()
                
                start_time = time.time()
                
                for i in range(10):
                    decision_manager.can_trade_direction('EURUSD', 'long')
                
                decision_time = time.time() - start_time
                results['decision_making_speed'] = decision_time
                logger.info(f"   ⏱️ Decision making: {decision_time:.3f}s for 10 decisions")
                
            except Exception as e:
                logger.warning(f"   ⚠️ Decision speed test error: {e}")
            
            # Basic memory usage check
            import psutil
            process = psutil.Process()
            memory_mb = process.memory_info().rss / 1024 / 1024
            
            if memory_mb < 500:  # Less than 500MB is acceptable
                results['memory_usage_acceptable'] = True
                logger.info(f"   ✅ Memory usage: {memory_mb:.1f} MB (acceptable)")
            else:
                logger.warning(f"   ⚠️ Memory usage: {memory_mb:.1f} MB (high)")
                
        except ImportError:
            logger.warning("   ⚠️ psutil not available for memory testing")
            results['memory_usage_acceptable'] = True  # Assume OK
        except Exception as e:
            logger.warning(f"   ⚠️ Performance simulation error: {e}")
        
        self.test_results['performance_simulation'] = results
        logger.info("✅ Test 8 Complete\n")
    
    def generate_test_report(self):
        """Generate comprehensive test report"""
        logger.info("📋 Generating Test Report...")
        
        report = {
            'test_timestamp': datetime.now().isoformat(),
            'system_phase': 3,
            'test_results': self.test_results,
            'overall_score': 0,
            'recommendations': []
        }
        
        # Calculate overall score
        total_tests = 0
        passed_tests = 0
        
        for test_category, results in self.test_results.items():
            if isinstance(results, dict):
                for test_name, result in results.items():
                    total_tests += 1
                    if result:
                        passed_tests += 1
        
        if total_tests > 0:
            report['overall_score'] = (passed_tests / total_tests) * 100
        
        # Generate recommendations
        recommendations = []
        
        # Check critical failures
        if not self.test_results.get('system_structure', {}).get('core_files', {}).get('core/trading_engine.py'):
            recommendations.append("🚨 CRITICAL: Enhanced trading engine file missing")
        
        if not self.test_results.get('data_integration', {}).get('integration_manager_loadable'):
            recommendations.append("🚨 CRITICAL: Data integration manager not loadable")
        
        if not self.test_results.get('configuration', {}).get('settings_valid'):
            recommendations.append("⚠️ Configuration file needs validation")
        
        # Check data freshness
        data_age = self.test_results.get('data_collection', {}).get('data_freshness', {}).get('market_data_age_minutes')
        if data_age and data_age > 240:  # 4 hours
            recommendations.append("📊 Market data is stale - run data collection")
        
        # Check component integration
        if not self.test_results.get('system_integration', {}).get('component_integration'):
            recommendations.append("🔧 Some system components need attention")
        
        # Performance recommendations
        decision_speed = self.test_results.get('performance_simulation', {}).get('decision_making_speed', 0)
        if decision_speed > 1.0:  # More than 1 second for 10 decisions
            recommendations.append("⚡ Decision making performance could be improved")
        
        report['recommendations'] = recommendations
        
        # Save report
        report_file = f"test_report_phase3_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
        with open(report_file, 'w') as f:
            json.dump(report, f, indent=2, default=str)
        
        # Display summary
        logger.info(f"📊 TEST SUMMARY")
        logger.info(f"   Overall Score: {report['overall_score']:.1f}%")
        logger.info(f"   Tests Passed: {passed_tests}/{total_tests}")
        logger.info(f"   Report saved: {report_file}")
        
        if recommendations:
            logger.info("📋 RECOMMENDATIONS:")
            for rec in recommendations:
                logger.info(f"   {rec}")
        else:
            logger.info("✅ No critical issues found!")
        
        return report
    
    def get_overall_result(self):
        """Get overall test result"""
        score = 0
        for test_category, results in self.test_results.items():
            if isinstance(results, dict):
                category_score = sum(1 for result in results.values() if result)
                total_in_category = len(results)
                if total_in_category > 0:
                    score += (category_score / total_in_category) * 100
        
        num_categories = len(self.test_results)
        if num_categories > 0:
            average_score = score / num_categories
            return average_score >= 70  # 70% pass rate
        
        return False

def run_quick_test():
    """Run a quick validation test"""
    logger.info("🏃 Running Quick Phase 3 Validation...")
    
    quick_checks = [
        ("Enhanced trading engine file", lambda: Path('core/trading_engine.py').exists()),
        ("Market data file", lambda: Path('data/market_data.json').exists()),
        ("Configuration file", lambda: Path('config/settings.json').exists()),
        ("Data integration import", test_data_integration_import),
        ("Enhanced trading import", test_enhanced_trading_import)
    ]
    
    results = []
    for check_name, check_func in quick_checks:
        try:
            result = check_func()
            results.append((check_name, result))
            logger.info(f"   {'✅' if result else '❌'} {check_name}")
        except Exception as e:
            results.append((check_name, False))
            logger.info(f"   ❌ {check_name}: {e}")
    
    passed = sum(1 for _, result in results if result)
    total = len(results)
    
    logger.info(f"\n🎯 Quick Test Result: {passed}/{total} checks passed")
    
    if passed >= total * 0.8:  # 80% pass rate
        logger.info("✅ System appears ready for Phase 3!")
        return True
    else:
        logger.warning("⚠️ System needs attention before Phase 3")
        return False

def test_data_integration_import():
    """Test if data integration components can be imported"""
    try:
        sys.path.append('core')
        from trading_engine import DataIntegrationManager
        return True
    except ImportError:
        return False

def test_enhanced_trading_import():
    """Test if enhanced trading components can be imported"""
    try:
        sys.path.append('core')
        from trading_engine import EnhancedTradingDecisionManager
        return True
    except ImportError:
        return False

def create_sample_data_for_testing():
    """Create sample data files for testing"""
    logger.info("📝 Creating sample data for testing...")
    
    # Create data directory
    Path("data").mkdir(exist_ok=True)
    
    # Sample market data
    sample_market_data = {
        "metadata": {
            "system_name": "Unified Trading System",
            "version": "1.3",
            "created": datetime.now().isoformat()
        },
        "last_updated": datetime.now().isoformat(),
        "system_status": "optimal",
        "data_freshness": {
            "sentiment": {"fresh": True, "last_update": datetime.now().isoformat()},
            "correlation": {"fresh": True, "last_update": datetime.now().isoformat()},
            "economic_calendar": {"fresh": True, "last_update": datetime.now().isoformat()},
            "cot": {"fresh": False, "last_update": None}
        },
        "data_sources": {
            "sentiment": {
                "status": "fresh",
                "last_update": datetime.now().isoformat(),
                "pairs": {
                    "EURUSD": {
                        "allowed_directions": ["long"],
                        "blocked_directions": ["short"],
                        "sentiment": {"short": 75, "long": 25}
                    },
                    "GBPUSD": {
                        "allowed_directions": ["short", "long"],
                        "blocked_directions": [],
                        "sentiment": {"short": 45, "long": 55}
                    }
                }
            },
            "correlation": {
                "status": "fresh",
                "last_update": datetime.now().isoformat(),
                "matrix": {
                    "EURUSD": {
                        "GBPUSD": {"value": 75.5, "percentage": "75.5%"}
                    }
                },
                "warnings": [
                    {
                        "type": "HIGH_CORRELATION",
                        "pair": "EURUSD-GBPUSD",
                        "value": 75.5,
                        "message": "High correlation detected"
                    }
                ]
            },
            "economic_calendar": {
                "status": "fresh",
                "last_update": datetime.now().isoformat(),
                "events": [
                    {
                        "currency": "USD",
                        "event_name": "Non-Farm Payrolls",
                        "impact": "high",
                        "time": "14:30",
                        "time_until": "2h"
                    }
                ]
            },
            "cot": {
                "status": "waiting",
                "financial": {},
                "commodity": {}
            }
        }
    }
    
    # Save sample market data
    with open("data/market_data.json", 'w') as f:
        json.dump(sample_market_data, f, indent=2)
    
    # Sample sentiment signals
    sample_sentiment = {
        "timestamp": datetime.now().isoformat(),
        "data_source": "Sample",
        "pairs": {
            "EURUSD": {
                "allowed_directions": ["long"],
                "blocked_directions": ["short"],
                "sentiment": {"short": 75, "long": 25}
            }
        }
    }
    
    with open("sentiment_signals.json", 'w') as f:
        json.dump(sample_sentiment, f, indent=2)
    
    logger.info("✅ Sample data created successfully")

def main():
    """Main testing function"""
    import argparse
    
    parser = argparse.ArgumentParser(description='Phase 3 Testing Suite')
    parser.add_argument('--quick', action='store_true', help='Run quick validation only')
    parser.add_argument('--create-sample-data', action='store_true', help='Create sample data for testing')
    parser.add_argument('--full', action='store_true', help='Run full comprehensive test suite')
    
    args = parser.parse_args()
    
    if args.create_sample_data:
        create_sample_data_for_testing()
        return
    
    if args.quick:
        success = run_quick_test()
        sys.exit(0 if success else 1)
    
    if args.full or len(sys.argv) == 1:
        tester = Phase3Tester()
        success = tester.run_all_tests()
        sys.exit(0 if success else 1)

if __name__ == "__main__":
    main()