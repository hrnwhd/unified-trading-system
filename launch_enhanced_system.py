#!/usr/bin/env python3
# ===== ENHANCED SYSTEM LAUNCHER =====
# Handles all imports and path issues automatically

import sys
import os
from pathlib import Path
import logging

# Setup logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def setup_paths():
    """Setup all necessary paths for imports"""
    base_path = Path(__file__).parent.absolute()
    
    # Add all necessary paths
    paths_to_add = [
        str(base_path),                    # Root directory
        str(base_path / 'core'),           # Core modules
        str(base_path / 'config'),         # Config modules  
        str(base_path / 'scrapers'),       # Scraper modules
        str(base_path / 'interfaces'),     # Interface modules
    ]
    
    for path in paths_to_add:
        if path not in sys.path:
            sys.path.insert(0, path)
    
    logger.info(f"✅ Python paths configured for {base_path}")
    return base_path

def check_file_structure():
    """Check if all required files are present"""
    base_path = Path(__file__).parent
    
    required_files = {
        'Enhanced Trading Engine': 'enhanced_trading_engine.py',
        'Enhanced Config Manager': 'config/enhanced_config_manager.py',
        'Integration Utilities': 'integration_utilities.py',
        'Original Trading Engine': 'core/trading_engine_backup.py',
        'Data Manager': 'core/data_manager.py'
    }
    
    missing_files = []
    present_files = []
    
    for description, file_path in required_files.items():
        full_path = base_path / file_path
        if full_path.exists():
            present_files.append(f"✅ {description}: {file_path}")
        else:
            missing_files.append(f"❌ {description}: {file_path}")
    
    # Show results
    print("📁 File Structure Check:")
    for file_info in present_files:
        print(f"  {file_info}")
    
    if missing_files:
        print("\n⚠️ Missing Files:")
        for file_info in missing_files:
            print(f"  {file_info}")
        return False
    
    print("✅ All required files present")
    return True

def launch_setup():
    """Launch the Phase 3 setup process"""
    print("🚀 Phase 3 Enhanced Trading System - Setup Launcher")
    print("=" * 60)
    
    try:
        # Setup paths
        base_path = setup_paths()
        
        # Check file structure
        if not check_file_structure():
            print("\n❌ Setup aborted due to missing files")
            return False
        
        # Import integration utilities
        try:
            from integration_utilities import Phase3IntegrationManager
            integration_manager = Phase3IntegrationManager()
            
            print("\n🔄 Starting Phase 3 integration...")
            
            # Initialize integration
            if integration_manager.initialize_integration():
                print("✅ Phase 3 integration completed successfully!")
                
                # Show status
                status = integration_manager.get_integration_status()
                print(f"\n📊 Integration Status:")
                for key, value in status['integration_status'].items():
                    emoji = "✅" if value else "❌"
                    print(f"  {emoji} {key.replace('_', ' ').title()}: {value}")
                
                return True
            else:
                print("❌ Phase 3 integration failed")
                return False
                
        except ImportError as e:
            print(f"❌ Import error: {e}")
            print("💡 Make sure all files are in the correct locations")
            return False
            
    except Exception as e:
        print(f"❌ Setup failed: {e}")
        return False

def launch_trading_modes():
    """Launch trading mode configuration"""
    print("🎛️ Trading Mode Configuration Launcher")
    print("=" * 45)
    
    try:
        setup_paths()
        
        if not check_file_structure():
            print("\n❌ Configuration aborted due to missing files")
            return False
        
        from enhanced_config_manager import EnhancedConfigManager
        config_manager = EnhancedConfigManager()
        
        print("\nAvailable trading modes:")
        print("1. 🔧 Pure TA Mode (Your original system only)")
        print("2. 🛡️ Conservative Intelligence (Safe enhancements)")
        print("3. 🧠 Full Intelligence (All features enabled)")
        print("4. ⚡ Aggressive Mode (Higher risk settings)")
        print("5. 🔒 Martingale Protection (Protect existing batches)")
        print("6. 📊 View Current Configuration")
        
        choice = input("\nSelect mode (1-6): ").strip()
        
        if choice == '1':
            if config_manager.set_pure_ta_mode():
                print("✅ Pure TA Mode activated")
                print("   🔧 All intelligence features disabled")
                print("   📈 Using your proven technical analysis only")
        elif choice == '2':
            if config_manager.set_conservative_mode():
                print("✅ Conservative Intelligence Mode activated")
                print("   🛡️ 50% risk level, enhanced safety")
                print("   📊 Intelligence features enabled with safe settings")
        elif choice == '3':
            if config_manager.set_full_intelligence_mode():
                print("✅ Full Intelligence Mode activated")
                print("   🧠 All intelligence features enabled")
                print("   ⚖️ 70% TA weight, 30% data weight")
        elif choice == '4':
            if config_manager.set_aggressive_mode():
                print("✅ Aggressive Mode activated")
                print("   ⚡ 150% risk level")
                print("   🎯 More opportunities, higher risk")
        elif choice == '5':
            if config_manager.set_martingale_protection_mode():
                print("✅ Martingale Protection Mode activated")
                print("   🔒 Existing batches fully protected")
                print("   🛡️ Intelligence bypassed for running batches")
        elif choice == '6':
            status = config_manager.get_status_summary()
            print(f"\n📊 Current Configuration:")
            print(f"  Enhanced Features: {status['features']['enhanced_features']}")
            print(f"  Master Risk Level: {status['risk_settings']['master_risk_level']}%")
            print(f"  TA Weight: {status['risk_settings']['ta_weight']}%")
            print(f"  Data Weight: {status['risk_settings']['data_weight']}%")
            print(f"  Martingale Protection: {status['risk_settings']['martingale_protection']}")
        else:
            print("❌ Invalid choice")
            return False
        
        return True
        
    except Exception as e:
        print(f"❌ Mode configuration failed: {e}")
        return False

def launch_enhanced_robot():
    """Launch the enhanced trading robot"""
    print("🚀 Enhanced Trading Robot Launcher")
    print("=" * 35)
    
    try:
        setup_paths()
        
        if not check_file_structure():
            print("\n❌ Launch aborted due to missing files")
            return False
        
        # Import and run enhanced robot
        from enhanced_trading_engine import run_enhanced_robot
        
        print("🎯 Starting Enhanced Trading Robot...")
        print("📊 Loading configuration and data sources...")
        
        # Run the robot
        run_enhanced_robot()
        
    except ImportError as e:
        print(f"❌ Import error: {e}")
        print("💡 Make sure enhanced_trading_engine.py is in the root directory")
        return False
    except Exception as e:
        print(f"❌ Launch failed: {e}")
        return False

def launch_original_robot():
    """Launch your original proven robot"""
    print("🔧 Original Trading Robot Launcher")
    print("=" * 35)
    
    try:
        setup_paths()
        
        # Check for original robot
        original_robot_path = Path('core/trading_engine_backup.py')
        if not original_robot_path.exists():
            print("❌ Original robot not found: core/trading_engine_backup.py")
            return False
        
        # Import and run original robot
        from core.trading_engine_backup import run_simplified_robot
        
        print("🎯 Starting Original Proven Trading Robot...")
        print("📈 Using your proven technical analysis system...")
        
        # Run the original robot
        run_simplified_robot()
        
    except ImportError as e:
        print(f"❌ Import error: {e}")
        print("💡 Make sure core/trading_engine_backup.py exists and is functional")
        return False
    except Exception as e:
        print(f"❌ Launch failed: {e}")
        return False

def show_system_status():
    """Show comprehensive system status"""
    print("📊 Enhanced Trading System Status")
    print("=" * 35)
    
    try:
        setup_paths()
        
        # Check file structure
        if not check_file_structure():
            print("\n⚠️ Some files are missing")
        
        # Try to get integration status
        try:
            from integration_utilities import Phase3IntegrationManager
            integration_manager = Phase3IntegrationManager()
            status = integration_manager.get_integration_status()
            
            print("\n🔄 Integration Status:")
            for key, value in status['integration_status'].items():
                emoji = "✅" if value else "❌"
                print(f"  {emoji} {key.replace('_', ' ').title()}")
            
            # Show data sources
            print(f"\n📊 Data Sources:")
            for file_path, info in status['data_sources'].items():
                if info['exists']:
                    age_hours = info['age_hours']
                    status_text = "Fresh" if age_hours < 2 else f"Stale ({age_hours:.1f}h old)"
                    emoji = "✅" if age_hours < 2 else "⚠️"
                    print(f"  {emoji} {file_path}: {status_text}")
                else:
                    print(f"  ❌ {file_path}: Missing")
            
        except Exception as e:
            print(f"❌ Could not get integration status: {e}")
        
        # Try to get configuration status
        try:
            from enhanced_config_manager import EnhancedConfigManager
            config_manager = EnhancedConfigManager()
            config_status = config_manager.get_status_summary()
            
            print(f"\n⚙️ Configuration:")
            print(f"  Enhanced Features: {config_status['features']['enhanced_features']}")
            print(f"  Master Risk Level: {config_status['risk_settings']['master_risk_level']}%")
            print(f"  TA/Data Weights: {config_status['risk_settings']['ta_weight']}%/{config_status['risk_settings']['data_weight']}%")
            
        except Exception as e:
            print(f"❌ Could not get configuration status: {e}")
        
        return True
        
    except Exception as e:
        print(f"❌ Status check failed: {e}")
        return False

def main():
    """Main launcher interface"""
    if len(sys.argv) < 2:
        print("🚀 Enhanced Trading System Launcher")
        print("=" * 40)
        print("Usage:")
        print("  python launch_enhanced_system.py setup     - Setup Phase 3 integration")
        print("  python launch_enhanced_system.py modes     - Configure trading modes")
        print("  python launch_enhanced_system.py enhanced  - Start enhanced robot")
        print("  python launch_enhanced_system.py original  - Start original robot")
        print("  python launch_enhanced_system.py status    - Show system status")
        print("  python launch_enhanced_system.py test      - Test all systems")
        print("")
        print("💡 Quick start: python launch_enhanced_system.py setup")
        return
    
    command = sys.argv[1].lower()
    
    try:
        if command == 'setup':
            success = launch_setup()
            if success:
                print("\n🎉 Setup completed! Next steps:")
                print("  1. python launch_enhanced_system.py modes   - Choose trading mode")
                print("  2. python launch_enhanced_system.py enhanced - Start enhanced trading")
            sys.exit(0 if success else 1)
            
        elif command == 'modes':
            success = launch_trading_modes()
            if success:
                print("\n🎯 Ready to trade! Run:")
                print("  python launch_enhanced_system.py enhanced")
            sys.exit(0 if success else 1)
            
        elif command == 'enhanced':
            launch_enhanced_robot()
            
        elif command == 'original':
            launch_original_robot()
            
        elif command == 'status':
            show_system_status()
            
        elif command == 'test':
            setup_paths()
            if check_file_structure():
                from integration_utilities import Phase3IntegrationManager
                integration_manager = Phase3IntegrationManager()
                test_results = integration_manager.run_comprehensive_test()
                
                print(f"\n🧪 Test Results: {test_results['overall_status'].upper()}")
                if test_results.get('recommendations'):
                    print("\n💡 Recommendations:")
                    for rec in test_results['recommendations']:
                        print(f"  {rec}")
            
        else:
            print(f"❌ Unknown command: {command}")
            print("Run without arguments to see usage")
            sys.exit(1)
            
    except KeyboardInterrupt:
        print("\n🛑 Stopped by user")
    except Exception as e:
        print(f"❌ Error: {e}")
        sys.exit(1)

if __name__ == "__main__":
    main()