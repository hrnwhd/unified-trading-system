#!/usr/bin/env python3
# ===== PHASE 2 SETUP SCRIPT - FIXED FOR WINDOWS =====
# Helps integrate your existing scrapers into the unified system
# Run this after copying your scraper files

import os
import json
import shutil
from pathlib import Path
import sys
import importlib.util  # FIX: Added missing import

def check_scraper_files():
    """Check which scraper files are available"""
    print("Checking scraper files...")
    
    required_files = {
        'calendar_scraper.py': 'Economic Calendar Scraper',
        'sentiment_scraper.py': 'Sentiment Analysis Scraper',
        'correlation_scraper.py': 'Correlation Data Scraper',
        'cot_scraper.py': 'COT Data Scraper'
    }
    
    scrapers_dir = Path("scrapers")
    scrapers_dir.mkdir(exist_ok=True)
    
    available = {}
    missing = {}
    
    for filename, description in required_files.items():
        file_path = scrapers_dir / filename
        if file_path.exists():
            available[filename] = description
            print(f"   OK {filename} - {description}")
        else:
            missing[filename] = description
            print(f"   MISSING {filename} - {description}")
    
    return available, missing

def copy_existing_files():
    """Help copy existing scraper files"""
    print("\nLooking for existing scraper files to copy...")
    
    # Common names your files might have
    file_mappings = {
        'Calender2.py': 'scrapers/calendar_scraper.py',
        'Calendar2.py': 'scrapers/calendar_scraper.py',
        'calendar2.py': 'scrapers/calendar_scraper.py',
        'sentiment_manager.py': 'scrapers/sentiment_scraper.py',
        'Co-relation Data.py': 'scrapers/correlation_scraper.py',
        'correlation_data.py': 'scrapers/correlation_scraper.py',
        'COT.py': 'scrapers/cot_scraper.py',
        'cot.py': 'scrapers/cot_scraper.py',
        'cot_data_manager.py': 'scrapers/cot_scraper.py'
    }
    
    copied_files = []
    
    for source_name, target_path in file_mappings.items():
        if Path(source_name).exists():
            try:
                shutil.copy2(source_name, target_path)
                print(f"   COPIED {source_name} -> {target_path}")
                copied_files.append(target_path)
            except Exception as e:
                print(f"   ERROR copying {source_name}: {e}")
    
    if not copied_files:
        print("   No matching files found for automatic copying")
        print("   Manual copy instructions:")
        print("   Copy your files to scrapers/ with these exact names:")
        for target in file_mappings.values():
            print(f"     - {target}")
    
    return copied_files

def test_scrapers():
    """Test each scraper individually"""
    print("\nTesting individual scrapers...")
    
    scrapers_to_test = [
        ('calendar_scraper.py', 'FixedEconomicCalendarScraper'),
        ('sentiment_scraper.py', 'SentimentSignalManager'),
        ('correlation_scraper.py', 'CorrelationSignalManager'),
        ('cot_scraper.py', 'COTDataManager')
    ]
    
    test_results = {}
    
    for scraper_file, class_name in scrapers_to_test:
        scraper_path = Path("scrapers") / scraper_file
        
        if not scraper_path.exists():
            print(f"   SKIP {scraper_file} - Not found")
            test_results[scraper_file] = 'missing'
            continue
        
        try:
            # Try to import the scraper
            sys.path.insert(0, str(Path("scrapers").absolute()))
            
            module_name = scraper_file.replace('.py', '')
            spec = importlib.util.spec_from_file_location(module_name, scraper_path)
            module = importlib.util.module_from_spec(spec)
            spec.loader.exec_module(module)
            
            # Check if the required class exists
            if hasattr(module, class_name):
                print(f"   OK {scraper_file} - Class {class_name} found")
                test_results[scraper_file] = 'success'
                
                # Try to instantiate (basic test)
                try:
                    scraper_class = getattr(module, class_name)
                    instance = scraper_class()
                    print(f"   OK {scraper_file} - Successfully instantiated")
                except Exception as e:
                    print(f"   WARNING {scraper_file} - Import OK but instantiation failed: {e}")
                    test_results[scraper_file] = 'partial'
            else:
                print(f"   ERROR {scraper_file} - Class {class_name} not found")
                available_classes = [name for name in dir(module) if not name.startswith('_') and name[0].isupper()]
                print(f"      Available classes: {available_classes}")
                test_results[scraper_file] = 'error'
                
        except Exception as e:
            print(f"   ERROR {scraper_file} - Import error: {e}")
            test_results[scraper_file] = 'error'
    
    return test_results

def test_data_manager():
    """Test the data manager integration"""
    print("\nTesting Data Manager integration...")
    
    try:
        # Import the data manager
        sys.path.insert(0, str(Path("core").absolute()))
        from data_manager import test_data_manager
        
        print("   OK Data Manager imported successfully")
        
        # Run the test
        data_manager = test_data_manager()
        
        if data_manager:
            print("   OK Data Manager test completed")
            
            # Show available scrapers
            available_scrapers = list(data_manager.scrapers.keys())
            if available_scrapers:
                print(f"   OK Integrated scrapers: {', '.join(available_scrapers)}")
            else:
                print("   WARNING No scrapers successfully integrated")
            
            return True
        else:
            print("   ERROR Data Manager test failed")
            return False
            
    except Exception as e:
        print(f"   ERROR Data Manager test error: {e}")
        return False

def create_test_configuration():
    """Create test configuration for faster testing"""
    print("\nCreating test configuration...")
    
    test_config = {
        "data_collection": {
            "sentiment": {
                "interval_minutes": 5,
                "enabled": True,
                "retry_attempts": 2,
                "timeout_seconds": 30
            },
            "correlation": {
                "interval_minutes": 5,
                "enabled": True,
                "retry_attempts": 2,
                "timeout_seconds": 30
            },
            "economic_calendar": {
                "interval_minutes": 10,
                "enabled": True,
                "retry_attempts": 2,
                "timeout_seconds": 30
            },
            "cot": {
                "update_day": "friday",
                "update_time": "18:00",
                "enabled": True,
                "historical_weeks": 2,
                "retry_attempts": 2,
                "timeout_seconds": 60
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
    
    # Save test configuration
    config_dir = Path("config")
    config_dir.mkdir(exist_ok=True)
    
    test_config_file = config_dir / "schedules_test.json"
    
    with open(test_config_file, 'w') as f:
        json.dump(test_config, f, indent=2)
    
    print(f"   OK Test configuration saved to {test_config_file}")
    print("   NOTE: To use test config: copy schedules_test.json to schedules.json")
    
    return test_config_file

def run_simple_system_test():
    """Run a simplified system test without subprocess"""
    print("\nRunning simple system test...")
    
    try:
        # Test main system import directly
        if Path("main.py").exists():
            print("   OK main.py found")
            
            # Direct import test (safer than subprocess on Windows)
            print("   Testing system imports...")
            
            # Temporarily redirect stdout to capture any unicode issues
            import io
            import contextlib
            
            # Test if we can import without unicode errors
            test_code = """
import sys
sys.path.insert(0, '.')

# Test basic imports
try:
    from main import ConfigManager
    print("ConfigManager import: OK")
    
    # Try creating config manager with simple print statements
    import os
    os.environ['PYTHONIOENCODING'] = 'utf-8'
    
    config = ConfigManager()
    print("ConfigManager creation: OK")
    print("System test: SUCCESS")
except Exception as e:
    print(f"System test error: {e}")
    print("System test: FAILED")
"""
            
            # Write test script to temporary file
            with open('temp_test.py', 'w', encoding='utf-8') as f:
                f.write(test_code)
            
            # Run test script
            import subprocess
            result = subprocess.run([sys.executable, 'temp_test.py'], 
                                  capture_output=True, text=True, timeout=30)
            
            # Clean up temp file
            if Path('temp_test.py').exists():
                os.remove('temp_test.py')
            
            if "System test: SUCCESS" in result.stdout:
                print("   OK System initialization test passed")
                return True
            else:
                print(f"   ERROR System test failed")
                print(f"   Output: {result.stdout}")
                print(f"   Error: {result.stderr}")
                return False
                
        else:
            print("   ERROR main.py not found")
            return False
            
    except Exception as e:
        print(f"   ERROR Simple test error: {e}")
        return False

def show_next_steps(test_results):
    """Show next steps based on test results"""
    print("\n" + "="*60)
    print("NEXT STEPS")
    print("="*60)
    
    successful_scrapers = sum(1 for result in test_results.values() if result == 'success')
    total_scrapers = len(test_results)
    
    if successful_scrapers == total_scrapers:
        print("SUCCESS: ALL SCRAPERS INTEGRATED!")
        print("\nReady to run the full system:")
        print("   python main.py --test-intervals --data-only")
        print("\nMonitor data collection:")
        print("   - Check logs/data.log for detailed logs")
        print("   - Check data/market_data.json for unified data")
        print("   - Use Ctrl+C to stop gracefully")
        
    elif successful_scrapers > 0:
        print(f"PARTIAL SUCCESS: {successful_scrapers}/{total_scrapers} scrapers integrated")
        print("\nFix remaining issues:")
        
        for scraper, result in test_results.items():
            if result != 'success':
                print(f"   ISSUE: {scraper} - {result}")
        
        print("\nYou can still run with available scrapers:")
        print("   python main.py --test-intervals --data-only")
        
    else:
        print("NO SCRAPERS INTEGRATED SUCCESSFULLY")
        print("\nRequired actions:")
        print("1. Copy your scraper files to scrapers/ directory")
        print("2. Ensure class names match requirements")
        print("3. Fix any import errors shown above")
        print("4. Re-run this setup script")
    
    print("\nFor detailed help, see the Integration Guide")
    print("For troubleshooting, check logs/system.log")

def main():
    """Main setup function"""
    print("Phase 2 Setup - Data Manager Integration")
    print("="*50)
    
    # Step 1: Check current state
    available, missing = check_scraper_files()
    
    # Step 2: Try to copy existing files
    if missing:
        copied = copy_existing_files()
        
        # Re-check after copying
        available, missing = check_scraper_files()
    
    # Step 3: Test scrapers
    test_results = test_scrapers()
    
    # Step 4: Test data manager integration
    data_manager_ok = test_data_manager()
    
    # Step 5: Create test configuration
    test_config_file = create_test_configuration()
    
    # Step 6: Simple system test
    if data_manager_ok:
        system_ok = run_simple_system_test()
    else:
        system_ok = False
    
    # Step 7: Show results and next steps
    print("\n" + "="*50)
    print("SETUP RESULTS")
    print("="*50)
    
    print(f"Available scrapers: {len(available)}")
    print(f"Missing scrapers: {len(missing)}")
    print(f"Test results: {test_results}")
    print(f"Data Manager: {'OK' if data_manager_ok else 'FAILED'}")
    print(f"System Test: {'OK' if system_ok else 'FAILED'}")
    
    show_next_steps(test_results)
    
    if system_ok:
        print("\nPhase 2 setup completed successfully!")
        print("Ready to run the unified data collection system!")
        return 0
    else:
        print("\nPhase 2 setup completed with issues")
        print("Some components may need attention before full operation")
        return 1

if __name__ == "__main__":
    sys.exit(main())