#!/usr/bin/env python3
# ===== WINDOWS TEST SCRIPT =====
# Quick test for Windows Unicode issues

import os
import sys

# Fix Windows UTF-8 encoding
if os.name == 'nt':  # Windows
    import codecs
    sys.stdout = codecs.getwriter('utf-8')(sys.stdout.detach())
    sys.stderr = codecs.getwriter('utf-8')(sys.stderr.detach())

def test_basic_system():
    """Test basic system functionality"""
    print("Testing basic system functionality...")
    
    try:
        # Test 1: Import main system
        print("Test 1: Importing main system...")
        from main import ConfigManager, TradingSystemManager
        print("   OK: Main system imports successful")
        
        # Test 2: Create config manager
        print("Test 2: Creating config manager...")
        config = ConfigManager()
        print("   OK: Config manager created successfully")
        
        # Test 3: Check data manager
        print("Test 3: Testing data manager...")
        sys.path.insert(0, 'core')
        from data_manager import DataManager
        data_manager = DataManager(config, None)  # None logger for testing
        print(f"   OK: Data manager created with {len(data_manager.scrapers)} scrapers")
        
        # Test 4: Check scraper files exist
        print("Test 4: Checking scraper files...")
        from pathlib import Path
        scrapers_dir = Path("scrapers")
        scraper_files = [
            'calendar_scraper.py',
            'sentiment_scraper.py', 
            'correlation_scraper.py',
            'cot_scraper.py'
        ]
        
        found_files = []
        for file in scraper_files:
            if (scrapers_dir / file).exists():
                found_files.append(file)
                print(f"   OK: {file} found")
            else:
                print(f"   MISSING: {file}")
        
        print(f"\nSUMMARY:")
        print(f"   Found scrapers: {len(found_files)}/{len(scraper_files)}")
        print(f"   Integrated scrapers: {len(data_manager.scrapers)}")
        
        if len(found_files) == len(scraper_files) and len(data_manager.scrapers) > 0:
            print("\nSTATUS: READY FOR TESTING")
            print("Next step: python main.py --test-intervals --data-only")
            return True
        else:
            print("\nSTATUS: ISSUES DETECTED")
            print("Check scraper files and integration")
            return False
            
    except Exception as e:
        print(f"ERROR: {e}")
        import traceback
        traceback.print_exc()
        return False

def test_data_collection():
    """Test data collection functionality"""
    print("\nTesting data collection...")
    
    try:
        # Import system components
        from main import TradingSystemManager
        
        # Create system manager
        system_manager = TradingSystemManager()
        
        if system_manager.data_manager:
            print("   OK: Data manager available")
            
            # Test force update
            print("   Testing force update...")
            success = system_manager.data_manager.force_update('sentiment')
            print(f"   Force update result: {'SUCCESS' if success else 'FAILED'}")
            
            # Check market data file
            from pathlib import Path
            market_data_file = Path("data/market_data.json")
            if market_data_file.exists():
                print("   OK: Market data file exists")
                
                # Load and check contents
                import json
                with open(market_data_file, 'r', encoding='utf-8') as f:
                    market_data = json.load(f)
                
                system_status = market_data.get('system_status', 'unknown')
                print(f"   System status: {system_status}")
                
                # Check data sources
                data_sources = market_data.get('data_sources', {})
                for source_name, source_data in data_sources.items():
                    status = source_data.get('status', 'unknown')
                    print(f"   {source_name}: {status}")
                
                return True
            else:
                print("   ERROR: Market data file not found")
                return False
        else:
            print("   ERROR: Data manager not available")
            return False
            
    except Exception as e:
        print(f"   ERROR: {e}")
        return False

def main():
    """Main test function"""
    print("="*60)
    print("WINDOWS COMPATIBILITY TEST")
    print("="*60)
    
    # Test 1: Basic system
    basic_ok = test_basic_system()
    
    # Test 2: Data collection (if basic works)
    if basic_ok:
        data_ok = test_data_collection()
    else:
        data_ok = False
    
    print("\n" + "="*60)
    print("TEST RESULTS")
    print("="*60)
    print(f"Basic System: {'PASS' if basic_ok else 'FAIL'}")
    print(f"Data Collection: {'PASS' if data_ok else 'FAIL'}")
    
    if basic_ok and data_ok:
        print("\nSTATUS: ALL TESTS PASSED")
        print("System is ready for operation!")
        print("\nNext steps:")
        print("1. python main.py --test-intervals --data-only")
        print("2. Monitor logs/data.log for data collection")
        print("3. Check data/market_data.json for results")
        return 0
    else:
        print("\nSTATUS: TESTS FAILED")
        print("System needs attention before operation")
        if not basic_ok:
            print("- Fix basic system setup issues")
        if not data_ok:
            print("- Fix data collection issues")
        return 1

if __name__ == "__main__":
    sys.exit(main())