#!/usr/bin/env python3
# ===== SYSTEM SETUP SCRIPT =====
# Run this first to set up the complete directory structure
# Creates all necessary files and folders

import os
import json
import shutil
from pathlib import Path

def create_directory_structure():
    """Create the complete directory structure"""
    directories = [
        "config",
        "core", 
        "scrapers",
        "interfaces",
        "data",
        "logs",
        "backup"
    ]
    
    print("📁 Creating directory structure...")
    for directory in directories:
        Path(directory).mkdir(exist_ok=True)
        print(f"   ✅ Created: {directory}/")
    
    print("✅ Directory structure created successfully")

def create_placeholder_files():
    """Create placeholder files for future development"""
    
    # Core module placeholders
    core_files = [
        "core/__init__.py",
        "core/data_manager.py", 
        "core/trading_engine.py",
        "core/risk_monitor.py"
    ]
    
    # Interface placeholders
    interface_files = [
        "interfaces/__init__.py",
        "interfaces/telegram_bot.py",
        "interfaces/web_dashboard.py",
        "interfaces/cli_interface.py"
    ]
    
    # Scraper placeholders (will be moved from your existing files)
    scraper_files = [
        "scrapers/__init__.py",
        "scrapers/calendar_scraper.py",
        "scrapers/sentiment_scraper.py", 
        "scrapers/correlation_scraper.py",
        "scrapers/cot_scraper.py"
    ]
    
    placeholder_content = '''# ===== PLACEHOLDER MODULE =====
# This module will be implemented in future phases
# Current status: Not implemented

def placeholder_function():
    """Placeholder function - will be implemented later"""
    pass

if __name__ == "__main__":
    print("This module is not yet implemented")
'''
    
    all_files = core_files + interface_files + scraper_files
    
    print("📄 Creating placeholder files...")
    for file_path in all_files:
        path = Path(file_path)
        if not path.exists():
            with open(path, 'w') as f:
                f.write(placeholder_content)
            print(f"   ✅ Created: {file_path}")
        else:
            print(f"   ⏭️ Exists: {file_path}")

def copy_existing_scrapers():
    """Instructions for copying existing scraper files"""
    
    print("\n📋 MANUAL STEPS REQUIRED:")
    print("="*50)
    print("You need to manually copy your existing scraper files:")
    print()
    print("1. Copy 'Calender2.py' → 'scrapers/calendar_scraper.py'")
    print("2. Copy 'Co-relation Data.py' → 'scrapers/correlation_scraper.py'") 
    print("3. Copy 'COT.py' → 'scrapers/cot_scraper.py'")
    print("4. Copy 'sentiment_manager.py' → 'scrapers/sentiment_scraper.py'")
    print()
    print("5. Copy 'Bot_simplified2.py' → 'core/trading_engine.py'")
    print("6. Copy 'emergency_monitor.py' → 'core/risk_monitor.py'")
    print()
    print("These will be refactored in Phase 2 to work with the unified system.")
    print("="*50)

def create_example_data_files():
    """Create example data files"""
    
    # Example market data
    example_market_data = {
        "metadata": {
            "system_name": "Unified Trading System",
            "version": "1.0",
            "created": "2025-07-30T10:00:00"
        },
        "last_updated": "2025-07-30T10:00:00",
        "system_status": "initializing",
        "data_freshness": {
            "economic_calendar": {"fresh": False, "last_update": None},
            "sentiment": {"fresh": False, "last_update": None},
            "correlation": {"fresh": False, "last_update": None},
            "cot": {"fresh": False, "last_update": None}
        },
        "data_sources": {
            "economic_calendar": {"status": "waiting", "events": []},
            "sentiment": {"status": "waiting", "pairs": {}},
            "correlation": {"status": "waiting", "matrix": {}},
            "cot": {"status": "waiting", "financial": {}, "commodity": {}}
        }
    }
    
    # Save example files
    data_files = {
        "data/market_data.json": example_market_data,
        "data/README.md": "# Data Directory\n\nThis directory contains all market data files:\n- market_data.json: Unified market data\n- bot_state.json: Trading bot state\n- emergency.json: Emergency status\n"
    }
    
    print("📊 Creating example data files...")
    for file_path, content in data_files.items():
        path = Path(file_path)
        if not path.exists():
            if file_path.endswith('.json'):
                with open(path, 'w') as f:
                    json.dump(content, f, indent=2)
            else:
                with open(path, 'w') as f:
                    f.write(content)
            print(f"   ✅ Created: {file_path}")

def create_run_scripts():
    """Create convenient run scripts"""
    
    # Windows batch script
    windows_script = '''@echo off
echo Starting Unified Trading System...
python main.py
pause
'''
    
    # Linux shell script  
    linux_script = '''#!/bin/bash
echo "Starting Unified Trading System..."
python3 main.py
'''
    
    scripts = {
        "run_windows.bat": windows_script,
        "run_linux.sh": linux_script
    }
    
    print("🚀 Creating run scripts...")
    for script_name, script_content in scripts.items():
        with open(script_name, 'w') as f:
            f.write(script_content)
        
        # Make Linux script executable
        if script_name.endswith('.sh'):
            os.chmod(script_name, 0o755)
        
        print(f"   ✅ Created: {script_name}")

def create_readme():
    """Create comprehensive README"""
    
    readme_content = '''# Unified Trading System

## Overview
Complete trading system that unifies data collection, analysis, and trading execution.

## Quick Start

### 1. Setup (First Time)
```bash
python setup.py
```

### 2. Copy Your Existing Files
Follow the instructions displayed after running setup.py

### 3. Start System
```bash
python main.py
```

Or use the convenience scripts:
- Windows: `run_windows.bat`
- Linux: `./run_linux.sh`

## Directory Structure

```
TradingSystem/
├── main.py                 # Main entry point
├── setup.py               # Setup script
├── config/
│   ├── settings.json       # System configuration
│   ├── pairs.json         # Trading pairs setup
│   └── schedules.json     # Update schedules
├── core/
│   ├── trading_hub.py     # Main coordinator
│   ├── data_manager.py    # Data collection manager
│   ├── trading_engine.py  # Trading bot
│   └── risk_monitor.py    # Risk monitoring
├── scrapers/              # Data scrapers
│   ├── calendar_scraper.py
│   ├── sentiment_scraper.py
│   ├── correlation_scraper.py
│   └── cot_scraper.py
├── interfaces/            # User interfaces
│   ├── telegram_bot.py    # Telegram interface
│   ├── web_dashboard.py   # Web interface
│   └── cli_interface.py   # Command line
├── data/                  # Data files
│   ├── market_data.json   # Unified market data
│   ├── bot_state.json     # Trading state
│   └── emergency.json     # Emergency status
└── logs/                  # Log files
    ├── system.log         # System logs
    ├── trading.log        # Trading logs
    └── data.log          # Data collection logs
```

## Command Line Options

```bash
python main.py --help                 # Show all options
python main.py --debug               # Enable debug mode
python main.py --dry-run             # Simulation mode
python main.py --test-intervals      # Use reduced intervals for testing
```

## Configuration

All settings are in `config/settings.json`. Key sections:

- **MT5 Settings**: Account number, magic number
- **Trading Settings**: Risk management, martingale settings
- **Data Collection**: Update intervals, sources
- **Telegram**: Bot token, chat ID
- **Testing**: Reduced intervals, dry run mode

## Development Phases

- ✅ **Phase 1**: Core foundation and configuration
- 🔄 **Phase 2**: Data integration and unified collection
- ⏳ **Phase 3**: Trading integration and risk monitoring  
- ⏳ **Phase 4**: Telegram bot and remote control
- ⏳ **Phase 5**: Web dashboard and production deployment

## Monitoring

- **Logs**: Check `logs/` directory for detailed logs
- **Data**: Check `data/market_data.json` for current data status
- **Status**: System reports status every 5 minutes

## Emergency Controls

- **Ctrl+C**: Graceful shutdown
- **Emergency File**: Create `data/shutdown_signal.json` to trigger shutdown
- **Risk Monitor**: Automatic emergency stop on high drawdown

## Support

Check logs for errors and status information. Each component logs to its own file in the `logs/` directory.
'''
    
    with open("README.md", 'w') as f:
        f.write(readme_content)
    
    print("📖 Created README.md")

def main():
    """Main setup function"""
    print("🚀 Setting up Unified Trading System...")
    print("="*50)
    
    # Create directory structure
    create_directory_structure()
    
    # Create placeholder files
    create_placeholder_files()
    
    # Create example data files
    create_example_data_files()
    
    # Create run scripts
    create_run_scripts()
    
    # Create README
    create_readme()
    
    print("\n" + "="*50)
    print("✅ SETUP COMPLETE!")
    print("="*50)
    
    # Show next steps
    copy_existing_scrapers()
    
    print("\n🚀 AFTER COPYING FILES:")
    print("1. Run: python main.py")
    print("2. Check logs/ for any errors")
    print("3. Check config/ to modify settings")
    print("4. The system will start with placeholders for now")
    print("\n📱 Ready for Phase 2: Data Integration!")

if __name__ == "__main__":
    main()