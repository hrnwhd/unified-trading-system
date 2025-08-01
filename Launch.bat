@echo off
echo ========================================
echo   Enhanced Trading System - Quick Setup
echo ========================================
echo.

REM Check if Python is available
python --version >nul 2>&1
if errorlevel 1 (
    echo ❌ Python not found. Please install Python first.
    pause
    exit /b 1
)

echo ✅ Python found
echo.

REM Run the setup
echo 🚀 Running Phase 3 setup...
python launch_enhanced_system.py setup

if errorlevel 1 (
    echo.
    echo ❌ Setup failed. Check the output above for details.
    echo 💡 Try running: python launch_enhanced_system.py status
    pause
    exit /b 1
)

echo.
echo ✅ Setup completed successfully!
echo.
echo 🎛️ Next step: Configure trading mode
choice /c 12345 /m "Choose mode: [1]Pure TA [2]Conservative [3]Full Intel [4]Aggressive [5]Protection"

if errorlevel 5 goto protection
if errorlevel 4 goto aggressive  
if errorlevel 3 goto full_intel
if errorlevel 2 goto conservative
if errorlevel 1 goto pure_ta

:pure_ta
echo 🔧 Setting Pure TA Mode...
python launch_enhanced_system.py modes
goto end

:conservative
echo 🛡️ Setting Conservative Mode...
python -c "import sys; sys.path.insert(0, 'config'); from enhanced_config_manager import EnhancedConfigManager; mgr = EnhancedConfigManager(); mgr.set_conservative_mode()"
echo ✅ Conservative mode activated
goto end

:full_intel
echo 🧠 Setting Full Intelligence Mode...
python -c "import sys; sys.path.insert(0, 'config'); from enhanced_config_manager import EnhancedConfigManager; mgr = EnhancedConfigManager(); mgr.set_full_intelligence_mode()"
echo ✅ Full Intelligence mode activated
goto end

:aggressive
echo ⚡ Setting Aggressive Mode...
python -c "import sys; sys.path.insert(0, 'config'); from enhanced_config_manager import EnhancedConfigManager; mgr = EnhancedConfigManager(); mgr.set_aggressive_mode()"
echo ✅ Aggressive mode activated
goto end

:protection
echo 🔒 Setting Martingale Protection Mode...
python -c "import sys; sys.path.insert(0, 'config'); from enhanced_config_manager import EnhancedConfigManager; mgr = EnhancedConfigManager(); mgr.set_martingale_protection_mode()"
echo ✅ Martingale Protection mode activated
goto end

:end
echo.
echo 🎉 Setup complete! You can now start trading:
echo.
echo To start Enhanced Trading:
echo   python launch_enhanced_system.py enhanced
echo.
echo To start Original System:
echo   python launch_enhanced_system.py original
echo.
echo To check status anytime:
echo   python launch_enhanced_system.py status
echo.
pause