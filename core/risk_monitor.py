# ===== STANDALONE EMERGENCY MONITOR =====
# Separate program that monitors account and creates emergency stop file
# Your bot reads this file and stops if emergency detected

import MetaTrader5 as mt5
import json
import time
from datetime import datetime, timedelta
import logging
import os

# ===== CONFIGURATION =====
ACCOUNT_NUMBER = 102820128
MAGIC_NUMBER = 80808088

# AGGRESSIVE (More risk, less protection)
FREE_MARGIN_THRESHOLD = 20       # 20% of used margin as free margin
RUNNING_LOSS_THRESHOLD = 50      # 50% total account loss  
SINGLE_PAIR_LOSS_THRESHOLD = 40  # 40% loss per pair
MARGIN_LEVEL_THRESHOLD = 120     # 120% margin level minimum

# Monitor settings
CHECK_INTERVAL = 30              # Check every 30 seconds
EMERGENCY_FILE = "emergency_stop.json"
LOG_FILE = "emergency_monitor.log"

# ===== LOGGING SETUP =====
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - EMERGENCY_MONITOR - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(LOG_FILE, encoding='utf-8'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

# ===== EMERGENCY STATUS MANAGER =====
class EmergencyStatusManager:
    def __init__(self):
        self.initial_balance = None
        self.emergency_active = False
        self.alerts_sent = set()
        self.last_alert_time = {}
        
    def save_emergency_status(self, status_data):
        """Save emergency status to file for bot to read"""
        try:
            with open(EMERGENCY_FILE, 'w') as f:
                json.dump(status_data, f, indent=2)
            return True
        except Exception as e:
            logger.error(f"Failed to save emergency status: {e}")
            return False
    
    def clear_emergency_status(self):
        """Clear emergency file"""
        try:
            if os.path.exists(EMERGENCY_FILE):
                os.remove(EMERGENCY_FILE)
                logger.info("Emergency status cleared")
        except Exception as e:
            logger.error(f"Failed to clear emergency status: {e}")
    
    def check_account_safety(self):
        """Main safety check function"""
        account_info = mt5.account_info()
        if not account_info:
            logger.error("Cannot get account info")
            return
        
        # Initialize balance tracking
        if self.initial_balance is None:
            self.initial_balance = account_info.balance
            logger.info(f"Initial balance set: ${self.initial_balance:.2f}")
        
        emergency_reasons = []
        warnings = []
        
        # 1. Free Margin Check
        if account_info.margin > 0:
            free_margin_pct = (account_info.margin_free / account_info.margin) * 100
            
            if free_margin_pct < 100:  # Critical
                emergency_reasons.append(f"CRITICAL_FREE_MARGIN: {free_margin_pct:.1f}%")
            elif free_margin_pct < FREE_MARGIN_THRESHOLD:  # Warning
                warnings.append(f"LOW_FREE_MARGIN: {free_margin_pct:.1f}%")
        
        # 2. Margin Level Check  
        if account_info.margin > 0:
            margin_level = (account_info.equity / account_info.margin) * 100
            
            if margin_level < 120:  # Critical
                emergency_reasons.append(f"CRITICAL_MARGIN_LEVEL: {margin_level:.1f}%")
            elif margin_level < MARGIN_LEVEL_THRESHOLD:  # Warning
                warnings.append(f"LOW_MARGIN_LEVEL: {margin_level:.1f}%")
        
        # 3. Running Loss Check
        if self.initial_balance:
            running_loss = self.initial_balance - account_info.equity
            running_loss_pct = (running_loss / self.initial_balance) * 100
            
            if running_loss_pct > 30:  # Critical
                emergency_reasons.append(f"CRITICAL_RUNNING_LOSS: {running_loss_pct:.1f}%")
            elif running_loss_pct > RUNNING_LOSS_THRESHOLD:  # Warning
                warnings.append(f"HIGH_RUNNING_LOSS: {running_loss_pct:.1f}%")
        
        # 4. Individual Pair Loss Check
        pair_losses = self.get_pair_losses(account_info)
        for pair, loss_pct in pair_losses.items():
            if loss_pct > 25:  # Critical
                emergency_reasons.append(f"CRITICAL_{pair}_LOSS: {loss_pct:.1f}%")
            elif loss_pct > SINGLE_PAIR_LOSS_THRESHOLD:  # Warning
                warnings.append(f"HIGH_{pair}_LOSS: {loss_pct:.1f}%")
        
        # Create status data
        status_data = {
            'timestamp': datetime.now().isoformat(),
            'account_number': ACCOUNT_NUMBER,
            'emergency_active': len(emergency_reasons) > 0,
            'emergency_reasons': emergency_reasons,
            'warnings': warnings,
            'account_status': {
                'balance': account_info.balance,
                'equity': account_info.equity,
                'margin': account_info.margin,
                'free_margin': account_info.margin_free,
                'margin_level': (account_info.equity / account_info.margin * 100) if account_info.margin > 0 else 0,
                'free_margin_pct': (account_info.margin_free / account_info.margin * 100) if account_info.margin > 0 else 0
            },
            'pair_losses': pair_losses
        }
        
        # Save status for bot to read
        self.save_emergency_status(status_data)
        
        # Handle emergency
        if emergency_reasons:
            if not self.emergency_active:
                self.emergency_active = True
                logger.critical("🚨🚨🚨 EMERGENCY STOP ACTIVATED 🚨🚨🚨")
                for reason in emergency_reasons:
                    logger.critical(f"   REASON: {reason}")
            
        elif self.emergency_active and not emergency_reasons:
            # Emergency cleared
            self.emergency_active = False
            logger.info("✅ Emergency conditions cleared")
        
        # Send warnings (with rate limiting)
        for warning in warnings:
            self.send_rate_limited_alert(warning)
        
        # Log status every 5 minutes
        if datetime.now().minute % 5 == 0 and datetime.now().second < 30:
            logger.info(f"📊 Status: Balance=${account_info.balance:.2f}, "
                       f"Equity=${account_info.equity:.2f}, "
                       f"Margin Level={status_data['account_status']['margin_level']:.1f}%, "
                       f"Free Margin={status_data['account_status']['free_margin_pct']:.1f}%")
    
    def get_pair_losses(self, account_info):
        """Calculate loss for each currency pair"""
        pair_losses = {}
        
        # Get all positions
        positions = mt5.positions_get()
        if not positions:
            return pair_losses
        
        # Group by symbol
        symbol_profits = {}
        for pos in positions:
            if pos.magic == MAGIC_NUMBER:
                symbol = pos.symbol
                if symbol not in symbol_profits:
                    symbol_profits[symbol] = 0
                symbol_profits[symbol] += pos.profit
        
        # Calculate loss percentages
        for symbol, profit in symbol_profits.items():
            if profit < 0:  # Only losses
                loss_pct = abs(profit / account_info.balance) * 100
                pair_losses[symbol] = round(loss_pct, 2)
        
        return pair_losses
    
    def send_rate_limited_alert(self, alert):
        """Send alert with rate limiting (max once per hour)"""
        alert_type = alert.split(':')[0]
        last_sent = self.last_alert_time.get(alert_type, datetime.min)
        
        if datetime.now() - last_sent > timedelta(hours=1):
            logger.warning(f"⚠️ {alert}")
            self.last_alert_time[alert_type] = datetime.now()

# ===== MAIN MONITOR LOOP =====
def run_emergency_monitor():
    """Main monitoring function"""
    logger.info("🚨 Emergency Monitor Starting...")
    
    # Initialize MT5
    if not mt5.initialize():
        logger.error("MT5 initialization failed")
        return
    
    # Validate account
    account_info = mt5.account_info()
    if not account_info or account_info.login != ACCOUNT_NUMBER:
        logger.error(f"Account validation failed. Expected: {ACCOUNT_NUMBER}")
        mt5.shutdown()
        return
    
    logger.info(f"✅ Connected to account: {account_info.login}")
    logger.info(f"💰 Starting balance: ${account_info.balance:.2f}")
    
    # Initialize emergency manager
    emergency_manager = EmergencyStatusManager()
    
    # Clear any existing emergency file
    emergency_manager.clear_emergency_status()
    
    consecutive_errors = 0
    max_errors = 10
    
    try:
        while True:
            try:
                # Check MT5 connection
                if not mt5.terminal_info():
                    logger.warning("MT5 disconnected, attempting reconnect...")
                    if not mt5.initialize():
                        logger.error("Reconnection failed")
                        consecutive_errors += 1
                        if consecutive_errors >= max_errors:
                            break
                        time.sleep(30)
                        continue
                
                # Run safety checks
                emergency_manager.check_account_safety()
                consecutive_errors = 0  # Reset on success
                
                # Sleep until next check
                time.sleep(CHECK_INTERVAL)
                
            except Exception as e:
                consecutive_errors += 1
                logger.error(f"Error in monitor loop: {e}")
                
                if consecutive_errors >= max_errors:
                    logger.critical(f"Too many consecutive errors ({consecutive_errors}) - stopping monitor")
                    break
                
                time.sleep(10)  # Wait before retry
                
    except KeyboardInterrupt:
        logger.info("🛑 Emergency monitor stopped by user")
    
    finally:
        # Cleanup
        try:
            emergency_manager.clear_emergency_status()
            mt5.shutdown()
            logger.info("Emergency monitor shutdown complete")
        except Exception as e:
            logger.error(f"Error during shutdown: {e}")

if __name__ == "__main__":
    run_emergency_monitor()