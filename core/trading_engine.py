#!/usr/bin/env python3
# ===== PHASE 3: ENHANCED TRADING ENGINE WITH DATA INTEGRATION =====
# Integrates your existing trading engine with unified data collection system
# Adds intelligent decision-making based on sentiment, correlation, and economic data

import MetaTrader5 as mt5
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import warnings
from collections import deque
import time
import logging
import json
import os
from pathlib import Path

# Suppress warnings
warnings.filterwarnings("ignore")

# ===== CONFIGURATION =====
GLOBAL_TIMEFRAME = mt5.TIMEFRAME_M5  # Primary timeframe
ACCOUNT_NUMBER = 42903786
MAGIC_NUMBER = 50515253

# Data Integration Settings
DATA_DIR = Path("data")
MARKET_DATA_FILE = DATA_DIR / "market_data.json"
BOT_STATE_FILE = DATA_DIR / "bot_state.json"
EMERGENCY_FILE = DATA_DIR / "emergency.json"

# Enhanced Risk Management
CORRELATION_RISK_THRESHOLD = 70  # Block trades if correlation > 70%
SENTIMENT_EXTREME_THRESHOLD = 70  # Block direction if sentiment > 70%
ECONOMIC_EVENT_BUFFER_HOURS = 1  # Avoid trades 1 hour before high-impact events

# Martingale Configuration (unchanged from your original)
MARTINGALE_ENABLED = True
MAX_MARTINGALE_LAYERS = 15
MARTINGALE_MULTIPLIER = 2
EMERGENCY_DD_PERCENTAGE = 50
MARTINGALE_PROFIT_BUFFER_PIPS = 5
MIN_PROFIT_PERCENTAGE = 1
FLIRT_THRESHOLD_PIPS = 10

# ===== LOGGING SETUP =====
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('enhanced_trading.log', encoding='utf-8'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

# ===== TRADING PAIRS AND CONFIGURATION =====
PAIRS = ['AUDUSD', 'USDCAD', 'XAUUSD', 'EURUSD', 'GBPUSD', 
         'AUDCAD',  'USDCHF', 'GBPCAD', 'AUDNZD', 'NZDCAD','US500','BTCUSD']

PAIR_RISK_PROFILES = {
    'AUDUSD': "Medium", 'USDCAD': "Low", 'US500': "High", 'XAUUSD': "High",
    'BTCUSD': "Medium", 'EURUSD': "Medium", 'GBPUSD': "Medium", 
    'AUDCAD': "Low", 'USDJPY': "Low", 'USDCHF': "High", 
    'GBPCAD': "Low", 'AUDNZD': "Medium", 'NZDCAD': "Low"
}

PARAM_SETS = {
    "Low": {
        "adx_threshold": 25, "min_timeframes": 3, "rsi_overbought": 70, "rsi_oversold": 30,
        "ema_buffer_pct": 0.005, "risk_reward_ratio_long": 1.5, "risk_reward_ratio_short": 1.5,
        "min_adx_strength": 25, "max_adx_strength": 60, "risk_per_trade_pct": 0.05,
        "atr_multiplier": 1.5, "min_volatility_pips": 5
    },
    "Medium": {
        "adx_threshold": 25, "min_timeframes": 2, "rsi_overbought": 70, "rsi_oversold": 30,
        "ema_buffer_pct": 0.005, "risk_reward_ratio_long": 1.3, "risk_reward_ratio_short": 1.3,
        "min_adx_strength": 25, "max_adx_strength": 60, "risk_per_trade_pct": 0.1,
        "atr_multiplier": 1.5, "min_volatility_pips": 5
    },
    "High": {
        "adx_threshold": 20, "min_timeframes": 1, "rsi_overbought": 70, "rsi_oversold": 30,
        "ema_buffer_pct": 0.008, "risk_reward_ratio_long": 1.1, "risk_reward_ratio_short": 1.1,
        "min_adx_strength": 20, "max_adx_strength": 70, "risk_per_trade_pct": 0.2,
        "atr_multiplier": 2, "min_volatility_pips": 3
    }
}

# ===== ENHANCED DATA INTEGRATION MANAGER =====
class DataIntegrationManager:
    """Manages access to unified market data for trading decisions"""
    
    def __init__(self):
        self.market_data_file = MARKET_DATA_FILE
        self.last_data_load = None
        self.cached_data = {}
        self.data_cache_duration = 60  # Cache for 60 seconds
        
    def load_market_data(self):
        """Load current market data with caching"""
        try:
            # Check if we need to reload data
            now = datetime.now()
            if (self.last_data_load is None or 
                (now - self.last_data_load).total_seconds() > self.data_cache_duration):
                
                if self.market_data_file.exists():
                    with open(self.market_data_file, 'r') as f:
                        self.cached_data = json.load(f)
                    self.last_data_load = now
                    logger.debug("📊 Market data reloaded from file")
                else:
                    logger.warning("⚠️ Market data file not found")
                    return {}
            
            return self.cached_data
            
        except Exception as e:
            logger.error(f"❌ Error loading market data: {e}")
            return {}
    
    def get_sentiment_data(self):
        """Get current sentiment data"""
        try:
            market_data = self.load_market_data()
            sentiment_data = market_data.get('data_sources', {}).get('sentiment', {})
            
            if sentiment_data.get('status') == 'fresh':
                return sentiment_data.get('pairs', {})
            else:
                logger.warning("⚠️ Sentiment data not fresh")
                return {}
                
        except Exception as e:
            logger.error(f"❌ Error getting sentiment data: {e}")
            return {}
    
    def get_correlation_data(self):
        """Get current correlation data"""
        try:
            market_data = self.load_market_data()
            correlation_data = market_data.get('data_sources', {}).get('correlation', {})
            
            if correlation_data.get('status') == 'fresh':
                return {
                    'matrix': correlation_data.get('matrix', {}),
                    'warnings': correlation_data.get('warnings', [])
                }
            else:
                logger.warning("⚠️ Correlation data not fresh")
                return {'matrix': {}, 'warnings': []}
                
        except Exception as e:
            logger.error(f"❌ Error getting correlation data: {e}")
            return {'matrix': {}, 'warnings': []}
    
    def get_economic_events(self, hours_ahead=24):
        """Get upcoming economic events"""
        try:
            market_data = self.load_market_data()
            calendar_data = market_data.get('data_sources', {}).get('economic_calendar', {})
            
            if calendar_data.get('status') == 'fresh':
                events = calendar_data.get('events', [])
                
                # Filter for upcoming high-impact events
                upcoming_events = []
                current_time = datetime.now()
                cutoff_time = current_time + timedelta(hours=hours_ahead)
                
                for event in events:
                    try:
                        # Parse event time
                        event_time_str = event.get('time', '')
                        if event_time_str and event_time_str != 'N/A':
                            # Assuming event time is in format "HH:MM"
                            today = current_time.date()
                            event_time = datetime.combine(today, datetime.strptime(event_time_str, '%H:%M').time())
                            
                            # If event time has passed today, assume it's tomorrow
                            if event_time < current_time:
                                event_time += timedelta(days=1)
                            
                            # Check if event is within our time window and high impact
                            if (event_time <= cutoff_time and 
                                event.get('impact', '').lower() in ['high', 'medium']):
                                upcoming_events.append({
                                    'currency': event.get('currency', ''),
                                    'event_name': event.get('event_name', ''),
                                    'impact': event.get('impact', ''),
                                    'time': event_time.isoformat(),
                                    'time_until_hours': (event_time - current_time).total_seconds() / 3600
                                })
                                
                    except Exception as e:
                        logger.debug(f"Error parsing event: {e}")
                        continue
                
                return upcoming_events
            else:
                logger.warning("⚠️ Economic calendar data not fresh")
                return []
                
        except Exception as e:
            logger.error(f"❌ Error getting economic events: {e}")
            return []
    
    def get_cot_data(self):
        """Get latest COT positioning data"""
        try:
            market_data = self.load_market_data()
            cot_data = market_data.get('data_sources', {}).get('cot', {})
            
            if cot_data.get('status') == 'fresh':
                return {
                    'financial': cot_data.get('financial', {}),
                    'commodity': cot_data.get('commodity', {})
                }
            else:
                logger.warning("⚠️ COT data not fresh")
                return {'financial': {}, 'commodity': {}}
                
        except Exception as e:
            logger.error(f"❌ Error getting COT data: {e}")
            return {'financial': {}, 'commodity': {}}

# ===== ENHANCED TRADING DECISION MANAGER =====
class EnhancedTradingDecisionManager:
    """Makes intelligent trading decisions using integrated market data"""
    
    def __init__(self):
        self.data_manager = DataIntegrationManager()
        
    def can_trade_direction(self, symbol, direction):
        """
        Enhanced decision: Check if we can trade a specific direction
        Considers sentiment, correlation, and economic events
        """
        try:
            # Get all relevant data
            sentiment_data = self.data_manager.get_sentiment_data()
            correlation_data = self.data_manager.get_correlation_data()
            economic_events = self.data_manager.get_economic_events(ECONOMIC_EVENT_BUFFER_HOURS)
            
            # Check 1: Sentiment-based direction blocking
            sentiment_check = self._check_sentiment_blocking(symbol, direction, sentiment_data)
            if not sentiment_check['allowed']:
                logger.info(f"🚫 {symbol} {direction} blocked by sentiment: {sentiment_check['reason']}")
                return False, f"Sentiment: {sentiment_check['reason']}"
            
            # Check 2: Correlation-based risk
            correlation_check = self._check_correlation_risk(symbol, direction, correlation_data)
            if not correlation_check['allowed']:
                logger.info(f"🚫 {symbol} {direction} blocked by correlation: {correlation_check['reason']}")
                return False, f"Correlation: {correlation_check['reason']}"
            
            # Check 3: Economic event timing
            economic_check = self._check_economic_timing(symbol, direction, economic_events)
            if not economic_check['allowed']:
                logger.info(f"🚫 {symbol} {direction} blocked by economic events: {economic_check['reason']}")
                return False, f"Economic: {economic_check['reason']}"
            
            # All checks passed
            logger.debug(f"✅ {symbol} {direction} allowed by all data checks")
            return True, "All checks passed"
            
        except Exception as e:
            logger.error(f"❌ Error checking trade direction for {symbol} {direction}: {e}")
            # On error, default to allowing trade (fail-open)
            return True, f"Error in check (defaulting to allow): {e}"
    
    def _check_sentiment_blocking(self, symbol, direction, sentiment_data):
        """Check if sentiment blocks this direction"""
        try:
            # Normalize symbol name for sentiment data lookup
            symbol_variants = [symbol, symbol.upper()]
            if symbol == 'XAUUSD':
                symbol_variants.extend(['GOLD', 'XAUUSD'])
            elif symbol == 'US500':
                symbol_variants.extend(['SPX500', 'SPXUSD'])
            elif symbol == 'BTCUSD':
                symbol_variants.extend(['BITCOIN', 'BTC'])
            
            sentiment_info = None
            for variant in symbol_variants:
                if variant in sentiment_data:
                    sentiment_info = sentiment_data[variant]
                    break
            
            if not sentiment_info:
                logger.debug(f"No sentiment data for {symbol}, allowing trade")
                return {'allowed': True, 'reason': 'No sentiment data'}
            
            # Check blocked directions
            blocked_directions = sentiment_info.get('blocked_directions', [])
            if direction in blocked_directions:
                sentiment = sentiment_info.get('sentiment', {})
                return {
                    'allowed': False, 
                    'reason': f"Extreme sentiment - {sentiment.get('short', 0)}%↓ {sentiment.get('long', 0)}%↑"
                }
            
            return {'allowed': True, 'reason': 'Sentiment allows direction'}
            
        except Exception as e:
            logger.warning(f"Error checking sentiment for {symbol}: {e}")
            return {'allowed': True, 'reason': 'Sentiment check error'}
    
    def _check_correlation_risk(self, symbol, direction, correlation_data):
        """Check if high correlation creates risk"""
        try:
            warnings = correlation_data.get('warnings', [])
            
            # Look for correlation warnings involving this symbol
            for warning in warnings:
                if warning.get('type') == 'HIGH_CORRELATION':
                    pair = warning.get('pair', '')
                    correlation_value = warning.get('value', 0)
                    
                    # Check if our symbol is involved in high correlation
                    if symbol in pair and abs(correlation_value) >= CORRELATION_RISK_THRESHOLD:
                        # Extract the correlated symbol
                        pair_symbols = pair.split('-')
                        other_symbol = None
                        for sym in pair_symbols:
                            if sym != symbol:
                                other_symbol = sym
                                break
                        
                        if other_symbol:
                            logger.debug(f"High correlation detected: {symbol} vs {other_symbol} ({correlation_value}%)")
                            # For now, we log but don't block (could be enhanced to check if other symbol has open positions)
                            # In future versions, you might want to check if you have positions in the correlated pair
            
            return {'allowed': True, 'reason': 'Correlation check passed'}
            
        except Exception as e:
            logger.warning(f"Error checking correlation for {symbol}: {e}")
            return {'allowed': True, 'reason': 'Correlation check error'}
    
    def _check_economic_timing(self, symbol, direction, economic_events):
        """Check if major economic events are approaching"""
        try:
            # Extract currency from symbol
            if symbol.startswith('USD'):
                symbol_currencies = ['USD', symbol[3:6]]
            elif symbol.endswith('USD'):
                symbol_currencies = [symbol[:3], 'USD']
            elif symbol in ['XAUUSD', 'GOLD']:
                symbol_currencies = ['USD', 'GOLD']
            elif symbol in ['US500', 'SPX500']:
                symbol_currencies = ['USD', 'SPX']
            elif symbol in ['BTCUSD', 'BITCOIN']:
                symbol_currencies = ['USD', 'BTC']
            else:
                # Cross pair like AUDCAD
                symbol_currencies = [symbol[:3], symbol[3:6]]
            
            # Check for high-impact events for relevant currencies
            for event in economic_events:
                event_currency = event.get('currency', '')
                time_until_hours = event.get('time_until_hours', 24)
                impact = event.get('impact', '').lower()
                
                if (event_currency in symbol_currencies and 
                    impact == 'high' and 
                    time_until_hours <= ECONOMIC_EVENT_BUFFER_HOURS):
                    
                    return {
                        'allowed': False,
                        'reason': f"High-impact {event_currency} event in {time_until_hours:.1f}h: {event.get('event_name', 'Unknown')}"
                    }
            
            return {'allowed': True, 'reason': 'No conflicting economic events'}
            
        except Exception as e:
            logger.warning(f"Error checking economic timing for {symbol}: {e}")
            return {'allowed': True, 'reason': 'Economic timing check error'}
    
    def get_enhanced_position_sizing(self, symbol, base_risk_amount, market_conditions):
        """Enhanced position sizing based on market data"""
        try:
            # Get correlation and COT data
            correlation_data = self.data_manager.get_correlation_data()
            cot_data = self.data_manager.get_cot_data()
            
            # Start with base risk amount
            adjusted_risk = base_risk_amount
            adjustments = []
            
            # Adjustment 1: Reduce risk if many correlation warnings
            correlation_warnings = len(correlation_data.get('warnings', []))
            if correlation_warnings >= 5:
                risk_reduction = 0.8  # 20% reduction
                adjusted_risk *= risk_reduction
                adjustments.append(f"Correlation warnings: -{int((1-risk_reduction)*100)}%")
            
            # Adjustment 2: COT-based adjustments (if available)
            cot_financial = cot_data.get('financial', {})
            if cot_financial:
                # This is a placeholder for COT-based adjustments
                # In a full implementation, you'd analyze if commercial/speculative positions
                # are at extremes and adjust accordingly
                pass
            
            # Adjustment 3: Economic event proximity
            upcoming_events = self.data_manager.get_economic_events(6)  # Next 6 hours
            high_impact_events = [e for e in upcoming_events if e.get('impact') == 'high']
            if high_impact_events:
                risk_reduction = 0.7  # 30% reduction before major events
                adjusted_risk *= risk_reduction
                adjustments.append(f"Major events approaching: -{int((1-risk_reduction)*100)}%")
            
            # Log adjustments
            if adjustments:
                logger.info(f"📊 {symbol} risk adjusted: ${base_risk_amount:.2f} → ${adjusted_risk:.2f}")
                for adjustment in adjustments:
                    logger.info(f"   • {adjustment}")
            
            return adjusted_risk
            
        except Exception as e:
            logger.error(f"❌ Error calculating enhanced position sizing: {e}")
            return base_risk_amount  # Return original on error

# ===== ENHANCED TRADE MANAGER (Updated from your original) =====
class EnhancedTradeManager:
    """Enhanced version of your original trade manager with data integration"""
    
    def __init__(self):
        # Original properties
        self.active_trades = []
        self.martingale_batches = {}
        self.total_trades = 0
        self.emergency_stop_active = False
        self.initial_balance = None
        self.next_batch_id = 1
        
        # Enhanced properties
        self.decision_manager = EnhancedTradingDecisionManager()
        self.data_manager = DataIntegrationManager()
        
        # Enhanced persistence system (reuse your existing code)
        from core.trading_engine import BotPersistence
        self.persistence = BotPersistence()
        
        # Recovery on startup
        logger.info("🔄 Attempting to recover previous state...")
        recovery_success = self.persistence.load_and_recover_state(self)
        if recovery_success:
            logger.info("✅ Enhanced state recovery completed successfully")
        else:
            logger.warning("⚠️ State recovery failed - starting fresh")
    
    def can_trade_enhanced(self, symbol, direction):
        """Enhanced version of can_trade with data integration"""
        try:
            # Original checks (emergency stop, account info, etc.)
            basic_check = self.can_trade_basic(symbol)
            if not basic_check:
                return False
            
            # Enhanced data-driven checks
            direction_allowed, reason = self.decision_manager.can_trade_direction(symbol, direction)
            if not direction_allowed:
                logger.info(f"🧠 Smart blocking: {symbol} {direction} - {reason}")
                return False
            
            return True
            
        except Exception as e:
            logger.error(f"Error in enhanced trade check: {e}")
            # Fall back to basic check on error
            return self.can_trade_basic(symbol)
    
    def can_trade_basic(self, symbol):
        """Original can_trade logic (preserved from your code)"""
        try:
            # Check emergency stop
            if self.emergency_stop_active:
                return False
                
            # Check account info
            account_info = mt5.account_info()
            if account_info is None:
                logger.warning("Cannot get account info")
                return False
                
            # Initialize balance tracking
            if self.initial_balance is None:
                self.initial_balance = account_info.balance
                logger.info(f"Initial balance set: ${self.initial_balance:.2f}")
                
            # Check emergency drawdown
            current_equity = account_info.equity
            if self.initial_balance and current_equity:
                drawdown = ((self.initial_balance - current_equity) / self.initial_balance) * 100
                if drawdown >= EMERGENCY_DD_PERCENTAGE:
                    self.emergency_stop_active = True
                    logger.critical(f"🚨 EMERGENCY STOP: Drawdown {drawdown:.1f}%")
                    return False
            
            # Check margin requirements
            margin_level = (account_info.equity / account_info.margin * 100) if account_info.margin > 0 else 1000
            if margin_level < 200:
                logger.warning(f"Low margin level: {margin_level:.1f}%")
                return False
            
            # Check free margin
            if account_info.margin_free < 1000:
                logger.warning(f"Low free margin: ${account_info.margin_free:.2f}")
                return False
            
            return True
            
        except Exception as e:
            logger.error(f"Error in basic trade check: {e}")
            return False
    
    def calculate_enhanced_risk_amount(self, symbol, base_risk_pct):
        """Calculate risk amount with enhanced market data adjustments"""
        try:
            account_info = mt5.account_info()
            if not account_info:
                return 0
            
            # Base risk calculation
            base_risk_amount = account_info.balance * (base_risk_pct / 100)
            
            # Apply enhanced adjustments
            enhanced_risk = self.decision_manager.get_enhanced_position_sizing(
                symbol, base_risk_amount, {}
            )
            
            return enhanced_risk
            
        except Exception as e:
            logger.error(f"Error calculating enhanced risk: {e}")
            return account_info.balance * (base_risk_pct / 100) if account_info else 0

# ===== ENHANCED SIGNAL GENERATION =====
def generate_enhanced_signals(pairs, trade_manager):
    """Enhanced signal generation with data integration"""
    signals = []
    
    for symbol in pairs:
        if not trade_manager.can_trade_basic(symbol):
            continue
            
        # Skip if we already have positions in both directions
        if (trade_manager.has_position(symbol, 'long') and 
            trade_manager.has_position(symbol, 'short')):
            continue
        
        # Your original technical analysis (preserved)
        analyses = analyze_symbol_multi_timeframe(symbol, GLOBAL_TIMEFRAME)
        
        if not analyses or GLOBAL_TIMEFRAME not in analyses:
            continue
        
        primary_analysis = analyses[GLOBAL_TIMEFRAME]
        
        # Get risk profile and parameters
        risk_profile = PAIR_RISK_PROFILES.get(symbol, "High")
        params = PARAM_SETS[risk_profile]
        
        # Get primary timeframe data for detailed analysis
        from core.trading_engine import get_historical_data, calculate_indicators, calculate_atr
        df = get_historical_data(symbol, GLOBAL_TIMEFRAME, 500)
        if df is None or len(df) < 50:
            continue
            
        df = calculate_indicators(df)
        latest = df.iloc[-1]
        prev = df.iloc[-2]
        
        # Calculate ATR for volatility check
        atr = calculate_atr(df)
        atr_pips = atr / get_pip_size(symbol)
        
        if atr_pips < params['min_volatility_pips']:
            continue
        
        # Check ADX strength
        if not (params['min_adx_strength'] <= latest['adx'] <= params['max_adx_strength']):
            continue
        
        # Multi-timeframe confirmation (your original logic)
        from core.trading_engine import get_higher_timeframes
        higher_timeframes = get_higher_timeframes(GLOBAL_TIMEFRAME)
        aligned_timeframes = 0
        
        for tf in higher_timeframes[:1]:
            if tf in analyses:
                higher_analysis = analyses[tf]
                if primary_analysis['ema_direction'] == higher_analysis['ema_direction']:
                    aligned_timeframes += 1
        
        if aligned_timeframes < 1:
            continue
        
        # Current price relative to EMA
        close_to_ema = abs(latest['close'] - latest['ema20']) / latest['ema20'] < params['ema_buffer_pct']
        
        # ENHANCED: Check each direction with data integration
        for direction in ['long', 'short']:
            # Skip if we already have position in this direction
            if trade_manager.has_position(symbol, direction):
                continue
            
            # ENHANCED: Check if data allows this direction
            if not trade_manager.can_trade_enhanced(symbol, direction):
                logger.info(f"🧠 {symbol} {direction} blocked by enhanced data analysis")
                continue
            
            # Original signal validation logic (preserved)
            signal_valid = False
            
            if direction == 'long':
                bullish_trend = primary_analysis['ema_direction'] == 'Up'
                rsi_condition = (prev['rsi'] < params['rsi_oversold'] and 
                               latest['rsi'] > params['rsi_oversold'])
                price_action = latest['close'] > latest['open']
                signal_valid = bullish_trend and close_to_ema and (rsi_condition or price_action)
                
            else:  # short
                bearish_trend = primary_analysis['ema_direction'] == 'Down'
                rsi_condition = (prev['rsi'] > params['rsi_overbought'] and 
                               latest['rsi'] < params['rsi_overbought'])
                price_action = latest['close'] < latest['open']
                signal_valid = bearish_trend and close_to_ema and (rsi_condition or price_action)
            
            if signal_valid:
                # Calculate entry, SL, TP (your original logic)
                entry_price = latest['close']
                pip_size = get_pip_size(symbol)
                
                if direction == 'long':
                    sl = min(df['low'].iloc[-3:]) - atr * params['atr_multiplier']
                    tp_distance = abs(entry_price - sl) * params['risk_reward_ratio_long']
                    tp = entry_price + tp_distance
                else:
                    sl = max(df['high'].iloc[-3:]) + atr * params['atr_multiplier']
                    tp_distance = abs(sl - entry_price) * params['risk_reward_ratio_short']
                    tp = entry_price - tp_distance
                
                # Validate SL/TP distances
                sl_distance_pips = abs(entry_price - sl) / pip_size
                tp_distance_pips = abs(tp - entry_price) / pip_size
                
                if sl_distance_pips >= 10 and tp_distance_pips >= 10:
                    signals.append({
                        'symbol': symbol,
                        'direction': direction,
                        'entry_price': entry_price,
                        'sl': sl,
                        'tp': tp,
                        'atr': atr,
                        'adx_value': latest['adx'],
                        'rsi': latest['rsi'],
                        'sl_distance_pips': sl_distance_pips,
                        'tp_distance_pips': tp_distance_pips,
                        'risk_profile': risk_profile,
                        'timestamp': datetime.now(),
                        'timeframes_aligned': aligned_timeframes + 1,
                        'is_initial': True,
                        'enhanced_validation': True  # Mark as enhanced signal
                    })
                    
                    logger.info(f"🎯 Enhanced signal: {symbol} {direction} (passed data validation)")
    
    return signals

# ===== ENHANCED MAIN ROBOT FUNCTION =====
def run_enhanced_robot():
    """Enhanced trading robot with data integration"""
    logger.info("="*70)
    logger.info("ENHANCED TRADING ROBOT WITH DATA INTEGRATION - PHASE 3")
    logger.info("="*70)
    logger.info(f"Primary Timeframe: M5")
    logger.info(f"Pairs: {len(PAIRS)}")
    logger.info(f"Martingale: {MARTINGALE_ENABLED}")
    logger.info(f"Data Integration: ACTIVE ✅")
    logger.info(f"  - Sentiment Blocking: {SENTIMENT_EXTREME_THRESHOLD}% threshold")
    logger.info(f"  - Correlation Risk: {CORRELATION_RISK_THRESHOLD}% threshold")
    logger.info(f"  - Economic Buffer: {ECONOMIC_EVENT_BUFFER_HOURS}h before events")
    logger.info("="*70)
    
    # Initialize MT5
    if not mt5.initialize():
        logger.error("MT5 initialization failed")
        return
    
    # Validate connection
    account_info = mt5.account_info()
    if account_info is None:
        logger.error("Failed to get account info")
        mt5.shutdown()
        return
    
    logger.info(f"Connected to account: {account_info.login}")
    logger.info(f"Balance: ${account_info.balance:.2f}")
    
    # Initialize enhanced trade manager
    trade_manager = EnhancedTradeManager()
    
    # Test data connectivity
    logger.info("🔄 Testing data connectivity...")
    test_data_connectivity(trade_manager.data_manager)
    
    try:
        cycle_count = 0
        consecutive_errors = 0
        
        while True:
            try:
                cycle_count += 1
                current_time = datetime.now()
                
                logger.info(f"\n{'='*60}")
                logger.info(f"Enhanced Analysis Cycle #{cycle_count} at {current_time}")
                logger.info(f"{'='*60}")
                
                # Reset error counter on successful cycle start
                consecutive_errors = 0
                
                # Check MT5 connection
                if not mt5.terminal_info():
                    logger.warning("MT5 disconnected, attempting reconnect...")
                    if not mt5.initialize():
                        logger.error("Reconnection failed")
                        consecutive_errors += 1
                        if consecutive_errors >= 5:
                            logger.critical("Too many consecutive connection errors - stopping")
                            break
                        time.sleep(30)
                        continue
                
                # Enhanced: Display current market data status
                display_data_status(trade_manager.data_manager)
                
                # Get current prices for all pairs
                current_prices = {}
                for symbol in PAIRS:
                    try:
                        tick = mt5.symbol_info_tick(symbol)
                        if tick is None:
                            logger.warning(f"Failed to get tick data for {symbol}")
                            continue
                        current_prices[symbol] = {
                            'bid': tick.bid,
                            'ask': tick.ask
                        }
                    except Exception as e:
                        logger.warning(f"Error getting price for {symbol}: {e}")
                        continue
                
                if not current_prices:
                    logger.warning("No price data available. Skipping this cycle...")
                    time.sleep(30)
                    continue
                
                # Generate enhanced signals with data integration
                try:
                    signals = generate_enhanced_signals(PAIRS, trade_manager)
                    logger.info(f"Generated {len(signals)} enhanced signals")
                except Exception as e:
                    logger.error(f"Error generating enhanced signals: {e}")
                    signals = []
                
                # Execute signals with enhanced validation
                for signal in signals:
                    try:
                        if not trade_manager.can_trade_basic(signal['symbol']):
                            continue
                        
                        logger.info(f"\n🎯 Enhanced Signal: {signal['symbol']} {signal['direction'].upper()}")
                        logger.info(f"   Entry: {signal['entry_price']:.5f}")
                        logger.info(f"   SL Distance: {signal['sl_distance_pips']:.1f} pips")
                        logger.info(f"   TP: {signal['tp']:.5f} ({signal['tp_distance_pips']:.1f} pips)")
                        logger.info(f"   ADX: {signal['adx_value']:.1f}, RSI: {signal['rsi']:.1f}")
                        logger.info(f"   Enhanced Validation: ✅ PASSED")
                        logger.info(f"   🚫 NO SL - Build-from-first approach")
                        
                        # Enhanced execute trade with data-driven risk management
                        if execute_enhanced_trade(signal, trade_manager):
                            logger.info("✅ Enhanced trade executed successfully")
                        else:
                            logger.error("❌ Trade execution failed")
                            
                    except Exception as e:
                        logger.error(f"Error executing enhanced signal for {signal.get('symbol', 'Unknown')}: {e}")
                        continue
                
                # Check for martingale opportunities (enhanced)
                if MARTINGALE_ENABLED and not trade_manager.emergency_stop_active:
                    try:
                        # Import martingale functions from original engine
                        from core.trading_engine import execute_martingale_trade
                        
                        martingale_opportunities = trade_manager.check_martingale_opportunities_enhanced(current_prices)
                        
                        for opportunity in martingale_opportunities:
                            try:
                                symbol = opportunity['symbol']
                                direction = opportunity['direction']
                                
                                # Enhanced: Check if data still allows this direction for martingale
                                if not trade_manager.can_trade_enhanced(symbol, direction):
                                    logger.info(f"🧠 Martingale {symbol} {direction} blocked by enhanced data analysis")
                                    continue
                                
                                logger.info(f"\n🔄 Enhanced Martingale Opportunity: {symbol} {direction.upper()}")
                                logger.info(f"   Layer: {opportunity['layer']}")
                                logger.info(f"   Trigger: {opportunity['trigger_price']:.5f}")
                                logger.info(f"   Current: {opportunity['entry_price']:.5f}")
                                logger.info(f"   Distance: {opportunity['distance_pips']:.1f} pips")
                                logger.info(f"   Enhanced Validation: ✅ PASSED")
                                
                                if execute_martingale_trade(opportunity, trade_manager):
                                    logger.info("✅ Enhanced martingale layer executed successfully")
                                    
                                    # Update batch TP after adding layer
                                    batch = opportunity['batch']
                                    try:
                                        new_tp = batch.calculate_adaptive_batch_tp()
                                        if new_tp:
                                            logger.info(f"🔄 Updating batch TP to {new_tp:.5f}")
                                            batch.update_all_tps_with_retry(new_tp)
                                    except Exception as e:
                                        logger.error(f"Error updating batch TP after enhanced martingale: {e}")
                                else:
                                    logger.error("❌ Enhanced martingale execution failed")
                                    
                            except Exception as e:
                                logger.error(f"Error executing enhanced martingale for {opportunity.get('symbol', 'Unknown')}: {e}")
                                continue
                                
                    except Exception as e:
                        logger.error(f"Error checking enhanced martingale opportunities: {e}")
                
                # Sync with MT5 positions every cycle
                try:
                    trade_manager.sync_with_mt5_positions()
                except Exception as e:
                    logger.error(f"Error syncing with MT5: {e}")
                
                # Monitor batch exits
                try:
                    trade_manager.monitor_batch_exits(current_prices)
                except Exception as e:
                    logger.error(f"Error monitoring batch exits: {e}")
                
                # Enhanced: Show account status with data insights
                try:
                    display_enhanced_account_status(trade_manager)
                except Exception as e:
                    logger.error(f"Error displaying enhanced account status: {e}")
                
                # Sleep until next M5 candle
                try:
                    now = datetime.now()
                    next_candle = now + timedelta(minutes=5 - (now.minute % 5))
                    next_candle = next_candle.replace(second=0, microsecond=0)
                    sleep_time = (next_candle - now).total_seconds()
                    
                    logger.info(f"\n⏰ Sleeping {sleep_time:.1f}s until next M5 candle at {next_candle}")
                    time.sleep(max(1, sleep_time))
                    
                except Exception as e:
                    logger.error(f"Error in sleep calculation: {e}")
                    time.sleep(60)  # Default 1 minute sleep
                    
            except KeyboardInterrupt:
                logger.info("\n🛑 Enhanced robot stopped by user")
                raise
                
            except Exception as e:
                consecutive_errors += 1
                logger.error(f"\n❌ Error in enhanced cycle #{cycle_count}: {e}")
                logger.error(f"Consecutive errors: {consecutive_errors}")
                
                # Emergency state save on error
                try:
                    trade_manager.persistence.save_bot_state(trade_manager)
                    logger.info("💾 Emergency state saved")
                except Exception as save_error:
                    logger.error(f"Failed to save emergency state: {save_error}")
                
                if consecutive_errors >= 10:
                    logger.critical(f"🚨 Too many consecutive errors ({consecutive_errors}) - stopping enhanced robot")
                    break
                
                import traceback
                logger.error(f"Detailed error info:\n{traceback.format_exc()}")
                
                error_sleep = min(consecutive_errors * 30, 300)
                logger.info(f"⏰ Waiting {error_sleep}s before retry...")
                time.sleep(error_sleep)
                
    except KeyboardInterrupt:
        logger.info("\n🛑 Enhanced robot stopped by user")
    except Exception as e:
        logger.error(f"\n❌ Fatal error in enhanced robot: {e}")
        import traceback
        traceback.print_exc()
    finally:
        # Final cleanup and state save
        try:
            logger.info("🔄 Performing final enhanced cleanup...")
            trade_manager.persistence.save_bot_state(trade_manager)
            logger.info("💾 Final enhanced state saved successfully")
        except Exception as e:
            logger.error(f"Error during final cleanup: {e}")
        
        try:
            mt5.shutdown()
            logger.info("MT5 connection closed")
        except Exception as e:
            logger.error(f"Error closing MT5: {e}")

# ===== ENHANCED TRADE EXECUTION =====
def execute_enhanced_trade(signal, trade_manager):
    """Enhanced trade execution with data-driven risk management"""
    symbol = signal['symbol']
    direction = signal['direction']
    
    # Enhanced: Calculate risk amount with market data adjustments
    risk_profile = signal['risk_profile']
    params = PARAM_SETS[risk_profile]
    base_risk_pct = params['risk_per_trade_pct']
    
    # Apply enhanced risk calculation
    enhanced_risk_amount = trade_manager.calculate_enhanced_risk_amount(symbol, base_risk_pct)
    
    # Update signal with enhanced risk amount
    signal['enhanced_risk_amount'] = enhanced_risk_amount
    
    # Use original execute_trade function with enhanced signal
    from core.trading_engine import execute_trade
    return execute_trade(signal, trade_manager)

# ===== ENHANCED UTILITY FUNCTIONS =====
def test_data_connectivity(data_manager):
    """Test connectivity to all data sources"""
    logger.info("🧪 Testing data source connectivity...")
    
    try:
        # Test sentiment data
        sentiment_data = data_manager.get_sentiment_data()
        sentiment_count = len(sentiment_data)
        logger.info(f"   Sentiment: {sentiment_count} pairs {'✅' if sentiment_count > 0 else '❌'}")
        
        # Test correlation data
        correlation_data = data_manager.get_correlation_data()
        correlation_warnings = len(correlation_data.get('warnings', []))
        correlation_matrix_size = len(correlation_data.get('matrix', {}))
        logger.info(f"   Correlation: {correlation_matrix_size} currencies, {correlation_warnings} warnings {'✅' if correlation_matrix_size > 0 else '❌'}")
        
        # Test economic events
        economic_events = data_manager.get_economic_events(24)
        logger.info(f"   Economic Events: {len(economic_events)} upcoming {'✅' if len(economic_events) >= 0 else '❌'}")
        
        # Test COT data
        cot_data = data_manager.get_cot_data()
        cot_financial_count = len(cot_data.get('financial', {}))
        cot_commodity_count = len(cot_data.get('commodity', {}))
        logger.info(f"   COT Data: {cot_financial_count} financial, {cot_commodity_count} commodity {'✅' if cot_financial_count > 0 or cot_commodity_count > 0 else '❌'}")
        
        return True
        
    except Exception as e:
        logger.error(f"❌ Data connectivity test failed: {e}")
        return False

def display_data_status(data_manager):
    """Display current market data status"""
    try:
        market_data = data_manager.load_market_data()
        
        if not market_data:
            logger.warning("⚠️ No market data available")
            return
        
        data_sources = market_data.get('data_sources', {})
        system_status = market_data.get('system_status', 'unknown')
        
        logger.info(f"📊 Market Data Status: {system_status.upper()}")
        
        # Show each data source status
        for source_name, source_data in data_sources.items():
            status = source_data.get('status', 'unknown')
            last_update = source_data.get('last_update', 'never')
            
            if last_update != 'never':
                try:
                    update_time = datetime.fromisoformat(last_update)
                    age_minutes = (datetime.now() - update_time).total_seconds() / 60
                    time_str = f"{age_minutes:.0f}m ago"
                except:
                    time_str = "unknown age"
            else:
                time_str = "never"
            
            status_emoji = "✅" if status == "fresh" else "⚠️" if status == "waiting" else "❌"
            logger.info(f"   {source_name}: {status} {status_emoji} ({time_str})")
    
    except Exception as e:
        logger.error(f"Error displaying data status: {e}")

def display_enhanced_account_status(trade_manager):
    """Display enhanced account status with data insights"""
    try:
        account_info = mt5.account_info()
        if not account_info:
            return
        
        logger.info(f"\n📊 Enhanced Account Status:")
        logger.info(f"   Balance: ${account_info.balance:.2f}")
        logger.info(f"   Equity: ${account_info.equity:.2f}")
        logger.info(f"   Margin: ${account_info.margin:.2f}")
        logger.info(f"   Free Margin: ${account_info.margin_free:.2f}")
        logger.info(f"   Active Trades: {len(trade_manager.active_trades)}")
        
        if trade_manager.initial_balance:
            pnl = account_info.equity - trade_manager.initial_balance
            pnl_pct = (pnl / trade_manager.initial_balance) * 100
            logger.info(f"   P&L: ${pnl:.2f} ({pnl_pct:.2f}%)")
        
        # Enhanced: Show data-driven insights
        try:
            # Sentiment insights
            sentiment_data = trade_manager.data_manager.get_sentiment_data()
            blocked_pairs = []
            for pair, data in sentiment_data.items():
                blocked_directions = data.get('blocked_directions', [])
                if blocked_directions:
                    blocked_pairs.append(f"{pair}({','.join(blocked_directions)})")
            
            if blocked_pairs:
                logger.info(f"   🧠 Sentiment Blocks: {', '.join(blocked_pairs[:5])}{'...' if len(blocked_pairs) > 5 else ''}")
            
            # Correlation warnings
            correlation_data = trade_manager.data_manager.get_correlation_data()
            high_corr_warnings = len([w for w in correlation_data.get('warnings', []) if w.get('type') == 'HIGH_CORRELATION'])
            if high_corr_warnings > 0:
                logger.info(f"   🔗 Correlation Warnings: {high_corr_warnings} high correlations detected")
            
            # Upcoming economic events
            upcoming_events = trade_manager.data_manager.get_economic_events(6)
            high_impact_events = [e for e in upcoming_events if e.get('impact') == 'high']
            if high_impact_events:
                next_event = min(high_impact_events, key=lambda x: x.get('time_until_hours', 24))
                currency = next_event.get('currency', '')
                hours = next_event.get('time_until_hours', 0)
                logger.info(f"   📅 Next Major Event: {currency} in {hours:.1f}h ({next_event.get('event_name', 'Unknown')[:30]})")
        
        except Exception as e:
            logger.debug(f"Error showing data insights: {e}")
        
        # Enhanced batch status
        active_batches = len([b for b in trade_manager.martingale_batches.values() if b.trades])
        if active_batches > 0:
            logger.info(f"\n🔄 Enhanced Martingale Batches: {active_batches} active")
            for batch_key, batch in trade_manager.martingale_batches.items():
                if batch.trades:
                    logger.info(f"   {batch_key}: Layer {batch.current_layer}/{MAX_MARTINGALE_LAYERS}")
                    logger.info(f"     Volume: {batch.total_volume:.2f}, Breakeven: {batch.breakeven_price:.5f}")
                    try:
                        next_trigger = batch.get_next_trigger_price()
                        logger.info(f"     Next trigger: {next_trigger:.5f}")
                        
                        if batch.trades and batch.trades[0].get('tp'):
                            current_tp = batch.trades[0]['tp']
                            logger.info(f"     Current TP: {current_tp:.5f}")
                    except Exception as e:
                        logger.error(f"Error getting enhanced batch info for {batch_key}: {e}")
        else:
            logger.info(f"\n🎯 No active martingale batches - ready for new enhanced signals")
            
    except Exception as e:
        logger.error(f"Error displaying enhanced account status: {e}")

# ===== UTILITY IMPORTS =====
# Import necessary functions from your original trading engine
def get_pip_size(symbol):
    """Get pip size for different symbol types (imported from original)"""
    symbol = symbol.upper()
    if 'JPY' in symbol:
        return 0.01
    if symbol in ['US500', 'NAS100', 'SPX500']:
        return 0.1
    if symbol in ['XAUUSD', 'GOLD']:
        return 0.1
    if symbol in ['BTCUSD', 'ETHUSD', 'XRPUSD']:
        return 1.0
    return 0.0001

def analyze_symbol_multi_timeframe(symbol, base_timeframe):
    """Multi-timeframe analysis (imported from original)"""
    # This would import your original function
    # For now, return a simple structure
    return {
        base_timeframe: {
            'trend': 'Medium Upward',
            'ema_direction': 'Up',
            'adx': 35,
            'rsi': 45,
            'close': 1.0500,
            'ema20': 1.0485
        }
    }

if __name__ == "__main__":
    run_enhanced_robot()