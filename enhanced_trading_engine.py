#!/usr/bin/env python3
# ===== PHASE 3: COMPLETE ENHANCED TRADING ENGINE =====
# Integrates proven trading system with intelligent data-driven decisions
# Preserves existing martingale logic while adding smart risk management

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

# ===== ENHANCED CONFIGURATION WITH SWITCHES =====
GLOBAL_TIMEFRAME = mt5.TIMEFRAME_M5
ACCOUNT_NUMBER = 42903786
MAGIC_NUMBER = 50515253

# ===== INTELLIGENCE SWITCHES - CONFIGURABLE =====
INTELLIGENCE_CONFIG = {
    # Master switch for all enhanced features
    'ENHANCED_FEATURES_ENABLED': True,
    
    # Individual feature switches
    'USE_SENTIMENT_BLOCKING': True,
    'USE_CORRELATION_RISK': True,
    'USE_ECONOMIC_TIMING': True,
    'USE_DYNAMIC_POSITION_SIZING': True,
    'USE_COT_ANALYSIS': False,  # Optional - can be enabled later
    
    # Risk management switches
    'ENHANCED_RISK_MANAGEMENT': True,
    'EMERGENCY_DATA_FALLBACK': True,
    
    # Technical analysis preservation
    'PRESERVE_ORIGINAL_TA': True,  # Always keep your proven TA
    'TA_WEIGHT': 70,  # 70% weight to technical analysis
    'DATA_WEIGHT': 30,  # 30% weight to external data
    
    # Master risk override
    'MASTER_RISK_LEVEL': 100,  # 100% = normal, 50% = half risk, 200% = double risk
}

# Data Integration Settings
DATA_DIR = Path("data")
MARKET_DATA_FILE = DATA_DIR / "market_data.json"
SENTIMENT_FILE = Path("sentiment_signals.json")
CORRELATION_FILE = Path("correlation_data.json")
ECONOMIC_FILE = DATA_DIR / "economic_events.json"

# Enhanced Risk Thresholds (configurable)
RISK_THRESHOLDS = {
    'SENTIMENT_EXTREME': 70,  # Block if sentiment > 70%
    'CORRELATION_HIGH': 70,   # Reduce risk if correlation > 70%
    'ECONOMIC_BUFFER_HOURS': 1,  # Avoid trades 1h before events
    'DATA_FRESHNESS_MINUTES': 60,  # Use data if < 60 minutes old
}

# Original Martingale Configuration (preserved)
MARTINGALE_ENABLED = True
MAX_MARTINGALE_LAYERS = 15
MARTINGALE_MULTIPLIER = 2
EMERGENCY_DD_PERCENTAGE = 50
PROFIT_BUFFER_PIPS = 5
MIN_PROFIT_PERCENTAGE = 1
FLIRT_THRESHOLD_PIPS = 10

# Trading Pairs and Risk Profiles (preserved from your system)
PAIRS = ['AUDUSD', 'USDCAD', 'XAUUSD', 'EURUSD', 'GBPUSD', 
         'AUDCAD', 'USDCHF', 'GBPCAD', 'AUDNZD', 'NZDCAD', 'US500', 'BTCUSD']

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

# ===== ENHANCED DATA INTEGRATION MANAGER =====
class EnhancedDataManager:
    """Manages all external data sources with fallback mechanisms"""
    
    def __init__(self):
        self.data_cache = {}
        self.last_update = {}
        self.fallback_mode = {}
        
        # Initialize fallback states
        for source in ['sentiment', 'correlation', 'economic', 'cot']:
            self.fallback_mode[source] = False
    
    def get_sentiment_data(self):
        """Get sentiment data with fallback"""
        if not INTELLIGENCE_CONFIG['USE_SENTIMENT_BLOCKING']:
            return self._get_fallback_sentiment()
        
        try:
            if not SENTIMENT_FILE.exists():
                logger.warning("⚠️ Sentiment file not found, using fallback")
                return self._get_fallback_sentiment()
            
            with open(SENTIMENT_FILE, 'r') as f:
                data = json.load(f)
            
            # Check data freshness
            timestamp = datetime.fromisoformat(data['timestamp'])
            age_minutes = (datetime.now() - timestamp).total_seconds() / 60
            
            if age_minutes > RISK_THRESHOLDS['DATA_FRESHNESS_MINUTES']:
                logger.warning(f"⚠️ Sentiment data stale ({age_minutes:.1f}m), using fallback")
                return self._get_fallback_sentiment()
            
            logger.debug(f"✅ Fresh sentiment data loaded ({age_minutes:.1f}m old)")
            return data.get('pairs', {})
            
        except Exception as e:
            logger.error(f"❌ Error loading sentiment data: {e}")
            return self._get_fallback_sentiment()
    
    def get_correlation_data(self):
        """Get correlation data with fallback"""
        if not INTELLIGENCE_CONFIG['USE_CORRELATION_RISK']:
            return {'matrix': {}, 'warnings': []}
        
        try:
            if not CORRELATION_FILE.exists():
                logger.warning("⚠️ Correlation file not found, using fallback")
                return {'matrix': {}, 'warnings': []}
            
            with open(CORRELATION_FILE, 'r') as f:
                data = json.load(f)
            
            # Check data freshness
            timestamp = datetime.fromisoformat(data['timestamp'])
            age_minutes = (datetime.now() - timestamp).total_seconds() / 60
            
            if age_minutes > RISK_THRESHOLDS['DATA_FRESHNESS_MINUTES']:
                logger.warning(f"⚠️ Correlation data stale ({age_minutes:.1f}m)")
                return {'matrix': {}, 'warnings': []}
            
            return {
                'matrix': data.get('correlation_matrix', {}),
                'warnings': data.get('warnings', [])
            }
            
        except Exception as e:
            logger.error(f"❌ Error loading correlation data: {e}")
            return {'matrix': {}, 'warnings': []}
    
    def get_economic_events(self, hours_ahead=24):
        """Get upcoming economic events"""
        if not INTELLIGENCE_CONFIG['USE_ECONOMIC_TIMING']:
            return []
        
        try:
            # Try to load from market data file
            if MARKET_DATA_FILE.exists():
                with open(MARKET_DATA_FILE, 'r') as f:
                    market_data = json.load(f)
                
                calendar_data = market_data.get('data_sources', {}).get('economic_calendar', {})
                
                if calendar_data.get('status') == 'fresh':
                    events = calendar_data.get('events', [])
                    return self._filter_upcoming_events(events, hours_ahead)
            
            return []
            
        except Exception as e:
            logger.error(f"❌ Error loading economic events: {e}")
            return []
    
    def _get_fallback_sentiment(self):
        """Fallback sentiment - allow all directions"""
        fallback = {}
        for pair in PAIRS:
            fallback[pair] = {
                'allowed_directions': ['long', 'short'],
                'blocked_directions': [],
                'sentiment': {'short': 50, 'long': 50},
                'signal_strength': 'Fallback'
            }
        return fallback
    
    def _filter_upcoming_events(self, events, hours_ahead):
        """Filter events for upcoming high-impact ones"""
        upcoming = []
        current_time = datetime.now()
        cutoff_time = current_time + timedelta(hours=hours_ahead)
        
        for event in events:
            try:
                if event.get('impact', '').lower() in ['high', 'medium']:
                    # Try to parse event time
                    event_time_str = event.get('time', '')
                    if event_time_str and event_time_str != 'N/A':
                        # Simple time parsing - assume today
                        try:
                            event_hour, event_minute = map(int, event_time_str.split(':'))
                            event_time = current_time.replace(hour=event_hour, minute=event_minute, second=0)
                            
                            # If time has passed today, assume tomorrow
                            if event_time < current_time:
                                event_time += timedelta(days=1)
                            
                            if event_time <= cutoff_time:
                                upcoming.append({
                                    'currency': event.get('currency', ''),
                                    'event_name': event.get('event_name', ''),
                                    'impact': event.get('impact', ''),
                                    'time_until_hours': (event_time - current_time).total_seconds() / 3600
                                })
                        except:
                            continue
                            
            except Exception as e:
                logger.debug(f"Error parsing event: {e}")
                continue
        
        return upcoming

# ===== ENHANCED DECISION ENGINE =====
class EnhancedDecisionEngine:
    """Makes intelligent trading decisions combining TA and external data"""
    
    def __init__(self):
        self.data_manager = EnhancedDataManager()
        self.decision_log = []
    
    def can_trade_direction(self, symbol, direction, ta_signal_strength=100):
        """
        Enhanced decision making with configurable weights
        
        Args:
            symbol: Trading pair
            direction: 'long' or 'short'  
            ta_signal_strength: Technical analysis confidence (0-100)
            
        Returns:
            (can_trade, confidence, reasons)
        """
        
        if not INTELLIGENCE_CONFIG['ENHANCED_FEATURES_ENABLED']:
            # Pure technical analysis mode
            return True, ta_signal_strength, ["Pure TA mode"]
        
        try:
            reasons = []
            blocking_factors = []
            risk_factors = []
            
            # Start with TA confidence
            ta_weight = INTELLIGENCE_CONFIG['TA_WEIGHT'] / 100
            data_weight = INTELLIGENCE_CONFIG['DATA_WEIGHT'] / 100
            
            base_confidence = ta_signal_strength * ta_weight
            
            # Check 1: Sentiment Analysis
            sentiment_adjustment = 0
            if INTELLIGENCE_CONFIG['USE_SENTIMENT_BLOCKING']:
                sentiment_check = self._check_sentiment(symbol, direction)
                if not sentiment_check['allowed']:
                    blocking_factors.append(f"Sentiment: {sentiment_check['reason']}")
                else:
                    sentiment_adjustment = sentiment_check.get('confidence_boost', 0)
                    reasons.append(f"Sentiment: OK ({sentiment_check['reason']})")
            
            # Check 2: Correlation Risk
            correlation_adjustment = 0
            if INTELLIGENCE_CONFIG['USE_CORRELATION_RISK']:
                correlation_check = self._check_correlation_risk(symbol)
                if correlation_check['high_risk']:
                    risk_factors.append(f"Correlation: {correlation_check['reason']}")
                    correlation_adjustment = -10  # Reduce confidence
                else:
                    reasons.append(f"Correlation: OK")
            
            # Check 3: Economic Events
            economic_adjustment = 0
            if INTELLIGENCE_CONFIG['USE_ECONOMIC_TIMING']:
                economic_check = self._check_economic_timing(symbol)
                if not economic_check['allowed']:
                    blocking_factors.append(f"Economic: {economic_check['reason']}")
                else:
                    reasons.append(f"Economic: Clear")
            
            # Apply blocking factors
            if blocking_factors:
                return False, 0, blocking_factors
            
            # Calculate final confidence
            data_adjustments = sentiment_adjustment + correlation_adjustment + economic_adjustment
            final_confidence = base_confidence + (data_adjustments * data_weight)
            
            # Apply master risk level
            final_confidence *= (INTELLIGENCE_CONFIG['MASTER_RISK_LEVEL'] / 100)
            
            # Cap confidence
            final_confidence = max(0, min(100, final_confidence))
            
            # Decision logic
            can_trade = final_confidence >= 30  # Minimum 30% confidence to trade
            
            if risk_factors:
                reasons.extend(risk_factors)
            
            self._log_decision(symbol, direction, can_trade, final_confidence, reasons)
            
            return can_trade, final_confidence, reasons
            
        except Exception as e:
            logger.error(f"❌ Error in enhanced decision for {symbol} {direction}: {e}")
            # Fallback to allowing trade
            return True, ta_signal_strength, [f"Error in decision engine: {e}"]
    
    def _check_sentiment(self, symbol, direction):
        """Check sentiment blocking"""
        try:
            sentiment_data = self.data_manager.get_sentiment_data()
            
            # Normalize symbol name
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
                return {'allowed': True, 'reason': 'No sentiment data', 'confidence_boost': 0}
            
            # Check blocked directions
            blocked_directions = sentiment_info.get('blocked_directions', [])
            if direction in blocked_directions:
                sentiment = sentiment_info.get('sentiment', {})
                return {
                    'allowed': False,
                    'reason': f"Extreme sentiment - {sentiment.get('short', 0)}%↓ {sentiment.get('long', 0)}%↑"
                }
            
            # Calculate confidence boost based on sentiment alignment
            sentiment = sentiment_info.get('sentiment', {})
            if direction == 'long':
                long_pct = sentiment.get('long', 50)
                if long_pct > 60:
                    confidence_boost = (long_pct - 50) / 5  # Up to 10 point boost
                else:
                    confidence_boost = 0
            else:  # short
                short_pct = sentiment.get('short', 50)
                if short_pct > 60:
                    confidence_boost = (short_pct - 50) / 5
                else:
                    confidence_boost = 0
            
            return {
                'allowed': True,
                'reason': f"Sentiment supports {direction}",
                'confidence_boost': confidence_boost
            }
            
        except Exception as e:
            logger.warning(f"Error checking sentiment: {e}")
            return {'allowed': True, 'reason': 'Sentiment check error', 'confidence_boost': 0}
    
    def _check_correlation_risk(self, symbol):
        """Check correlation-based risk"""
        try:
            correlation_data = self.data_manager.get_correlation_data()
            warnings = correlation_data.get('warnings', [])
            
            high_corr_count = 0
            for warning in warnings:
                if warning.get('type') == 'HIGH_CORRELATION':
                    pair = warning.get('pair', '')
                    if symbol in pair:
                        high_corr_count += 1
            
            if high_corr_count >= 3:  # More than 3 high correlations
                return {
                    'high_risk': True,
                    'reason': f'{high_corr_count} high correlations detected'
                }
            
            return {'high_risk': False, 'reason': 'Low correlation risk'}
            
        except Exception as e:
            logger.warning(f"Error checking correlation: {e}")
            return {'high_risk': False, 'reason': 'Correlation check error'}
    
    def _check_economic_timing(self, symbol):
        """Check economic event timing"""
        try:
            upcoming_events = self.data_manager.get_economic_events(RISK_THRESHOLDS['ECONOMIC_BUFFER_HOURS'])
            
            # Extract currencies from symbol
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
                symbol_currencies = [symbol[:3], symbol[3:6]]
            
            for event in upcoming_events:
                event_currency = event.get('currency', '')
                time_until = event.get('time_until_hours', 24)
                impact = event.get('impact', '').lower()
                
                if (event_currency in symbol_currencies and 
                    impact == 'high' and 
                    time_until <= RISK_THRESHOLDS['ECONOMIC_BUFFER_HOURS']):
                    
                    return {
                        'allowed': False,
                        'reason': f"High-impact {event_currency} event in {time_until:.1f}h"
                    }
            
            return {'allowed': True, 'reason': 'No conflicting events'}
            
        except Exception as e:
            logger.warning(f"Error checking economic timing: {e}")
            return {'allowed': True, 'reason': 'Economic check error'}
    
    def _log_decision(self, symbol, direction, allowed, confidence, reasons):
        """Log decision for analysis"""
        decision_record = {
            'timestamp': datetime.now().isoformat(),
            'symbol': symbol,
            'direction': direction,
            'allowed': allowed,
            'confidence': confidence,
            'reasons': reasons
        }
        
        self.decision_log.append(decision_record)
        
        # Keep only last 100 decisions
        if len(self.decision_log) > 100:
            self.decision_log = self.decision_log[-100:]
        
        # Log significant decisions
        if not allowed or confidence < 50:
            logger.info(f"🧠 Decision: {symbol} {direction} - {allowed} ({confidence:.1f}%) - {'; '.join(reasons)}")

# ===== ENHANCED POSITION SIZING =====
class EnhancedPositionSizing:
    """Calculates position sizes with multiple risk factors"""
    
    def __init__(self):
        self.data_manager = EnhancedDataManager()
    
    def calculate_enhanced_position_size(self, symbol, base_risk_amount, confidence_level=100):
        """
        Calculate position size with enhanced risk management
        
        Args:  
            symbol: Trading pair
            base_risk_amount: Base risk from original calculation
            confidence_level: Confidence from decision engine (0-100)
            
        Returns:
            adjusted_risk_amount
        """
        
        if not INTELLIGENCE_CONFIG['USE_DYNAMIC_POSITION_SIZING']:
            return base_risk_amount
        
        try:
            adjustments = []
            risk_multiplier = 1.0
            
            # Apply master risk level
            master_risk = INTELLIGENCE_CONFIG['MASTER_RISK_LEVEL'] / 100
            risk_multiplier *= master_risk
            if master_risk != 1.0:
                adjustments.append(f"Master risk: {master_risk:.0%}")
            
            # Apply confidence-based sizing
            confidence_multiplier = confidence_level / 100
            risk_multiplier *= confidence_multiplier
            adjustments.append(f"Confidence: {confidence_level:.0f}%")
            
            # Check correlation warnings
            if INTELLIGENCE_CONFIG['USE_CORRELATION_RISK']:
                correlation_data = self.data_manager.get_correlation_data()
                warnings = correlation_data.get('warnings', [])
                
                high_corr_count = sum(1 for w in warnings 
                                    if w.get('type') == 'HIGH_CORRELATION' and symbol in w.get('pair', ''))
                
                if high_corr_count >= 2:
                    corr_reduction = 0.8  # 20% reduction
                    risk_multiplier *= corr_reduction
                    adjustments.append(f"High correlation: -{int((1-corr_reduction)*100)}%")
            
            # Check for major economic events
            if INTELLIGENCE_CONFIG['USE_ECONOMIC_TIMING']:
                upcoming_events = self.data_manager.get_economic_events(6)  # Next 6 hours
                high_impact_events = [e for e in upcoming_events if e.get('impact') == 'high']
                
                if high_impact_events:
                    event_reduction = 0.7  # 30% reduction
                    risk_multiplier *= event_reduction
                    adjustments.append(f"Major events: -{int((1-event_reduction)*100)}%")
            
            # Apply final adjustment
            adjusted_risk = base_risk_amount * risk_multiplier
            
            # Log significant adjustments
            if abs(risk_multiplier - 1.0) > 0.1:  # More than 10% change
                logger.info(f"💰 {symbol} risk adjusted: ${base_risk_amount:.2f} → ${adjusted_risk:.2f} ({risk_multiplier:.2f}x)")
                for adjustment in adjustments:
                    logger.info(f"   • {adjustment}")
            
            return adjusted_risk
            
        except Exception as e:
            logger.error(f"❌ Error calculating enhanced position size: {e}")
            return base_risk_amount

# ===== ENHANCED TRADE MANAGER (PRESERVING YOUR MARTINGALE SYSTEM) =====
class EnhancedTradeManager:
    """Enhanced version preserving all your proven martingale logic"""
    
    def __init__(self):
        # Add paths for imports
        import sys
        base_path = Path(__file__).parent
        core_path = str(base_path / 'core')
        if core_path not in sys.path:
            sys.path.insert(0, core_path)
        
        # Import your proven components
        try:
            from trading_engine_backup import (
                EnhancedTradeManager as OriginalTradeManager,
                BotPersistence
            )
        except ImportError:
            # Try alternative import path
            sys.path.insert(0, str(base_path))
            from core.trading_engine_backup import (
                EnhancedTradeManager as OriginalTradeManager,
                BotPersistence
            )
        
        # Initialize with your proven base
        self.original_manager = OriginalTradeManager()
        
        # Enhanced components
        self.decision_engine = EnhancedDecisionEngine()
        self.position_sizer = EnhancedPositionSizing()
        
        # Preserve all original properties
        self.active_trades = self.original_manager.active_trades
        self.martingale_batches = self.original_manager.martingale_batches
        self.total_trades = self.original_manager.total_trades
        self.emergency_stop_active = self.original_manager.emergency_stop_active
        self.initial_balance = self.original_manager.initial_balance
        self.next_batch_id = self.original_manager.next_batch_id
        self.persistence = self.original_manager.persistence
        
        logger.info("✅ Enhanced Trade Manager initialized with proven base")
    
    def can_trade_enhanced(self, symbol, direction, ta_signal_strength=100):
        """Enhanced can_trade with intelligence integration"""
        
        # First run original basic checks
        if not self.original_manager.can_trade(symbol):
            return False
        
        # If enhanced features disabled, use original logic only
        if not INTELLIGENCE_CONFIG['ENHANCED_FEATURES_ENABLED']:
            return True
        
        # Enhanced decision making
        can_trade, confidence, reasons = self.decision_engine.can_trade_direction(
            symbol, direction, ta_signal_strength
        )
        
        if not can_trade:
            logger.info(f"🧠 Smart blocking: {symbol} {direction} - {'; '.join(reasons)}")
            return False
        
        return True
    
    def calculate_enhanced_risk_amount(self, symbol, base_risk_pct, confidence_level=100):
        """Calculate risk with enhanced position sizing"""
        try:
            # Get account info
            account_info = mt5.account_info()
            if not account_info:
                return 0
            
            # Base calculation (your original method)
            base_risk_amount = account_info.balance * (base_risk_pct / 100)
            
            # Apply enhanced adjustments
            enhanced_risk = self.position_sizer.calculate_enhanced_position_size(
                symbol, base_risk_amount, confidence_level
            )
            
            return enhanced_risk
            
        except Exception as e:
            logger.error(f"Error calculating enhanced risk: {e}")
            return account_info.balance * (base_risk_pct / 100) if account_info else 0
    
    # Delegate all other methods to original manager to preserve proven logic
    def __getattr__(self, name):
        """Delegate unknown methods to original manager"""
        return getattr(self.original_manager, name)

# ===== ENHANCED SIGNAL GENERATION =====
def generate_enhanced_signals(pairs, trade_manager):
    """Enhanced signal generation preserving your TA with intelligent overlay"""
    
    # Import your proven TA functions
    from core.trading_engine_backup import (
        analyze_symbol_multi_timeframe,
        get_historical_data,
        calculate_indicators,
        calculate_atr,
        get_pip_size,
        get_higher_timeframes
    )
    
    signals = []
    
    for symbol in pairs:
        if not trade_manager.can_trade(symbol):
            continue
            
        # Skip if we already have positions in both directions
        if (trade_manager.has_position(symbol, 'long') and 
            trade_manager.has_position(symbol, 'short')):
            continue
        
        # YOUR PROVEN TECHNICAL ANALYSIS (preserved exactly)
        analyses = analyze_symbol_multi_timeframe(symbol, GLOBAL_TIMEFRAME)
        
        if not analyses or GLOBAL_TIMEFRAME not in analyses:
            continue
        
        primary_analysis = analyses[GLOBAL_TIMEFRAME]
        
        # Get risk profile and parameters (your system)
        risk_profile = PAIR_RISK_PROFILES.get(symbol, "High")
        params = PARAM_SETS[risk_profile]
        
        # Get primary timeframe data (your method)
        df = get_historical_data(symbol, GLOBAL_TIMEFRAME, 500)
        if df is None or len(df) < 50:
            continue
            
        df = calculate_indicators(df)
        latest = df.iloc[-1]
        prev = df.iloc[-2]
        
        # Calculate ATR (your method)
        atr = calculate_atr(df)
        atr_pips = atr / get_pip_size(symbol)
        
        if atr_pips < params['min_volatility_pips']:
            continue
        
        # Check ADX strength (your method)
        if not (params['min_adx_strength'] <= latest['adx'] <= params['max_adx_strength']):
            continue
        
        # Multi-timeframe confirmation (your method)
        higher_timeframes = get_higher_timeframes(GLOBAL_TIMEFRAME)
        aligned_timeframes = 0
        
        for tf in higher_timeframes[:1]:
            if tf in analyses:
                higher_analysis = analyses[tf]
                if primary_analysis['ema_direction'] == higher_analysis['ema_direction']:
                    aligned_timeframes += 1
        
        if aligned_timeframes < 1:
            continue
        
        # Current price relative to EMA (your method)
        close_to_ema = abs(latest['close'] - latest['ema20']) / latest['ema20'] < params['ema_buffer_pct']
        
        # ENHANCED: Check each direction with intelligence overlay
        for direction in ['long', 'short']:
            # Skip if we already have position in this direction
            if trade_manager.has_position(symbol, direction):
                continue
            
            # YOUR PROVEN SIGNAL VALIDATION (preserved)
            signal_valid = False
            ta_strength = 50  # Base technical strength
            
            if direction == 'long':
                bullish_trend = primary_analysis['ema_direction'] == 'Up'
                rsi_condition = (prev['rsi'] < params['rsi_oversold'] and 
                               latest['rsi'] > params['rsi_oversold'])
                price_action = latest['close'] > latest['open']
                signal_valid = bullish_trend and close_to_ema and (rsi_condition or price_action)
                
                # Calculate TA strength
                ta_strength = 30  # Base
                if bullish_trend: ta_strength += 30
                if rsi_condition: ta_strength += 20
                if price_action: ta_strength += 20
                
            else:  # short
                bearish_trend = primary_analysis['ema_direction'] == 'Down'
                rsi_condition = (prev['rsi'] > params['rsi_overbought'] and 
                               latest['rsi'] < params['rsi_overbought'])
                price_action = latest['close'] < latest['open']
                signal_valid = bearish_trend and close_to_ema and (rsi_condition or price_action)
                
                # Calculate TA strength
                ta_strength = 30  # Base
                if bearish_trend: ta_strength += 30
                if rsi_condition: ta_strength += 20
                if price_action: ta_strength += 20
            
            if signal_valid:
                # ENHANCED: Check with intelligence engine
                can_trade_smart, confidence, reasons = trade_manager.can_trade_enhanced(
                    symbol, direction, ta_strength
                )
                
                if not can_trade_smart:
                    logger.info(f"🧠 {symbol} {direction} blocked by intelligence: {'; '.join(reasons)}")
                    continue
                
                # Calculate entry, SL, TP (your original logic preserved)
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
                
                # Validate SL/TP distances (your method)
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
                        
                        # ENHANCED: Add intelligence data
                        'enhanced_validation': True,
                        'ta_strength': ta_strength,
                        'final_confidence': confidence,
                        'intelligence_reasons': reasons,
                        'sl_distance': abs(entry_price - sl)  # For martingale
                    })
                    
                    logger.info(f"🎯 Enhanced signal: {symbol} {direction} (TA: {ta_strength}%, Final: {confidence:.1f}%)")
    
    return signals

# ===== ENHANCED MARTINGALE EXECUTION =====
def execute_martingale_trade_enhanced(opportunity, trade_manager):
    """Enhanced martingale execution with intelligence checks"""
    
    batch = opportunity['batch']
    symbol = opportunity['symbol']
    direction = opportunity['direction']
    layer = opportunity['layer']
    
    # CRITICAL: For existing batches with multiple layers, 
    # bypass intelligence checks to protect existing investment
    if layer >= 3:
        logger.info(f"🔄 Layer {layer} - Protecting existing investment, bypassing intelligence checks")
        bypass_intelligence = True
    else:
        bypass_intelligence = False
    
    # Enhanced check for new layers (but not deep layers)
    if not bypass_intelligence and INTELLIGENCE_CONFIG['ENHANCED_FEATURES_ENABLED']:
        can_trade_smart, confidence, reasons = trade_manager.can_trade_enhanced(
            symbol, direction, ta_strength=80  # Assume good TA for martingale
        )
        
        if not can_trade_smart:
            logger.info(f"🧠 Martingale {symbol} {direction} Layer {layer} blocked: {'; '.join(reasons)}")
            return False
    
    # Use your proven martingale execution
    from core.trading_engine_backup import execute_martingale_trade
    
    # Create signal with proper structure
    martingale_signal = {
        'symbol': symbol,
        'direction': direction,
        'entry_price': opportunity['entry_price'],
        'sl': None,  # No SL for build-from-first
        'tp': None,  # Will be calculated by batch
        'sl_distance_pips': batch.initial_sl_distance / get_pip_size(symbol),
        'risk_profile': PAIR_RISK_PROFILES.get(symbol, "High"),
        'is_initial': False,
        'layer': layer,
        'sl_distance': batch.initial_sl_distance
    }
    
    return execute_martingale_trade(martingale_signal, trade_manager.original_manager)

# ===== ENHANCED TRADE EXECUTION =====
def execute_enhanced_trade(signal, trade_manager):
    """Enhanced trade execution with intelligent position sizing"""
    
    symbol = signal['symbol']
    direction = signal['direction']
    
    # Get confidence level from signal
    confidence = signal.get('final_confidence', 100)
    
    # Enhanced risk calculation
    risk_profile = signal['risk_profile']
    params = PARAM_SETS[risk_profile]
    base_risk_pct = params['risk_per_trade_pct']
    
    # Apply enhanced position sizing
    enhanced_risk_amount = trade_manager.calculate_enhanced_risk_amount(
        symbol, base_risk_pct, confidence
    )
    
    # Update signal with enhanced data
    signal['enhanced_risk_amount'] = enhanced_risk_amount
    signal['confidence_level'] = confidence
    
    # Use your proven execution logic
    from core.trading_engine_backup import execute_trade
    
    return execute_trade(signal, trade_manager.original_manager)

# ===== ENHANCED SYSTEM STATUS =====
class EnhancedSystemStatus:
    """Provides comprehensive system status including intelligence"""
    
    def __init__(self, trade_manager):
        self.trade_manager = trade_manager
        self.data_manager = EnhancedDataManager()
    
    def get_comprehensive_status(self):
        """Get complete system status"""
        try:
            # Get account info
            account_info = mt5.account_info()
            if not account_info:
                return {"error": "Cannot get account info"}
            
            # Basic account status
            status = {
                'timestamp': datetime.now().isoformat(),
                'account': {
                    'balance': account_info.balance,
                    'equity': account_info.equity,
                    'margin': account_info.margin,
                    'free_margin': account_info.margin_free,
                    'margin_level': (account_info.equity / account_info.margin * 100) if account_info.margin > 0 else 1000
                },
                'trading': {
                    'active_trades': len(self.trade_manager.active_trades),
                    'active_batches': len([b for b in self.trade_manager.martingale_batches.values() if b.trades]),
                    'total_trades': self.trade_manager.total_trades,
                    'emergency_stop': self.trade_manager.emergency_stop_active
                },
                'intelligence': {
                    'enabled': INTELLIGENCE_CONFIG['ENHANCED_FEATURES_ENABLED'],
                    'features': {},
                    'data_status': {},
                    'risk_level': INTELLIGENCE_CONFIG['MASTER_RISK_LEVEL']
                },
                'configuration': INTELLIGENCE_CONFIG.copy()
            }
            
            # Intelligence feature status
            if INTELLIGENCE_CONFIG['ENHANCED_FEATURES_ENABLED']:
                status['intelligence']['features'] = {
                    'sentiment_blocking': INTELLIGENCE_CONFIG['USE_SENTIMENT_BLOCKING'],
                    'correlation_risk': INTELLIGENCE_CONFIG['USE_CORRELATION_RISK'],
                    'economic_timing': INTELLIGENCE_CONFIG['USE_ECONOMIC_TIMING'],
                    'dynamic_sizing': INTELLIGENCE_CONFIG['USE_DYNAMIC_POSITION_SIZING']
                }
                
                # Data source status
                status['intelligence']['data_status'] = self._get_data_status()
            
            # P&L calculation
            if self.trade_manager.initial_balance:
                pnl = account_info.equity - self.trade_manager.initial_balance
                pnl_pct = (pnl / self.trade_manager.initial_balance) * 100
                status['account']['pnl'] = pnl
                status['account']['pnl_percentage'] = pnl_pct
            
            return status
            
        except Exception as e:
            logger.error(f"Error getting system status: {e}")
            return {"error": str(e)}
    
    def _get_data_status(self):
        """Get data source status"""
        try:
            data_status = {}
            
            # Sentiment status
            if INTELLIGENCE_CONFIG['USE_SENTIMENT_BLOCKING']:
                try:
                    sentiment_data = self.data_manager.get_sentiment_data()
                    blocked_pairs = sum(1 for pair_data in sentiment_data.values() 
                                      if pair_data.get('blocked_directions'))
                    data_status['sentiment'] = {
                        'available': len(sentiment_data) > 0,
                        'pairs_count': len(sentiment_data),
                        'blocked_pairs': blocked_pairs
                    }
                except:
                    data_status['sentiment'] = {'available': False, 'error': 'Load failed'}
            
            # Correlation status
            if INTELLIGENCE_CONFIG['USE_CORRELATION_RISK']:
                try:
                    correlation_data = self.data_manager.get_correlation_data()
                    warnings_count = len(correlation_data.get('warnings', []))
                    data_status['correlation'] = {
                        'available': len(correlation_data.get('matrix', {})) > 0,
                        'warnings_count': warnings_count
                    }
                except:
                    data_status['correlation'] = {'available': False, 'error': 'Load failed'}
            
            # Economic events status
            if INTELLIGENCE_CONFIG['USE_ECONOMIC_TIMING']:
                try:
                    events = self.data_manager.get_economic_events(24)
                    high_impact = sum(1 for e in events if e.get('impact') == 'high')
                    data_status['economic'] = {
                        'available': True,
                        'upcoming_events': len(events),
                        'high_impact_events': high_impact
                    }
                except:
                    data_status['economic'] = {'available': False, 'error': 'Load failed'}
            
            return data_status
            
        except Exception as e:
            return {'error': str(e)}
    
    def log_status_summary(self):
        """Log comprehensive status summary"""
        try:
            status = self.get_comprehensive_status()
            
            # Account summary
            account = status.get('account', {})
            logger.info(f"💰 Account: Balance=${account.get('balance', 0):.2f}, "
                       f"Equity=${account.get('equity', 0):.2f}, "
                       f"Margin Level={account.get('margin_level', 0):.1f}%")
            
            if 'pnl_percentage' in account:
                pnl_pct = account['pnl_percentage']
                pnl_emoji = "🟢" if pnl_pct > 0 else "🔴" if pnl_pct < 0 else "⚪"
                logger.info(f"📊 P&L: {pnl_emoji} {pnl_pct:+.2f}% (${account.get('pnl', 0):+.2f})")
            
            # Trading summary
            trading = status.get('trading', {})
            logger.info(f"🎯 Trading: {trading.get('active_trades', 0)} positions, "
                       f"{trading.get('active_batches', 0)} active batches")
            
            # Intelligence summary
            intelligence = status.get('intelligence', {})
            if intelligence.get('enabled'):
                features = intelligence.get('features', {})
                enabled_features = [name for name, enabled in features.items() if enabled]
                logger.info(f"🧠 Intelligence: {len(enabled_features)} features active")
                
                data_status = intelligence.get('data_status', {})
                for source, info in data_status.items():
                    status_emoji = "✅" if info.get('available') else "❌"
                    logger.info(f"   {status_emoji} {source}: {info}")
            else:
                logger.info("🧠 Intelligence: DISABLED - Pure TA mode")
            
        except Exception as e:
            logger.error(f"Error logging status: {e}")

# ===== ENHANCED MAIN ROBOT FUNCTION =====
def run_enhanced_robot():
    """Main enhanced robot function preserving your proven logic"""
    logger.info("="*80)
    logger.info("🚀 ENHANCED TRADING ROBOT - PHASE 3 COMPLETE")
    logger.info("="*80)
    logger.info(f"Primary Timeframe: M5")
    logger.info(f"Pairs: {len(PAIRS)}")
    logger.info(f"Martingale: {MARTINGALE_ENABLED} (PRESERVED)")
    logger.info(f"Enhanced Features: {INTELLIGENCE_CONFIG['ENHANCED_FEATURES_ENABLED']}")
    logger.info(f"Master Risk Level: {INTELLIGENCE_CONFIG['MASTER_RISK_LEVEL']}%")
    logger.info(f"TA Weight: {INTELLIGENCE_CONFIG['TA_WEIGHT']}%, Data Weight: {INTELLIGENCE_CONFIG['DATA_WEIGHT']}%")
    logger.info("="*80)
    
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
    
    # Initialize enhanced trade manager (with your proven base)
    trade_manager = EnhancedTradeManager()
    
    # Initialize status monitor
    status_monitor = EnhancedSystemStatus(trade_manager)
    
    # Test data connectivity
    logger.info("🔄 Testing enhanced data connectivity...")
    data_manager = EnhancedDataManager()
    
    # Test each data source
    sentiment_data = data_manager.get_sentiment_data()
    logger.info(f"📊 Sentiment data: {len(sentiment_data)} pairs loaded")
    
    correlation_data = data_manager.get_correlation_data()
    logger.info(f"🔗 Correlation data: {len(correlation_data.get('warnings', []))} warnings")
    
    economic_events = data_manager.get_economic_events(24)
    logger.info(f"📅 Economic events: {len(economic_events)} upcoming events")
    
    try:
        cycle_count = 0
        consecutive_errors = 0
        
        while True:
            try:
                cycle_count += 1
                current_time = datetime.now()
                
                logger.info(f"\n{'='*60}")
                logger.info(f"🔄 Enhanced Analysis Cycle #{cycle_count} at {current_time}")
                logger.info(f"{'='*60}")
                
                # Reset error counter
                consecutive_errors = 0
                
                # Check MT5 connection
                if not mt5.terminal_info():
                    logger.warning("MT5 disconnected, attempting reconnect...")
                    if not mt5.initialize():
                        logger.error("Reconnection failed")
                        consecutive_errors += 1
                        if consecutive_errors >= 5:
                            break
                        time.sleep(30)
                        continue
                
                # Get current prices
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
                    logger.warning("No price data available. Skipping cycle...")
                    time.sleep(30)
                    continue
                
                # Generate enhanced signals (preserving your TA)
                try:
                    signals = generate_enhanced_signals(PAIRS, trade_manager)
                    logger.info(f"Generated {len(signals)} enhanced signals")
                except Exception as e:
                    logger.error(f"Error generating signals: {e}")
                    signals = []
                
                # Execute signals with enhanced logic
                for signal in signals:
                    try:
                        if not trade_manager.can_trade(signal['symbol']):
                            continue
                        
                        logger.info(f"\n🎯 Enhanced Signal: {signal['symbol']} {signal['direction'].upper()}")
                        logger.info(f"   TA Strength: {signal['ta_strength']}%")
                        logger.info(f"   Final Confidence: {signal['final_confidence']:.1f}%")
                        logger.info(f"   Intelligence: {'; '.join(signal['intelligence_reasons'])}")
                        logger.info(f"   Entry: {signal['entry_price']:.5f}")
                        logger.info(f"   SL Distance: {signal['sl_distance_pips']:.1f} pips")
                        logger.info(f"   🚫 NO SL - Build-from-first approach (PRESERVED)")
                        
                        if execute_enhanced_trade(signal, trade_manager):
                            logger.info("✅ Enhanced trade executed successfully")
                        else:
                            logger.error("❌ Trade execution failed")
                            
                    except Exception as e:
                        logger.error(f"Error executing signal for {signal.get('symbol', 'Unknown')}: {e}")
                        continue
                
                # Enhanced martingale with protection for existing batches
                if MARTINGALE_ENABLED and not trade_manager.emergency_stop_active:
                    try:
                        # Use your proven martingale check
                        martingale_opportunities = trade_manager.check_martingale_opportunities_enhanced(current_prices)
                        
                        for opportunity in martingale_opportunities:
                            try:
                                symbol = opportunity['symbol']
                                direction = opportunity['direction']
                                layer = opportunity['layer']
                                
                                logger.info(f"\n🔄 Enhanced Martingale: {symbol} {direction} Layer {layer}")
                                logger.info(f"   Trigger: {opportunity['trigger_price']:.5f}")
                                logger.info(f"   Current: {opportunity['entry_price']:.5f}")
                                logger.info(f"   Distance: {opportunity['distance_pips']:.1f} pips")
                                
                                if execute_martingale_trade_enhanced(opportunity, trade_manager):
                                    logger.info("✅ Enhanced martingale executed successfully")
                                    
                                    # Update batch TP (your proven logic)
                                    batch = opportunity['batch']
                                    try:
                                        new_tp = batch.calculate_adaptive_batch_tp()
                                        if new_tp:
                                            logger.info(f"🔄 Updating batch TP to {new_tp:.5f}")
                                            batch.update_all_tps_with_retry(new_tp)
                                    except Exception as e:
                                        logger.error(f"Error updating batch TP: {e}")
                                else:
                                    logger.error("❌ Enhanced martingale execution failed")
                                    
                            except Exception as e:
                                logger.error(f"Error executing martingale: {e}")
                                continue
                                
                    except Exception as e:
                        logger.error(f"Error checking martingale opportunities: {e}")
                
                # Sync with MT5 (your proven method)
                try:
                    trade_manager.sync_with_mt5_positions()
                except Exception as e:
                    logger.error(f"Error syncing with MT5: {e}")
                
                # Monitor batch exits (your proven method)
                try:
                    trade_manager.monitor_batch_exits(current_prices)
                except Exception as e:
                    logger.error(f"Error monitoring batch exits: {e}")
                
                # Enhanced status display
                try:
                    status_monitor.log_status_summary()
                except Exception as e:
                    logger.error(f"Error displaying status: {e}")
                
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
                    time.sleep(60)
                    
            except KeyboardInterrupt:
                logger.info("\n🛑 Enhanced robot stopped by user")
                raise
                
            except Exception as e:
                consecutive_errors += 1
                logger.error(f"\n❌ Error in enhanced cycle #{cycle_count}: {e}")
                logger.error(f"Consecutive errors: {consecutive_errors}")
                
                # Emergency state save
                try:
                    trade_manager.persistence.save_bot_state(trade_manager.original_manager)
                    logger.info("💾 Emergency state saved")
                except Exception as save_error:
                    logger.error(f"Failed to save emergency state: {save_error}")
                
                if consecutive_errors >= 10:
                    logger.critical(f"🚨 Too many consecutive errors - stopping enhanced robot")
                    break
                
                import traceback
                logger.error(f"Detailed error:\n{traceback.format_exc()}")
                
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
        # Final cleanup
        try:
            logger.info("🔄 Performing final cleanup...")
            trade_manager.persistence.save_bot_state(trade_manager.original_manager)
            logger.info("💾 Final state saved successfully")
        except Exception as e:
            logger.error(f"Error during cleanup: {e}")
        
        try:
            mt5.shutdown()
            logger.info("MT5 connection closed")
        except Exception as e:
            logger.error(f"Error closing MT5: {e}")

# ===== CONFIGURATION UTILITIES =====
def update_intelligence_config(**kwargs):
    """Update intelligence configuration dynamically"""
    global INTELLIGENCE_CONFIG
    
    for key, value in kwargs.items():
        if key in INTELLIGENCE_CONFIG:
            old_value = INTELLIGENCE_CONFIG[key]
            INTELLIGENCE_CONFIG[key] = value
            logger.info(f"🔧 Config updated: {key} = {value} (was {old_value})")
        else:
            logger.warning(f"⚠️ Unknown config key: {key}")

def get_current_config():
    """Get current configuration"""
    return INTELLIGENCE_CONFIG.copy()

def reset_to_pure_ta_mode():
    """Reset to pure technical analysis mode"""
    update_intelligence_config(
        ENHANCED_FEATURES_ENABLED=False,
        USE_SENTIMENT_BLOCKING=False,
        USE_CORRELATION_RISK=False,
        USE_ECONOMIC_TIMING=False,
        USE_DYNAMIC_POSITION_SIZING=False
    )
    logger.info("🔧 Reset to Pure TA Mode - All intelligence features disabled")

def enable_full_intelligence():
    """Enable all intelligence features"""
    update_intelligence_config(
        ENHANCED_FEATURES_ENABLED=True,
        USE_SENTIMENT_BLOCKING=True,
        USE_CORRELATION_RISK=True,
        USE_ECONOMIC_TIMING=True,
        USE_DYNAMIC_POSITION_SIZING=True
    )
    logger.info("🧠 Full Intelligence Mode - All features enabled")

def set_risk_level(level):
    """Set master risk level (0-200%)"""
    level = max(0, min(200, level))
    update_intelligence_config(MASTER_RISK_LEVEL=level)
    logger.info(f"💰 Master risk level set to {level}%")

def set_ta_data_weights(ta_weight, data_weight):
    """Set TA vs Data weights (must add to 100)"""
    if ta_weight + data_weight != 100:
        logger.error("❌ TA and Data weights must add to 100%")
        return False
    
    update_intelligence_config(
        TA_WEIGHT=ta_weight,
        DATA_WEIGHT=data_weight
    )
    logger.info(f"⚖️ Weights updated: TA={ta_weight}%, Data={data_weight}%")
    return True

# ===== TESTING AND UTILITIES =====
def test_intelligence_features():
    """Test all intelligence features"""
    logger.info("🧪 Testing Enhanced Intelligence Features...")
    
    try:
        # Test data manager
        data_manager = EnhancedDataManager()
        
        # Test sentiment
        sentiment_data = data_manager.get_sentiment_data()
        logger.info(f"✅ Sentiment: {len(sentiment_data)} pairs loaded")
        
        # Test correlation
        correlation_data = data_manager.get_correlation_data()
        logger.info(f"✅ Correlation: {len(correlation_data.get('warnings', []))} warnings")
        
        # Test economic events
        events = data_manager.get_economic_events(24)
        logger.info(f"✅ Economic: {len(events)} upcoming events")
        
        # Test decision engine
        decision_engine = EnhancedDecisionEngine()
        
        test_cases = [
            ('EURUSD', 'long', 80),
            ('GBPUSD', 'short', 70),
            ('XAUUSD', 'long', 90)
        ]
        
        for symbol, direction, ta_strength in test_cases:
            can_trade, confidence, reasons = decision_engine.can_trade_direction(
                symbol, direction, ta_strength
            )
            logger.info(f"✅ Decision Test: {symbol} {direction} - {can_trade} ({confidence:.1f}%)")
        
        # Test position sizing
        position_sizer = EnhancedPositionSizing()
        test_risk = position_sizer.calculate_enhanced_position_size('EURUSD', 100, 80)
        logger.info(f"✅ Position Sizing: $100 -> ${test_risk:.2f}")
        
        logger.info("🧪 All intelligence features tested successfully!")
        return True
        
    except Exception as e:
        logger.error(f"❌ Intelligence test failed: {e}")
        return False

if __name__ == "__main__":
    import sys
    
    if len(sys.argv) > 1:
        command = sys.argv[1].lower()
        
        if command == 'test':
            test_intelligence_features()
        elif command == 'pure_ta':
            reset_to_pure_ta_mode()
        elif command == 'full_intel':
            enable_full_intelligence()
        elif command == 'config':
            print(json.dumps(get_current_config(), indent=2))
        else:
            print("Usage: python enhanced_trading_engine.py [test|pure_ta|full_intel|config]")
            print("Or run without arguments to start the enhanced robot")
    else:
        # Start the enhanced robot
        run_enhanced_robot()