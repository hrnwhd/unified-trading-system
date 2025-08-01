#!/usr/bin/env python3
# ===== ENHANCED TRADING ENGINE - UNIFIED VERSION =====
# Uses unified configuration manager and data manager
# NO hardcoded configurations - everything comes from unified config

import MetaTrader5 as mt5
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import warnings
import time
import logging
import json
import os
import sys
from pathlib import Path

# Suppress warnings
warnings.filterwarnings("ignore")

# Add paths for imports
sys.path.insert(0, str(Path(__file__).parent))
sys.path.insert(0, str(Path(__file__).parent / 'core'))

# Import unified managers
from unified_config_manager import UnifiedConfigManager
from unified_data_manager import UnifiedDataManager

# ===== LOGGING SETUP =====
def setup_logging(config_manager):
    """Setup logging based on configuration"""
    log_level = getattr(logging, config_manager.get('logging.level', 'INFO'))
    log_file = config_manager.get('file_paths.trading_log', 'enhanced_trading.log')
    
    logging.basicConfig(
        level=log_level,
        format='%(asctime)s - %(levelname)s - %(message)s',
        handlers=[
            logging.FileHandler(log_file, encoding='utf-8'),
            logging.StreamHandler() if config_manager.get('logging.console_output', True) else logging.NullHandler()
        ]
    )
    return logging.getLogger(__name__)

# ===== ENHANCED DATA INTEGRATION MANAGER =====
class EnhancedDataManager:
    """Manages all external data sources with unified configuration"""
    
    def __init__(self, config_manager, data_manager):
        self.config = config_manager
        self.data_manager = data_manager
        self.intelligence_config = config_manager.get_intelligence_config()
        self.risk_thresholds = config_manager.get_risk_thresholds()
    
    def get_sentiment_data(self):
        """Get sentiment data with fallback"""
        if not self.intelligence_config['USE_SENTIMENT_BLOCKING']:
            return self._get_fallback_sentiment()
        
        try:
            sentiment_data = self.data_manager.get_sentiment_data()
            
            # Check data freshness using config
            freshness_limit = self.config.get('intelligence.sentiment_blocking.freshness_limit_minutes', 60)
            if not self.data_manager.is_data_fresh('sentiment', freshness_limit):
                logger.warning(f"⚠️ Sentiment data stale, using fallback")
                return self._get_fallback_sentiment()
            
            logger.debug(f"✅ Fresh sentiment data loaded")
            return sentiment_data
            
        except Exception as e:
            logger.error(f"❌ Error loading sentiment data: {e}")
            return self._get_fallback_sentiment()
    
    def get_correlation_data(self):
        """Get correlation data with fallback"""
        if not self.intelligence_config['USE_CORRELATION_RISK']:
            return {'matrix': {}, 'warnings': []}
        
        try:
            correlation_data = self.data_manager.get_correlation_data()
            
            # Check data freshness using config
            freshness_limit = self.config.get('intelligence.correlation_risk.freshness_limit_minutes', 60)
            if not self.data_manager.is_data_fresh('correlation', freshness_limit):
                logger.warning(f"⚠️ Correlation data stale")
                return {'matrix': {}, 'warnings': []}
            
            return correlation_data
            
        except Exception as e:
            logger.error(f"❌ Error loading correlation data: {e}")
            return {'matrix': {}, 'warnings': []}
    
    def get_economic_events(self, hours_ahead=24):
        """Get upcoming economic events"""
        if not self.intelligence_config['USE_ECONOMIC_TIMING']:
            return []
        
        try:
            # Check data freshness using config
            freshness_limit = self.config.get('intelligence.economic_timing.freshness_limit_minutes', 120)
            if not self.data_manager.is_data_fresh('economic_calendar', freshness_limit):
                logger.warning(f"⚠️ Economic calendar data stale")
                return []
            
            return self.data_manager.get_economic_events(hours_ahead)
            
        except Exception as e:
            logger.error(f"❌ Error loading economic events: {e}")
            return []
    
    def _get_fallback_sentiment(self):
        """Fallback sentiment - allow all directions"""
        fallback = {}
        pairs = self.config.get_pairs_list()
        for pair in pairs:
            fallback[pair] = {
                'allowed_directions': ['long', 'short'],
                'blocked_directions': [],
                'sentiment': {'short': 50, 'long': 50},
                'signal_strength': 'Fallback'
            }
        return fallback

# ===== ENHANCED DECISION ENGINE =====
class EnhancedDecisionEngine:
    """Makes intelligent trading decisions using unified configuration"""
    
    def __init__(self, config_manager, data_manager):
        self.config = config_manager
        self.data_manager = data_manager
        self.intelligence_config = config_manager.get_intelligence_config()
        self.risk_thresholds = config_manager.get_risk_thresholds()
        self.enhanced_data_manager = EnhancedDataManager(config_manager, data_manager)
        self.decision_log = []
    
    def can_trade_direction(self, symbol, direction, ta_signal_strength=100):
        """Enhanced decision making with unified configuration"""
        
        if not self.intelligence_config['ENHANCED_FEATURES_ENABLED']:
            return True, ta_signal_strength, ["Pure TA mode"]
        
        try:
            reasons = []
            blocking_factors = []
            risk_factors = []
            
            # Get weights from config
            ta_weight = self.intelligence_config['TA_WEIGHT'] / 100
            data_weight = self.intelligence_config['DATA_WEIGHT'] / 100
            
            base_confidence = ta_signal_strength * ta_weight
            
            # Check 1: Sentiment Analysis
            sentiment_adjustment = 0
            if self.intelligence_config['USE_SENTIMENT_BLOCKING']:
                sentiment_check = self._check_sentiment(symbol, direction)
                if not sentiment_check['allowed']:
                    blocking_factors.append(f"Sentiment: {sentiment_check['reason']}")
                else:
                    sentiment_adjustment = sentiment_check.get('confidence_boost', 0)
                    reasons.append(f"Sentiment: OK ({sentiment_check['reason']})")
            
            # Check 2: Correlation Risk
            correlation_adjustment = 0
            if self.intelligence_config['USE_CORRELATION_RISK']:
                correlation_check = self._check_correlation_risk(symbol)
                if correlation_check['high_risk']:
                    risk_factors.append(f"Correlation: {correlation_check['reason']}")
                    correlation_adjustment = -10
                else:
                    reasons.append(f"Correlation: OK")
            
            # Check 3: Economic Events
            economic_adjustment = 0
            if self.intelligence_config['USE_ECONOMIC_TIMING']:
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
            
            # Apply master risk level from config
            final_confidence *= (self.intelligence_config['MASTER_RISK_LEVEL'] / 100)
            
            # Cap confidence
            final_confidence = max(0, min(100, final_confidence))
            
            # Decision logic using config
            min_confidence = self.config.get('risk_management.min_confidence_to_trade', 30)
            can_trade = final_confidence >= min_confidence
            
            if risk_factors:
                reasons.extend(risk_factors)
            
            self._log_decision(symbol, direction, can_trade, final_confidence, reasons)
            
            return can_trade, final_confidence, reasons
            
        except Exception as e:
            logger.error(f"❌ Error in enhanced decision for {symbol} {direction}: {e}")
            return True, ta_signal_strength, [f"Error in decision engine: {e}"]
    
    def _check_sentiment(self, symbol, direction):
        """Check sentiment blocking using unified config"""
        try:
            sentiment_data = self.enhanced_data_manager.get_sentiment_data()
            
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
            
            # Check blocked directions using config threshold
            blocked_directions = sentiment_info.get('blocked_directions', [])
            if direction in blocked_directions:
                sentiment = sentiment_info.get('sentiment', {})
                return {
                    'allowed': False,
                    'reason': f"Extreme sentiment - {sentiment.get('short', 0)}%↓ {sentiment.get('long', 0)}%↑"
                }
            
            # Calculate confidence boost
            sentiment = sentiment_info.get('sentiment', {})
            if direction == 'long':
                long_pct = sentiment.get('long', 50)
                confidence_boost = max(0, (long_pct - 50) / 5) if long_pct > 60 else 0
            else:
                short_pct = sentiment.get('short', 50)
                confidence_boost = max(0, (short_pct - 50) / 5) if short_pct > 60 else 0
            
            return {
                'allowed': True,
                'reason': f"Sentiment supports {direction}",
                'confidence_boost': confidence_boost
            }
            
        except Exception as e:
            logger.warning(f"Error checking sentiment: {e}")
            return {'allowed': True, 'reason': 'Sentiment check error', 'confidence_boost': 0}
    
    def _check_correlation_risk(self, symbol):
        """Check correlation-based risk using unified config"""
        try:
            correlation_data = self.enhanced_data_manager.get_correlation_data()
            warnings = correlation_data.get('warnings', [])
            
            # Use config threshold
            warning_threshold = self.config.get('intelligence.correlation_risk.warning_threshold', 3)
            
            high_corr_count = 0
            for warning in warnings:
                if warning.get('type') == 'HIGH_CORRELATION':
                    pair = warning.get('pair', '')
                    if symbol in pair:
                        high_corr_count += 1
            
            if high_corr_count >= warning_threshold:
                return {
                    'high_risk': True,
                    'reason': f'{high_corr_count} high correlations detected'
                }
            
            return {'high_risk': False, 'reason': 'Low correlation risk'}
            
        except Exception as e:
            logger.warning(f"Error checking correlation: {e}")
            return {'high_risk': False, 'reason': 'Correlation check error'}
    
    def _check_economic_timing(self, symbol):
        """Check economic event timing using unified config"""
        try:
            buffer_hours = self.risk_thresholds['ECONOMIC_BUFFER_HOURS']
            upcoming_events = self.enhanced_data_manager.get_economic_events(buffer_hours)
            
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
                    time_until <= buffer_hours):
                    
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
    """Calculates position sizes using unified configuration"""
    
    def __init__(self, config_manager, data_manager):
        self.config = config_manager
        self.enhanced_data_manager = EnhancedDataManager(config_manager, data_manager)
        self.intelligence_config = config_manager.get_intelligence_config()
    
    def calculate_enhanced_position_size(self, symbol, base_risk_amount, confidence_level=100):
        """Calculate position size with enhanced risk management"""
        
        if not self.intelligence_config['USE_DYNAMIC_POSITION_SIZING']:
            return base_risk_amount
        
        try:
            adjustments = []
            risk_multiplier = 1.0
            
            # Apply master risk level from config
            master_risk = self.intelligence_config['MASTER_RISK_LEVEL'] / 100
            risk_multiplier *= master_risk
            if master_risk != 1.0:
                adjustments.append(f"Master risk: {master_risk:.0%}")
            
            # Apply confidence-based sizing
            confidence_multiplier = confidence_level / 100
            risk_multiplier *= confidence_multiplier
            adjustments.append(f"Confidence: {confidence_level:.0f}%")
            
            # Check correlation warnings using config
            if self.intelligence_config['USE_CORRELATION_RISK']:
                correlation_data = self.enhanced_data_manager.get_correlation_data()
                warnings = correlation_data.get('warnings', [])
                
                high_corr_count = sum(1 for w in warnings 
                                    if w.get('type') == 'HIGH_CORRELATION' and symbol in w.get('pair', ''))
                
                warning_threshold = self.config.get('intelligence.correlation_risk.warning_threshold', 3)
                if high_corr_count >= warning_threshold - 1:  # Trigger one before threshold
                    risk_reduction = self.config.get('intelligence.correlation_risk.risk_reduction_factor', 0.8)
                    risk_multiplier *= risk_reduction
                    adjustments.append(f"High correlation: -{int((1-risk_reduction)*100)}%")
            
            # Check for major economic events using config
            if self.intelligence_config['USE_ECONOMIC_TIMING']:
                buffer_hours = self.config.get('intelligence.economic_timing.buffer_hours', 1) * 6  # Look ahead 6x buffer
                upcoming_events = self.enhanced_data_manager.get_economic_events(buffer_hours)
                high_impact_events = [e for e in upcoming_events if e.get('impact') == 'high']
                
                if high_impact_events:
                    risk_reduction = self.config.get('intelligence.economic_timing.risk_reduction_factor', 0.7)
                    risk_multiplier *= risk_reduction
                    adjustments.append(f"Major events: -{int((1-risk_reduction)*100)}%")
            
            # Apply final adjustment
            adjusted_risk = base_risk_amount * risk_multiplier
            
            # Log significant adjustments
            if abs(risk_multiplier - 1.0) > 0.1:
                logger.info(f"💰 {symbol} risk adjusted: ${base_risk_amount:.2f} → ${adjusted_risk:.2f} ({risk_multiplier:.2f}x)")
                for adjustment in adjustments:
                    logger.info(f"   • {adjustment}")
            
            return adjusted_risk
            
        except Exception as e:
            logger.error(f"❌ Error calculating enhanced position size: {e}")
            return base_risk_amount

# ===== ENHANCED TRADE MANAGER =====
class EnhancedTradeManager:
    """Enhanced trade manager using unified configuration and proven base"""
    
    def __init__(self, config_manager, data_manager):
        self.config = config_manager
        self.intelligence_config = config_manager.get_intelligence_config()
        self.martingale_config = config_manager.get_martingale_config()
        
        # Import and initialize proven base
        try:
            from trading_engine_backup import (
                EnhancedTradeManager as OriginalTradeManager,
                BotPersistence
            )
            self.original_manager = OriginalTradeManager()
        except ImportError:
            logger.error("❌ Could not import original trade manager")
            raise
        
        # Enhanced components
        self.decision_engine = EnhancedDecisionEngine(config_manager, data_manager)
        self.position_sizer = EnhancedPositionSizing(config_manager, data_manager)
        
        # Preserve all original properties
        self.active_trades = self.original_manager.active_trades
        self.martingale_batches = self.original_manager.martingale_batches
        self.total_trades = self.original_manager.total_trades
        self.emergency_stop_active = self.original_manager.emergency_stop_active
        self.initial_balance = self.original_manager.initial_balance
        self.next_batch_id = self.original_manager.next_batch_id
        self.persistence = self.original_manager.persistence
        
        logger.info("✅ Enhanced Trade Manager initialized with unified config")
    
    def can_trade_enhanced(self, symbol, direction, ta_signal_strength=100):
        """Enhanced can_trade with intelligence integration"""
        
        # First run original basic checks
        if not self.original_manager.can_trade(symbol):
            return False
        
        # If enhanced features disabled, use original logic only
        if not self.intelligence_config['ENHANCED_FEATURES_ENABLED']:
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
            account_info = mt5.account_info()
            if not account_info:
                return 0
            
            base_risk_amount = account_info.balance * (base_risk_pct / 100)
            
            enhanced_risk = self.position_sizer.calculate_enhanced_position_size(
                symbol, base_risk_amount, confidence_level
            )
            
            return enhanced_risk
            
        except Exception as e:
            logger.error(f"Error calculating enhanced risk: {e}")
            return account_info.balance * (base_risk_pct / 100) if account_info else 0
    
    # Delegate all other methods to original manager
    def __getattr__(self, name):
        """Delegate unknown methods to original manager"""
        return getattr(self.original_manager, name)

# ===== ENHANCED SIGNAL GENERATION =====
def generate_enhanced_signals(config_manager, trade_manager):
    """Enhanced signal generation using unified configuration"""
    
    # Import proven TA functions
    try:
        from trading_engine_backup import (
            analyze_symbol_multi_timeframe,
            get_historical_data,
            calculate_indicators,
            calculate_atr,
            get_pip_size,
            get_higher_timeframes
        )
    except ImportError:
        logger.error("❌ Could not import TA functions from trading_engine_backup")
        return []
    
    signals = []
    pairs = config_manager.get_pairs_list()
    pair_risk_profiles = config_manager.get_pair_risk_profiles()
    param_sets = config_manager.get_param_sets()
    global_timeframe = config_manager.get_timeframe_constant()
    
    for symbol in pairs:
        if not trade_manager.can_trade(symbol):
            continue
            
        # Skip if we already have positions in both directions
        if (trade_manager.has_position(symbol, 'long') and 
            trade_manager.has_position(symbol, 'short')):
            continue
        
        # Multi-timeframe analysis (preserved from original)
        analyses = analyze_symbol_multi_timeframe(symbol, global_timeframe)
        
        if not analyses or global_timeframe not in analyses:
            continue
        
        primary_analysis = analyses[global_timeframe]
        
        # Get risk profile and parameters from config
        risk_profile = pair_risk_profiles.get(symbol, "High")
        params = param_sets.get(risk_profile, param_sets.get("Medium", {}))
        
        # Get primary timeframe data
        df = get_historical_data(symbol, global_timeframe, 500)
        if df is None or len(df) < 50:
            continue
            
        df = calculate_indicators(df)
        latest = df.iloc[-1]
        prev = df.iloc[-2]
        
        # Calculate ATR
        atr = calculate_atr(df)
        atr_pips = atr / get_pip_size(symbol)
        
        if atr_pips < params.get('min_volatility_pips', 5):
            continue
        
        # Check ADX strength using config params
        min_adx = params.get('min_adx_strength', 25)
        max_adx = params.get('max_adx_strength', 60)
        if not (min_adx <= latest['adx'] <= max_adx):
            continue
        
        # Multi-timeframe confirmation
        higher_timeframes = get_higher_timeframes(global_timeframe)
        aligned_timeframes = 0
        min_timeframes = params.get('min_timeframes', 2)
        
        for tf in higher_timeframes[:min_timeframes-1]:
            if tf in analyses:
                higher_analysis = analyses[tf]
                if primary_analysis['ema_direction'] == higher_analysis['ema_direction']:
                    aligned_timeframes += 1
        
        if aligned_timeframes < 1:
            continue
        
        # Current price relative to EMA using config
        ema_buffer = params.get('ema_buffer_pct', 0.005)
        close_to_ema = abs(latest['close'] - latest['ema20']) / latest['ema20'] < ema_buffer
        
        # Check each direction with intelligence overlay
        for direction in ['long', 'short']:
            if trade_manager.has_position(symbol, direction):
                continue
            
            # Signal validation using config params
            signal_valid = False
            ta_strength = 50
            
            rsi_oversold = params.get('rsi_oversold', 30)
            rsi_overbought = params.get('rsi_overbought', 70)
            
            if direction == 'long':
                bullish_trend = primary_analysis['ema_direction'] == 'Up'
                rsi_condition = (prev['rsi'] < rsi_oversold and latest['rsi'] > rsi_oversold)
                price_action = latest['close'] > latest['open']
                signal_valid = bullish_trend and close_to_ema and (rsi_condition or price_action)
                
                ta_strength = 30
                if bullish_trend: ta_strength += 30
                if rsi_condition: ta_strength += 20
                if price_action: ta_strength += 20
                
            else:  # short
                bearish_trend = primary_analysis['ema_direction'] == 'Down'
                rsi_condition = (prev['rsi'] > rsi_overbought and latest['rsi'] < rsi_overbought)
                price_action = latest['close'] < latest['open']
                signal_valid = bearish_trend and close_to_ema and (rsi_condition or price_action)
                
                ta_strength = 30
                if bearish_trend: ta_strength += 30
                if rsi_condition: ta_strength += 20
                if price_action: ta_strength += 20
            
            if signal_valid:
                # Enhanced decision check
                can_trade_smart, confidence, reasons = trade_manager.can_trade_enhanced(
                    symbol, direction, ta_strength
                )
                
                if not can_trade_smart:
                    logger.info(f"🧠 {symbol} {direction} blocked by intelligence: {'; '.join(reasons)}")
                    continue
                
                # Calculate entry, SL, TP using config params
                entry_price = latest['close']
                pip_size = get_pip_size(symbol)
                atr_multiplier = params.get('atr_multiplier', 1.5)
                
                if direction == 'long':
                    sl = min(df['low'].iloc[-3:]) - atr * atr_multiplier
                    rr_ratio = params.get('risk_reward_ratio_long', 1.3)
                    tp_distance = abs(entry_price - sl) * rr_ratio
                    tp = entry_price + tp_distance
                else:
                    sl = max(df['high'].iloc[-3:]) + atr * atr_multiplier
                    rr_ratio = params.get('risk_reward_ratio_short', 1.3)
                    tp_distance = abs(sl - entry_price) * rr_ratio
                    tp = entry_price - tp_distance
                
                # Validate distances
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
                        
                        # Enhanced data
                        'enhanced_validation': True,
                        'ta_strength': ta_strength,
                        'final_confidence': confidence,
                        'intelligence_reasons': reasons,
                        'sl_distance': abs(entry_price - sl)
                    })
                    
                    logger.info(f"🎯 Enhanced signal: {symbol} {direction} (TA: {ta_strength}%, Final: {confidence:.1f}%)")
    
    return signals

# ===== ENHANCED MARTINGALE EXECUTION =====
def execute_martingale_trade_enhanced(opportunity, trade_manager, config_manager):
    """Enhanced martingale execution with unified config"""
    
    batch = opportunity['batch']
    symbol = opportunity['symbol']
    direction = opportunity['direction']
    layer = opportunity['layer']
    
    # Get bypass layer from config
    bypass_layer = config_manager.get('martingale_config.intelligence_bypass_layer', 3)
    
    # Bypass intelligence checks for deep layers
    if layer >= bypass_layer:
        logger.info(f"🔄 Layer {layer} - Protecting existing investment, bypassing intelligence checks")
        bypass_intelligence = True
    else:
        bypass_intelligence = False
    
    # Enhanced check for new layers
    if not bypass_intelligence and config_manager.get('master_switches.enhanced_features_enabled'):
        can_trade_smart, confidence, reasons = trade_manager.can_trade_enhanced(
            symbol, direction, ta_strength=80
        )
        
        if not can_trade_smart:
            logger.info(f"🧠 Martingale {symbol} {direction} Layer {layer} blocked: {'; '.join(reasons)}")
            return False
    
    # Use proven martingale execution
    try:
        from trading_engine_backup import execute_martingale_trade, get_pip_size
        
        martingale_signal = {
            'symbol': symbol,
            'direction': direction,
            'entry_price': opportunity['entry_price'],
            'sl': None,
            'tp': None,
            'sl_distance_pips': batch.initial_sl_distance / get_pip_size(symbol),
            'risk_profile': config_manager.get_pair_risk_profiles().get(symbol, "High"),
            'is_initial': False,
            'layer': layer,
            'sl_distance': batch.initial_sl_distance
        }
        
        return execute_martingale_trade(martingale_signal, trade_manager.original_manager)
        
    except ImportError:
        logger.error("❌ Could not import martingale execution functions")
        return False

# ===== ENHANCED TRADE EXECUTION =====
def execute_enhanced_trade(signal, trade_manager, config_manager):
    """Enhanced trade execution with unified config"""
    
    symbol = signal['symbol']
    direction = signal['direction']
    confidence = signal.get('final_confidence', 100)
    
    # Enhanced risk calculation using config
    risk_profile = signal['risk_profile']
    param_sets = config_manager.get_param_sets()
    params = param_sets.get(risk_profile, param_sets.get("Medium", {}))
    base_risk_pct = params.get('risk_per_trade_pct', 0.1)
    
    # Apply enhanced position sizing
    enhanced_risk_amount = trade_manager.calculate_enhanced_risk_amount(
        symbol, base_risk_pct, confidence
    )
    
    # Update signal with enhanced data
    signal['enhanced_risk_amount'] = enhanced_risk_amount
    signal['confidence_level'] = confidence
    
    # Use proven execution logic
    try:
        from trading_engine_backup import execute_trade
        return execute_trade(signal, trade_manager.original_manager)
    except ImportError:
        logger.error("❌ Could not import trade execution function")
        return False

# ===== ENHANCED SYSTEM STATUS =====
class EnhancedSystemStatus:
    """Provides comprehensive system status using unified config"""
    
    def __init__(self, config_manager, trade_manager, data_manager):
        self.config = config_manager
        self.trade_manager = trade_manager
        self.data_manager = data_manager
        self.intelligence_config = config_manager.get_intelligence_config()
    
    def get_comprehensive_status(self):
        """Get complete system status"""
        try:
            account_info = mt5.account_info()
            if not account_info:
                return {"error": "Cannot get account info"}
            
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
                    'enabled': self.intelligence_config['ENHANCED_FEATURES_ENABLED'],
                    'features': {},
                    'data_status': {},
                    'risk_level': self.intelligence_config['MASTER_RISK_LEVEL']
                },
                'configuration': {
                    'config_file': str(self.config.config_file),
                    'ta_weight': self.intelligence_config['TA_WEIGHT'],
                    'data_weight': self.intelligence_config['DATA_WEIGHT']
                }
            }
            
            # Intelligence feature status
            if self.intelligence_config['ENHANCED_FEATURES_ENABLED']:
                status['intelligence']['features'] = {
                    'sentiment_blocking': self.intelligence_config['USE_SENTIMENT_BLOCKING'],
                    'correlation_risk': self.intelligence_config['USE_CORRELATION_RISK'],
                    'economic_timing': self.intelligence_config['USE_ECONOMIC_TIMING'],
                    'dynamic_sizing': self.intelligence_config['USE_DYNAMIC_POSITION_SIZING']
                }
                
                # Data source status from data manager
                data_health = self.data_manager.get_system_health()
                status['intelligence']['data_status'] = data_health.get('data_freshness', {})
            
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
                
                # Data freshness summary
                data_status = intelligence.get('data_status', {})
                fresh_sources = [name for name, data in data_status.items() if data.get('fresh')]
                stale_sources = [name for name, data in data_status.items() if not data.get('fresh')]
                
                if fresh_sources:
                    logger.info(f"   ✅ Fresh data: {', '.join(fresh_sources)}")
                if stale_sources:
                    logger.info(f"   ⚠️ Stale data: {', '.join(stale_sources)}")
            else:
                logger.info("🧠 Intelligence: DISABLED - Pure TA mode")
            
        except Exception as e:
            logger.error(f"Error logging status: {e}")

# ===== MAIN ENHANCED ROBOT FUNCTION =====
def run_enhanced_robot():
    """Main enhanced robot function using unified configuration"""
    
    # Initialize unified configuration
    config_manager = UnifiedConfigManager()
    
    # Setup logging from config
    global logger
    logger = setup_logging(config_manager)
    
    logger.info("="*80)
    logger.info("🚀 ENHANCED TRADING ROBOT - UNIFIED VERSION")
    logger.info("="*80)
    logger.info(f"Config File: {config_manager.config_file}")
    logger.info(f"Primary Timeframe: {config_manager.get('trading_config.global_timeframe')}")
    logger.info(f"Pairs: {len(config_manager.get_pairs_list())}")
    logger.info(f"Enhanced Features: {config_manager.get('master_switches.enhanced_features_enabled')}")
    logger.info(f"Master Risk Level: {config_manager.get('risk_management.master_risk_level')}%")
    logger.info(f"TA Weight: {config_manager.get('risk_management.ta_weight')}%, Data Weight: {config_manager.get('risk_management.data_weight')}%")
    logger.info("="*80)
    
    # Validate configuration
    validation = config_manager.validate_config()
    if validation['errors']:
        logger.error("❌ Configuration validation failed:")
        for error in validation['errors']:
            logger.error(f"   {error}")
        return
    
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
    
    # Validate account matches config
    expected_account = config_manager.get('system_info.account_number')
    if expected_account and account_info.login != expected_account:
        logger.error(f"Account mismatch: Expected {expected_account}, got {account_info.login}")
        mt5.shutdown()
        return
    
    logger.info(f"Connected to account: {account_info.login}")
    logger.info(f"Balance: ${account_info.balance:.2f}")
    
    # Initialize data manager if data collection enable#!/usr/bin/env python3
# ===== ENHANCED TRADING ENGINE - UNIFIED VERSION =====
# Uses unified configuration manager and data manager
# NO hardcoded configurations - everything comes from unified config

import MetaTrader5 as mt5
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import warnings
import time
import logging
import json
import os
import sys
from pathlib import Path

# Suppress warnings
warnings.filterwarnings("ignore")

# Add paths for imports
sys.path.insert(0, str(Path(__file__).parent))
sys.path.insert(0, str(Path(__file__).parent / 'core'))

# Import unified managers
from unified_config_manager import UnifiedConfigManager
from unified_data_manager import UnifiedDataManager

# ===== LOGGING SETUP =====
def setup_logging(config_manager):
    """Setup logging based on configuration"""
    log_level = getattr(logging, config_manager.get('logging.level', 'INFO'))
    log_file = config_manager.get('file_paths.trading_log', 'enhanced_trading.log')
    
    logging.basicConfig(
        level=log_level,
        format='%(asctime)s - %(levelname)s - %(message)s',
        handlers=[
            logging.FileHandler(log_file, encoding='utf-8'),
            logging.StreamHandler() if config_manager.get('logging.console_output', True) else logging.NullHandler()
        ]
    )
    return logging.getLogger(__name__)

# ===== ENHANCED DATA INTEGRATION MANAGER =====
class EnhancedDataManager:
    """Manages all external data sources with unified configuration"""
    
    def __init__(self, config_manager, data_manager):
        self.config = config_manager
        self.data_manager = data_manager
        self.intelligence_config = config_manager.get_intelligence_config()
        self.risk_thresholds = config_manager.get_risk_thresholds()
    
    def get_sentiment_data(self):
        """Get sentiment data with fallback"""
        if not self.intelligence_config['USE_SENTIMENT_BLOCKING']:
            return self._get_fallback_sentiment()
        
        try:
            sentiment_data = self.data_manager.get_sentiment_data()
            
            # Check data freshness using config
            freshness_limit = self.config.get('intelligence.sentiment_blocking.freshness_limit_minutes', 60)
            if not self.data_manager.is_data_fresh('sentiment', freshness_limit):
                logger.warning(f"⚠️ Sentiment data stale, using fallback")
                return self._get_fallback_sentiment()
            
            logger.debug(f"✅ Fresh sentiment data loaded")
            return sentiment_data
            
        except Exception as e:
            logger.error(f"❌ Error loading sentiment data: {e}")
            return self._get_fallback_sentiment()
    
    def get_correlation_data(self):
        """Get correlation data with fallback"""
        if not self.intelligence_config['USE_CORRELATION_RISK']:
            return {'matrix': {}, 'warnings': []}
        
        try:
            correlation_data = self.data_manager.get_correlation_data()
            
            # Check data freshness using config
            freshness_limit = self.config.get('intelligence.correlation_risk.freshness_limit_minutes', 60)
            if not self.data_manager.is_data_fresh('correlation', freshness_limit):
                logger.warning(f"⚠️ Correlation data stale")
                return {'matrix': {}, 'warnings': []}
            
            return correlation_data
            
        except Exception as e:
            logger.error(f"❌ Error loading correlation data: {e}")
            return {'matrix': {}, 'warnings': []}
    
    def get_economic_events(self, hours_ahead=24):
        """Get upcoming economic events"""
        if not self.intelligence_config['USE_ECONOMIC_TIMING']:
            return []
        
        try:
            # Check data freshness using config
            freshness_limit = self.config.get('intelligence.economic_timing.freshness_limit_minutes', 120)
            if not self.data_manager.is_data_fresh('economic_calendar', freshness_limit):
                logger.warning(f"⚠️ Economic calendar data stale")
                return []
            
            return self.data_manager.get_economic_events(hours_ahead)
            
        except Exception as e:
            logger.error(f"❌ Error loading economic events: {e}")
            return []
    
    def _get_fallback_sentiment(self):
        """Fallback sentiment - allow all directions"""
        fallback = {}
        pairs = self.config.get_pairs_list()
        for pair in pairs:
            fallback[pair] = {
                'allowed_directions': ['long', 'short'],
                'blocked_directions': [],
                'sentiment': {'short': 50, 'long': 50},
                'signal_strength': 'Fallback'
            }
        return fallback

# ===== ENHANCED DECISION ENGINE =====
class EnhancedDecisionEngine:
    """Makes intelligent trading decisions using unified configuration"""
    
    def __init__(self, config_manager, data_manager):
        self.config = config_manager
        self.data_manager = data_manager
        self.intelligence_config = config_manager.get_intelligence_config()
        self.risk_thresholds = config_manager.get_risk_thresholds()
        self.enhanced_data_manager = EnhancedDataManager(config_manager, data_manager)
        self.decision_log = []
    
    def can_trade_direction(self, symbol, direction, ta_signal_strength=100):
        """Enhanced decision making with unified configuration"""
        
        if not self.intelligence_config['ENHANCED_FEATURES_ENABLED']:
            return True, ta_signal_strength, ["Pure TA mode"]
        
        try:
            reasons = []
            blocking_factors = []
            risk_factors = []
            
            # Get weights from config
            ta_weight = self.intelligence_config['TA_WEIGHT'] / 100
            data_weight = self.intelligence_config['DATA_WEIGHT'] / 100
            
            base_confidence = ta_signal_strength * ta_weight
            
            # Check 1: Sentiment Analysis
            sentiment_adjustment = 0
            if self.intelligence_config['USE_SENTIMENT_BLOCKING']:
                sentiment_check = self._check_sentiment(symbol, direction)
                if not sentiment_check['allowed']:
                    blocking_factors.append(f"Sentiment: {sentiment_check['reason']}")
                else:
                    sentiment_adjustment = sentiment_check.get('confidence_boost', 0)
                    reasons.append(f"Sentiment: OK ({sentiment_check['reason']})")
            
            # Check 2: Correlation Risk
            correlation_adjustment = 0
            if self.intelligence_config['USE_CORRELATION_RISK']:
                correlation_check = self._check_correlation_risk(symbol)
                if correlation_check['high_risk']:
                    risk_factors.append(f"Correlation: {correlation_check['reason']}")
                    correlation_adjustment = -10
                else:
                    reasons.append(f"Correlation: OK")
            
            # Check 3: Economic Events
            economic_adjustment = 0
            if self.intelligence_config['USE_ECONOMIC_TIMING']:
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
            
            # Apply master risk level from config
            final_confidence *= (self.intelligence_config['MASTER_RISK_LEVEL'] / 100)
            
            # Cap confidence
            final_confidence = max(0, min(100, final_confidence))
            
            # Decision logic using config
            min_confidence = self.config.get('risk_management.min_confidence_to_trade', 30)
            can_trade = final_confidence >= min_confidence
            
            if risk_factors:
                reasons.extend(risk_factors)
            
            self._log_decision(symbol, direction, can_trade, final_confidence, reasons)
            
            return can_trade, final_confidence, reasons
            
        except Exception as e:
            logger.error(f"❌ Error in enhanced decision for {symbol} {direction}: {e}")
            return True, ta_signal_strength, [f"Error in decision engine: {e}"]
    
    def _check_sentiment(self, symbol, direction):
        """Check sentiment blocking using unified config"""
        try:
            sentiment_data = self.enhanced_data_manager.get_sentiment_data()
            
            # Normalize symbol name
            symbol_variants = [symbol, symbol.upper()]
            if symbol == 'XAUUSD':
                symbol_variants.extend(['GOLD', 'XAUUSD'])
            elif symbol == 'US500':
                symbol_variants.extend(['