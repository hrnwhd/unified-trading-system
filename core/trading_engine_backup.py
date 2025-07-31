# ===== FIXED BM TRADING ROBOT - BASED ON WORKING VERSION =====
# Simplified version that maintains signal generation capabilities
# Removed complex modular structure causing heartbeat issues

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

# Suppress warnings
warnings.filterwarnings("ignore")

# ===== CONFIGURATION =====
GLOBAL_TIMEFRAME = mt5.TIMEFRAME_M5  # Primary timeframe
ACCOUNT_NUMBER = 42903786
MAGIC_NUMBER = 50515253

# Martingale Configuration
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
        logging.FileHandler('bm_robot.log', encoding='utf-8'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

# ===== TRADING PAIRS AND SCHEDULES =====
PAIRS = ['AUDUSD', 'USDCAD', 'XAUUSD', 'EURUSD', 'GBPUSD', 
         'AUDCAD',  'USDCHF', 'GBPCAD', 'AUDNZD', 'NZDCAD','US500','BTCUSD']

PAIR_TRADING_DAYS = {pair: [0, 1, 2, 3, 4] for pair in PAIRS}  # Monday to Friday
PAIR_TRADING_HOURS = {pair: {'start': 0, 'end': 23} for pair in PAIRS}  # 24/7

# Risk Profiles - SIMPLIFIED
PAIR_RISK_PROFILES = {
    'AUDUSD': "Medium", 'USDCAD': "Medium", 'US500': "Medium", 'XAUUSD': "Medium",
    'BTCUSD': "Medium", 'EURUSD': "Medium", 'GBPUSD': "Medium", 'AUDCAD': "Medium",
    'USDJPY': "Low", 'USDCHF': "Low", 'GBPCAD': "Medium", 'AUDNZD': "Medium",
    'NZDCAD': "Medium"
}

# SIMPLIFIED PARAMETER SETS
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

# ===== UTILITY FUNCTIONS =====
def get_pip_size(symbol):
    """Get pip size for different symbol types"""
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

def get_pip_value(symbol, lot_size):
    """Calculate pip value for position sizing - FIXED for CAD pairs"""
    symbol_info = mt5.symbol_info(symbol)
    if symbol_info is None:
        logger.error(f"Could not get symbol info for {symbol}")
        return 0
    
    point = symbol_info.point
    contract_size = symbol_info.trade_contract_size
    current_bid = symbol_info.bid
    
    logger.debug(f"{symbol} symbol info: point={point}, contract_size={contract_size}, bid={current_bid}")
    
    # ✅ FIXED CRYPTO CALCULATIONS
    if symbol in ['BTCUSD', 'ETHUSD']:
        pip_value = lot_size * 1.0
        logger.debug(f"{symbol} crypto pip value: ${pip_value:.2f} per lot")
        return pip_value
    
    # ✅ FIXED INDICES CALCULATIONS
    if symbol in ['US500', 'NAS100', 'SPX500', 'USTEC']:
        pip_value = lot_size * 10.0  # $10 per pip per lot
        logger.debug(f"{symbol} index pip value: ${pip_value:.2f} per lot")
        return pip_value
    
    if symbol in ['XAUUSD', 'GOLD']:
        pip_value = lot_size * 10.0
        logger.debug(f"{symbol} gold pip value: ${pip_value:.2f} per lot")
        return pip_value
    
    # Standard forex calculation
    if symbol.endswith('USD'):
        # USD as quote currency: 1 pip = $10 per standard lot
        pip_value = lot_size * 10.0
        logger.debug(f"{symbol} USD quote pip value: ${pip_value:.2f} per lot")
        return pip_value
    
    if symbol.startswith('USD'):
        # USD as base currency
        if current_bid == 0:
            logger.error(f"Error: Zero rate for {symbol}")
            return 0
        pip_value = (lot_size * 10.0) / current_bid
        logger.debug(f"{symbol} USD base pip value: ${pip_value:.2f} per lot")
        return pip_value
    
    # ✅ FIXED CAD PAIRS CALCULATION - Get proper USD conversion
    if symbol.endswith('CAD'):
        # For pairs like GBPCAD, AUDCAD, NZDCAD - need to convert CAD to USD
        try:
            # Get USDCAD rate for conversion
            usdcad_tick = mt5.symbol_info_tick('USDCAD')
            if usdcad_tick and usdcad_tick.bid > 0:
                # CAD pip value = (lot_size * 10 CAD) / USDCAD_rate
                cad_pip_value = lot_size * 10.0  # 10 CAD per pip per lot
                usd_pip_value = cad_pip_value / usdcad_tick.bid
                logger.debug(f"{symbol} CAD pair pip value: ${usd_pip_value:.2f} per lot (via USDCAD: {usdcad_tick.bid:.4f})")
                return usd_pip_value
            else:
                logger.warning(f"Could not get USDCAD rate for {symbol}, using approximation")
                # Fallback approximation (assuming USDCAD ≈ 1.35)
                pip_value = lot_size * 7.4  # 10 CAD / 1.35 ≈ 7.4 USD
                logger.debug(f"{symbol} CAD pair pip value (approx): ${pip_value:.2f} per lot")
                return pip_value
        except Exception as e:
            logger.error(f"Error calculating CAD pair pip value for {symbol}: {e}")
            # Conservative fallback
            pip_value = lot_size * 7.0
            return pip_value
    
    # ✅ FIXED OTHER CROSS PAIRS - Get proper conversion rates
    # For other cross pairs, try to get the appropriate USD conversion
    base_currency = symbol[:3]
    quote_currency = symbol[3:]
    
    try:
        # Try to find a USD pair for conversion
        usd_conversion_rate = 1.0
        
        if base_currency != 'USD' and quote_currency != 'USD':
            # Both currencies are not USD - need conversion
            # Try base currency to USD first
            base_usd_pair = f"{base_currency}USD"
            base_usd_tick = mt5.symbol_info_tick(base_usd_pair)
            
            if base_usd_tick:
                # Base currency can be converted directly
                usd_conversion_rate = base_usd_tick.bid
                pip_value = lot_size * 10.0 * usd_conversion_rate
                logger.debug(f"{symbol} cross pair pip value: ${pip_value:.2f} per lot (via {base_usd_pair}: {usd_conversion_rate:.4f})")
                return pip_value
            else:
                # Try USD to base currency
                usd_base_pair = f"USD{base_currency}"
                usd_base_tick = mt5.symbol_info_tick(usd_base_pair)
                
                if usd_base_tick and usd_base_tick.bid > 0:
                    usd_conversion_rate = 1.0 / usd_base_tick.bid
                    pip_value = lot_size * 10.0 * usd_conversion_rate
                    logger.debug(f"{symbol} cross pair pip value: ${pip_value:.2f} per lot (via {usd_base_pair}: {usd_base_tick.bid:.4f})")
                    return pip_value
        
        # If no conversion found, use conservative approximation
        pip_value = lot_size * 8.0  # Conservative estimate for cross pairs
        logger.debug(f"{symbol} cross pair pip value (fallback): ${pip_value:.2f} per lot")
        return pip_value
        
    except Exception as e:
        logger.error(f"Error calculating cross pair pip value for {symbol}: {e}")
        # Very conservative fallback
        pip_value = lot_size * 5.0
        return pip_value


def normalize_volume(symbol, volume):
    """Normalize volume to broker requirements with detailed logging"""
    symbol_info = mt5.symbol_info(symbol)
    if symbol_info is None:
        logger.error(f"Cannot get symbol info for {symbol}")
        return 0.01
    
    min_volume = symbol_info.volume_min
    max_volume = symbol_info.volume_max
    volume_step = symbol_info.volume_step
    
    logger.info(f"{symbol} MT5 volume constraints:")
    logger.info(f"  Min: {min_volume}, Max: {max_volume}, Step: {volume_step}")
    logger.info(f"  Input volume: {volume}")
    
    # Apply min/max constraints
    volume = max(min_volume, min(volume, max_volume))
    logger.info(f"  After min/max: {volume}")
    
    # Apply step constraints
    if volume_step > 0:
        volume = round(volume / volume_step) * volume_step
        logger.info(f"  After step rounding: {volume}")
    
    # Final validation
    if volume < min_volume:
        logger.warning(f"{symbol}: Volume {volume} below minimum {min_volume}, using minimum")
        volume = min_volume
    
    logger.info(f"  Final normalized volume: {volume}")
    return volume

def calculate_position_size(symbol, stop_loss_pips, risk_amount, is_martingale=False, base_volume=None, layer=1):
    """Calculate position size with enhanced validation and CAD pair fixes"""
    
    # ✅ SPECIAL HANDLING FOR CAD PAIRS
    cad_pairs_risk_reduction = {
        'GBPCAD': 0.6,  # Reduce risk to 60% for GBPCAD
        'AUDCAD': 0.7,  # Reduce risk to 70% for AUDCAD  
        'NZDCAD': 0.7,  # Reduce risk to 70% for NZDCAD
        'USDCAD': 0.8,  # Reduce risk to 80% for USDCAD
        'EURCAD': 0.7,  # Reduce risk to 70% for EURCAD
        'CADJPY': 0.7,  # Reduce risk to 70% for CADJPY
    }
    
    # Apply CAD pair risk reduction
    if symbol in cad_pairs_risk_reduction:
        risk_multiplier = cad_pairs_risk_reduction[symbol]
        adjusted_risk_amount = risk_amount * risk_multiplier
        logger.info(f"🍁 CAD pair {symbol}: Reducing risk from ${risk_amount:.2f} to ${adjusted_risk_amount:.2f} ({risk_multiplier*100:.0f}%)")
        risk_amount = adjusted_risk_amount
    
    # Symbol-specific settings
    symbol_settings = {
        'BTCUSD': {'min_vol': 0.01, 'max_vol': 1.0, 'step': 0.01},
        'ETHUSD': {'min_vol': 0.01, 'max_vol': 1.0, 'step': 0.01},
        'XRPUSD': {'min_vol': 0.01, 'max_vol': 1.0, 'step': 0.01},
        'XAUUSD': {'min_vol': 0.01, 'max_vol': 10.0, 'step': 0.01},
        'US500': {'min_vol': 0.01, 'max_vol': 1.0, 'step': 0.01},
        'NAS100': {'min_vol': 0.01, 'max_vol': 1.0, 'step': 0.01},
        # ✅ CAD PAIRS VOLUME LIMITS
        'GBPCAD': {'min_vol': 0.01, 'max_vol': 2.0, 'step': 0.01},
        'AUDCAD': {'min_vol': 0.01, 'max_vol': 2.0, 'step': 0.01},
        'NZDCAD': {'min_vol': 0.01, 'max_vol': 2.0, 'step': 0.01},
        'USDCAD': {'min_vol': 0.01, 'max_vol': 3.0, 'step': 0.01},
        'EURCAD': {'min_vol': 0.01, 'max_vol': 2.0, 'step': 0.01},
    }
    
    # Get symbol info from MT5
    symbol_info = mt5.symbol_info(symbol)
    if symbol_info:
        min_vol = symbol_info.volume_min
        max_vol = symbol_info.volume_max
        step = symbol_info.volume_step
    else:
        # Fallback to preset values
        settings = symbol_settings.get(symbol, {'min_vol': 0.01, 'max_vol': 10.0, 'step': 0.01})
        min_vol = settings['min_vol']
        max_vol = settings['max_vol']
        step = settings['step']
    
    logger.debug(f"{symbol} volume constraints: min={min_vol}, max={max_vol}, step={step}")
    
    if is_martingale and base_volume:
        position_size = base_volume * (MARTINGALE_MULTIPLIER ** (layer - 1))
        position_size = max(position_size, min_vol)
        position_size = min(position_size, max_vol)
        return normalize_volume(symbol, position_size)
    
    # Calculate base position size using FIXED pip value calculation
    pip_value = get_pip_value(symbol, 1.0)
    if pip_value <= 0:
        logger.warning(f"Invalid pip value for {symbol}: {pip_value}")
        return min_vol
    
    # Additional risk reduction for volatile pairs
    risk_multiplier = 1.0
    if symbol in ['BTCUSD', 'ETHUSD', 'XAUUSD']:
        risk_multiplier = 0.5  # Half risk for crypto and gold
    elif symbol in ['US500', 'NAS100', 'SPX500']:
        risk_multiplier = 0.2  # Much lower risk for indices
    elif symbol in cad_pairs_risk_reduction:
        # Already applied above, but ensure it's not double-applied
        pass
    else:
        risk_multiplier = 1.0
    
    # Don't double-apply CAD pair risk reduction
    if symbol not in cad_pairs_risk_reduction:
        adjusted_risk = risk_amount * risk_multiplier
    else:
        adjusted_risk = risk_amount  # Already adjusted above
    
    position_size = adjusted_risk / (stop_loss_pips * pip_value)
    
    # Apply constraints
    position_size = max(position_size, min_vol)
    position_size = min(position_size, max_vol)
    
    # ✅ ADDITIONAL SAFETY CHECK FOR CAD PAIRS
    if symbol.endswith('CAD') and position_size > 0.05:
        logger.warning(f"🍁 {symbol}: Large position size {position_size:.3f} detected, capping at 0.05")
        position_size = min(position_size, 0.05)
    
    # Cap position size to reasonable levels based on account size
    account_info = mt5.account_info()
    if account_info:
        max_position_value = account_info.balance * 0.05  # Max 5% of balance per trade for CAD pairs
        current_price = mt5.symbol_info_tick(symbol).bid if mt5.symbol_info_tick(symbol) else 1.0
        max_size_by_value = max_position_value / current_price
        position_size = min(position_size, max_size_by_value)
    
    normalized_size = normalize_volume(symbol, position_size)
    
    logger.info(f"Position sizing for {symbol}:")
    logger.info(f"  Risk amount: ${adjusted_risk:.2f}")
    logger.info(f"  Stop loss pips: {stop_loss_pips:.1f}")
    logger.info(f"  Pip value: ${pip_value:.2f}")
    logger.info(f"  Calculated size: {position_size:.3f}")
    logger.info(f"  Final size: {normalized_size:.3f}")
    
    return normalized_size

# ===== UPDATE YOUR PAIR RISK PROFILES FOR CAD PAIRS =====

# Add this to your existing configuration
PAIR_RISK_PROFILES = {
    'AUDUSD': "Medium", 'USDCAD': "Low", 'US500': "High", 'XAUUSD': "High",  # ✅ Changed USDCAD to Low
    'BTCUSD': "Medium", 'EURUSD': "Medium", 'GBPUSD': "Medium", 
    'AUDCAD': "Low",    # ✅ Changed to Low risk
    'USDJPY': "Low", 'USDCHF': "High", 
    'GBPCAD': "Low",    # ✅ Changed to Low risk
    'AUDNZD': "Medium",
    'NZDCAD': "Low"     # ✅ Changed to Low risk
}

# ===== DIAGNOSTIC FUNCTION TO CHECK CAD PAIR CALCULATIONS =====

# ===== TECHNICAL ANALYSIS =====
def get_historical_data(symbol, timeframe, num_bars=500):
    """Get historical data"""
    if not mt5.symbol_select(symbol, True):
        return None
    
    rates = mt5.copy_rates_from_pos(symbol, timeframe, 0, num_bars)
    if rates is None:
        return None
    
    df = pd.DataFrame(rates)
    df['time'] = pd.to_datetime(df['time'], unit='s')
    df.set_index('time', inplace=True)
    return df

def calculate_indicators(df):
    """Calculate technical indicators"""
    if df is None or df.empty:
        return df
    
    # EMA
    df['ema20'] = df['close'].ewm(span=20, adjust=False).mean()
    
    # ADX
    high, low, close = df['high'], df['low'], df['close']
    df['plus_dm'] = np.where((high.diff() > -low.diff()) & (high.diff() > 0), high.diff(), 0)
    df['minus_dm'] = np.where((-low.diff() > high.diff()) & (-low.diff() > 0), -low.diff(), 0)
    df['tr'] = pd.concat([high-low, (high-close.shift(1)).abs(), (low-close.shift(1)).abs()], axis=1).max(axis=1)
    
    alpha = 1/14
    plus_dm_smooth = df['plus_dm'].ewm(alpha=alpha, adjust=False).mean()
    minus_dm_smooth = df['minus_dm'].ewm(alpha=alpha, adjust=False).mean()
    tr_smooth = df['tr'].ewm(alpha=alpha, adjust=False).mean()
    
    df['plus_di'] = 100 * (plus_dm_smooth / tr_smooth)
    df['minus_di'] = 100 * (minus_dm_smooth / tr_smooth)
    df['dx'] = 100 * abs(df['plus_di'] - df['minus_di']) / (df['plus_di'] + df['minus_di']).replace(0, 0.001)
    df['adx'] = df['dx'].ewm(alpha=alpha, adjust=False).mean()
    
    # RSI
    delta = close.diff()
    gain = delta.where(delta > 0, 0)
    loss = -delta.where(delta < 0, 0)
    avg_gain = gain.rolling(14).mean()
    avg_loss = loss.rolling(14).mean()
    rs = avg_gain / avg_loss
    df['rsi'] = 100 - (100 / (1 + rs))
    
    return df

def calculate_atr(df, period=14):
    """Calculate ATR"""
    high_low = df['high'] - df['low']
    high_close = (df['high'] - df['close'].shift()).abs()
    low_close = (df['low'] - df['close'].shift()).abs()
    true_range = pd.concat([high_low, high_close, low_close], axis=1).max(axis=1)
    return true_range.rolling(period).mean().iloc[-1]

# ===== ENHANCED MARTINGALE BATCH CLASS =====
class MartingaleBatch:
    def __init__(self, symbol, direction, initial_sl_distance, entry_price):
        self.symbol = symbol
        self.direction = direction
        self.initial_sl_distance = initial_sl_distance  # Fixed distance for layer triggers
        self.initial_entry_price = entry_price
        self.trades = []
        self.current_layer = 0
        self.total_volume = 0
        self.total_invested = 0
        self.breakeven_price = 0
        self.created_time = datetime.now()
        self.last_layer_time = datetime.now()
        self.batch_id = None
        
    def add_trade(self, trade):
        """Add trade to batch and recalculate batch TP"""
        self.current_layer += 1
        trade['layer'] = self.current_layer
        trade['batch_id'] = self.batch_id
        
        # Enhanced trade labeling for clear identification
        batch_prefix = f"BM{self.batch_id:02d}"
        layer_suffix = f"L{self.current_layer:02d}"
        direction_code = "B" if self.direction == 'long' else "S"
        
        trade['enhanced_comment'] = f"{batch_prefix}_{self.symbol[:6]}_{direction_code}{layer_suffix}"
        
        logger.info(f"Adding {trade['enhanced_comment']} to {self.symbol} {self.direction} batch")
        
        self.trades.append(trade)
        self.total_volume += trade['volume']
        self.total_invested += trade['volume'] * trade['entry_price']
        self.last_layer_time = datetime.now()
        
        # Recalculate breakeven price
        if self.total_volume > 0:
            self.breakeven_price = self.total_invested / self.total_volume
            
    def get_next_trigger_price(self):
        """Calculate next price level to trigger new layer"""
        distance_for_next_layer = self.initial_sl_distance * self.current_layer
        
        if self.direction == 'long':
            return self.initial_entry_price - distance_for_next_layer
        else:
            return self.initial_entry_price + distance_for_next_layer
    
    def calculate_smart_batch_tp(self, base_profit_pips=15):
        """Smart TP calculation that gets easier to hit as layers increase"""
        if not self.trades or self.total_volume == 0:
            return None
            
        pip_size = get_pip_size(self.symbol)
        
        # Smart TP calculation based on layer psychology:
        # Layer 1: Need more profit to justify risk
        # Layer 2-3: Moderate profit acceptable  
        # Layer 4+: Just want to get out with small profit
        
        if self.current_layer == 1:
            # Layer 1: Full profit target
            profit_pips = base_profit_pips
        elif self.current_layer == 2:
            # Layer 2: 75% of original target
            profit_pips = base_profit_pips * 0.75
        elif self.current_layer == 3:
            # Layer 3: 50% of original target  
            profit_pips = base_profit_pips * 0.5
        elif self.current_layer <= 5:
            # Layer 4-5: 25% of original target
            profit_pips = base_profit_pips * 0.25
        else:
            # Layer 6+: Just breakeven + small buffer (survival mode)
            profit_pips = max(5, base_profit_pips * 0.15)
        
        # Minimum safety: Never less than 3 pips profit
        profit_pips = max(profit_pips, 3)
        
        # Additional smart adjustments based on market volatility
        if self.symbol in ['BTCUSD', 'ETHUSD']:
            profit_pips *= 2  # Crypto needs more room
        elif self.symbol in ['XAUUSD']:
            profit_pips *= 1.5  # Gold needs more room
        elif self.symbol in ['US500']:
            profit_pips *= 1.2  # Indices need slightly more
        
        logger.info(f"Smart TP Calculation for {self.symbol} Layer {self.current_layer}:")
        logger.info(f"  Breakeven: {self.breakeven_price:.5f}")
        logger.info(f"  Profit pips: {profit_pips:.1f}")
        logger.info(f"  Strategy: {'Aggressive' if self.current_layer <= 2 else 'Conservative' if self.current_layer <= 5 else 'Survival'}")
        
        # Calculate TP from breakeven
        if self.direction == 'long':
            return self.breakeven_price + (profit_pips * pip_size)
        else:
            return self.breakeven_price - (profit_pips * pip_size)
    
    def calculate_adaptive_batch_tp(self, market_volatility_pips=None):
        """Adaptive TP that considers market conditions and urgency"""
        if not self.trades or self.total_volume == 0:
            return None
            
        pip_size = get_pip_size(self.symbol)
        
        # Base profit calculation using "urgency factor"
        # The more layers, the more urgent to exit = smaller TP
        urgency_factor = min(self.current_layer, 8)  # Cap at layer 8
        
        # Urgency scale: 1=relaxed, 8=desperate
        urgency_multipliers = {
            1: 1.0,    # 100% - Take full profit
            2: 0.8,    # 80% - Still confident  
            3: 0.6,    # 60% - Getting concerned
            4: 0.4,    # 40% - Want out soon
            5: 0.25,   # 25% - Getting desperate
            6: 0.15,   # 15% - Just want out
            7: 0.1,    # 10% - Survival mode
            8: 0.05    # 5% - Emergency exit
        }
        
        urgency_mult = urgency_multipliers[urgency_factor]
        
        # Symbol-specific base targets (considering typical volatility)
        base_targets = {
            'BTCUSD': 50,   # Crypto: High volatility
            'ETHUSD': 40,
            'XRPUSD': 30,
            'XAUUSD': 25,   # Gold: Medium-high volatility
            'US500': 20,    # Indices: Medium volatility
            'EURUSD': 15,   # Major forex: Lower volatility
            'GBPUSD': 15,
            'AUDUSD': 12,
            'USDCAD': 12,
            'USDJPY': 12,
            'GBPCAD': 18,   # Cross pairs: Slightly higher
            'AUDCAD': 15,
            'AUDNZD': 10,
            'NZDCAD': 12
        }
        
        base_profit_pips = base_targets.get(self.symbol, 15)
        final_profit_pips = base_profit_pips * urgency_mult
        
        # Absolute minimum: 2 pips profit (except emergency exit)
        if urgency_factor < 8:
            final_profit_pips = max(final_profit_pips, 2)
        else:
            final_profit_pips = max(final_profit_pips, 1)  # Emergency: 1 pip is fine
        
        logger.info(f"Adaptive TP for {self.symbol} Layer {self.current_layer}:")
        logger.info(f"  Breakeven: {self.breakeven_price:.5f}")
        logger.info(f"  Base target: {base_profit_pips} pips")
        logger.info(f"  Urgency factor: {urgency_factor} (multiplier: {urgency_mult:.2f})")
        logger.info(f"  Final target: {final_profit_pips:.1f} pips")
        
        # Calculate TP from breakeven
        if self.direction == 'long':
            return self.breakeven_price + (final_profit_pips * pip_size)
        else:
            return self.breakeven_price - (final_profit_pips * pip_size)
    
    def should_add_layer(self, current_price, fast_move_threshold_seconds=30):  # Reduced from 60 to 30
        """More aggressive layer addition with reduced wait time"""
        # Check maximum layers
        if self.current_layer >= MAX_MARTINGALE_LAYERS:
            return False
            
        # Check if price has reached trigger level
        trigger_price = self.get_next_trigger_price()
        price_triggered = False
        
        if self.direction == 'long':
            price_triggered = current_price <= trigger_price
        else:
            price_triggered = current_price >= trigger_price
            
        if not price_triggered:
            return False
            
        # Reduced fast move protection - more aggressive
        time_since_last_layer = (datetime.now() - self.last_layer_time).total_seconds()
        if time_since_last_layer < fast_move_threshold_seconds:
            logger.info(f"Fast move protection: {self.symbol} - {time_since_last_layer:.0f}s < {fast_move_threshold_seconds}s")
            return False
            
        return True
    
    def update_all_tps_with_retry(self, new_tp, max_attempts=3):
        """Update TP for all trades with retry mechanism"""
        logger.info(f"🔄 Updating ALL TPs in {self.symbol} batch to {new_tp:.5f}")
        
        success_count = 0
        total_trades = len(self.trades)
        
        for attempt in range(max_attempts):
            remaining_trades = [trade for trade in self.trades if trade.get('tp') != new_tp]
            
            if not remaining_trades:
                logger.info(f"✅ All {total_trades} trades already have correct TP")
                return True
            
            logger.info(f"Attempt {attempt + 1}: Updating {len(remaining_trades)} remaining trades")
            
            for trade in remaining_trades:
                if self.update_trade_tp_with_retry(trade, new_tp):
                    success_count += 1
                    
            # Check success rate
            if success_count >= total_trades * 0.8:  # 80% success rate acceptable
                logger.info(f"✅ TP Update successful: {success_count}/{total_trades} trades updated")
                return True
            
            time.sleep(1)  # Wait between attempts
        
        logger.warning(f"⚠️ TP Update partial success: {success_count}/{total_trades} trades updated")
        return success_count > 0
    
    def update_trade_tp_with_retry(self, trade, new_tp, max_attempts=2):
        """Update TP for individual trade with retry"""
        for attempt in range(max_attempts):
            try:
                positions = mt5.positions_get(symbol=self.symbol)
                if not positions:
                    logger.warning(f"No positions found for {self.symbol}")
                    return False
                    
                # Find matching position
                target_position = None
                order_id = trade.get('order_id')
                
                for pos in positions:
                    if pos.magic == MAGIC_NUMBER and pos.ticket == order_id:
                        target_position = pos
                        break
                        
                if not target_position:
                    logger.warning(f"Position {order_id} not found for TP update")
                    return False
                
                # Skip if TP already correct
                if abs(target_position.tp - new_tp) < get_pip_size(self.symbol):
                    trade['tp'] = new_tp
                    return True
                    
                # Update TP
                request = {
                    "action": mt5.TRADE_ACTION_SLTP,
                    "symbol": self.symbol,
                    "position": target_position.ticket,
                    "sl": target_position.sl,
                    "tp": float(new_tp),
                }
                
                result = mt5.order_send(request)
                if result and result.retcode == mt5.TRADE_RETCODE_DONE:
                    trade['tp'] = new_tp
                    logger.debug(f"✅ Updated TP for {order_id}: {new_tp:.5f}")
                    return True
                else:
                    error_code = result.retcode if result else "No result"
                    logger.warning(f"TP update attempt {attempt + 1} failed for {order_id}: {error_code}")
                    
            except Exception as e:
                logger.error(f"Error updating TP attempt {attempt + 1}: {e}")
                
            time.sleep(0.5)  # Wait between attempts
                
        return False
# ===== ENHANCED PERSISTENCE SYSTEM =====
class BotPersistence:
    def __init__(self, data_file="BM_bot_state.json"):
        self.data_file = data_file
        self.backup_file = data_file + ".backup"
        
    def save_bot_state(self, trade_manager):
        """Save complete bot state after every trade execution"""
        try:
            # Create backup of current state
            if os.path.exists(self.data_file):
                import shutil
                shutil.copy2(self.data_file, self.backup_file)
            
            # Prepare state data
            state_data = {
                'timestamp': datetime.now().isoformat(),
                'account_number': ACCOUNT_NUMBER,
                'magic_number': MAGIC_NUMBER,
                'bot_version': '2.0_enhanced',
                'total_trades': trade_manager.total_trades,
                'next_batch_id': trade_manager.next_batch_id,
                'emergency_stop_active': trade_manager.emergency_stop_active,
                'initial_balance': trade_manager.initial_balance,
                'batches': {}
            }
            
            # Save all martingale batches
            for batch_key, batch in trade_manager.martingale_batches.items():
                state_data['batches'][batch_key] = {
                    'batch_id': batch.batch_id,
                    'symbol': batch.symbol,
                    'direction': batch.direction,
                    'initial_entry_price': batch.initial_entry_price,
                    'initial_sl_distance': batch.initial_sl_distance,
                    'current_layer': batch.current_layer,
                    'total_volume': batch.total_volume,
                    'total_invested': batch.total_invested,
                    'breakeven_price': batch.breakeven_price,
                    'created_time': batch.created_time.isoformat(),
                    'last_layer_time': batch.last_layer_time.isoformat(),
                    'trades': []
                }
                
                # Save all trades in batch
                for trade in batch.trades:
                    trade_data = {
                        'order_id': trade.get('order_id'),
                        'layer': trade.get('layer'),
                        'volume': trade.get('volume'),
                        'entry_price': trade.get('entry_price'),
                        'tp': trade.get('tp'),
                        'entry_time': trade.get('entry_time').isoformat() if trade.get('entry_time') else None,
                        'enhanced_comment': trade.get('enhanced_comment'),
                        'trade_id': trade.get('trade_id')
                    }
                    state_data['batches'][batch_key]['trades'].append(trade_data)
            
            # Write to file
            with open(self.data_file, 'w') as f:
                json.dump(state_data, f, indent=2, default=str)
                
            logger.info(f"💾 State saved: {len(state_data['batches'])} batches, {sum(len(b['trades']) for b in state_data['batches'].values())} trades")
            return True
            
        except Exception as e:
            logger.error(f"❌ Failed to save state: {e}")
            return False
    
    def load_and_recover_state(self, trade_manager):
        """Load state and perform automatic recovery with MT5 validation"""
        try:
            # Try to load saved state
            saved_batches = self.load_saved_state()
            if not saved_batches:
                logger.info("🆕 No saved state found - starting fresh")
                return True
            
            # Get current MT5 positions
            mt5_positions = self.get_mt5_positions()
            
            # Perform intelligent recovery
            recovered_batches = self.recover_batches(saved_batches, mt5_positions, trade_manager)
            
            logger.info(f"🔄 Recovery complete: {len(recovered_batches)} batches restored")
            return True
            
        except Exception as e:
            logger.error(f"❌ Recovery failed: {e}")
            return self.try_backup_recovery(trade_manager)
    
    def load_saved_state(self):
        """Load saved state from file"""
        if not os.path.exists(self.data_file):
            return None
            
        try:
            with open(self.data_file, 'r') as f:
                state_data = json.load(f)
            
            # Validate saved state
            if state_data.get('account_number') != ACCOUNT_NUMBER:
                logger.warning(f"⚠️ Account mismatch: saved={state_data.get('account_number')}, current={ACCOUNT_NUMBER}")
                return None
            
            if state_data.get('magic_number') != MAGIC_NUMBER:
                logger.warning(f"⚠️ Magic number mismatch: saved={state_data.get('magic_number')}, current={MAGIC_NUMBER}")
                return None
            
            saved_time = datetime.fromisoformat(state_data['timestamp'])
            time_diff = (datetime.now() - saved_time).total_seconds()
            
            logger.info(f"📁 Loaded saved state from {saved_time} ({time_diff:.0f}s ago)")
            logger.info(f"   Saved batches: {len(state_data.get('batches', {}))}")
            
            return state_data
            
        except Exception as e:
            logger.error(f"❌ Error loading saved state: {e}")
            return None
    
    def get_mt5_positions(self):
        """Get current MT5 positions with our magic number"""
        try:
            positions = mt5.positions_get()
            if positions is None:
                return []
            
            our_positions = [pos for pos in positions if pos.magic == MAGIC_NUMBER]
            
            logger.info(f"🔍 Found {len(our_positions)} MT5 positions with our magic number")
            
            # Parse positions to extract batch info from comments
            parsed_positions = []
            for pos in our_positions:
                parsed_pos = {
                    'ticket': pos.ticket,
                    'symbol': pos.symbol,
                    'type': pos.type,
                    'volume': pos.volume,
                    'price_open': pos.price_open,
                    'price_current': pos.price_current,
                    'tp': pos.tp,
                    'sl': pos.sl,
                    'profit': pos.profit,
                    'comment': pos.comment,
                    'time': pos.time
                }
                
                # Parse batch info from comment (BM01_BTCUSD_B01)
                batch_info = self.parse_comment(pos.comment)
                parsed_pos.update(batch_info)
                
                parsed_positions.append(parsed_pos)
            
            return parsed_positions
            
        except Exception as e:
            logger.error(f"❌ Error getting MT5 positions: {e}")
            return []
    
    def parse_comment(self, comment):
        """Parse batch information from trade comment"""
        try:
            # Expected format: BM01_BTCUSD_B01 or BM01_BTCUSD_S02
            if not comment or not comment.startswith('BM'):
                return {'batch_id': None, 'direction': None, 'layer': None}
            
            parts = comment.split('_')
            if len(parts) < 3:
                return {'batch_id': None, 'direction': None, 'layer': None}
            
            # Extract batch ID: BM01 -> 1
            batch_id = int(parts[0][2:]) if parts[0][2:].isdigit() else None
            
            # Extract direction and layer: B01 -> long, 1 or S02 -> short, 2
            direction_layer = parts[2]
            if direction_layer.startswith('B'):
                direction = 'long'
                layer = int(direction_layer[1:]) if direction_layer[1:].isdigit() else None
            elif direction_layer.startswith('S'):
                direction = 'short' 
                layer = int(direction_layer[1:]) if direction_layer[1:].isdigit() else None
            else:
                direction = None
                layer = None
            
            return {
                'batch_id': batch_id,
                'direction': direction,
                'layer': layer
            }
            
        except Exception as e:
            logger.error(f"❌ Error parsing comment '{comment}': {e}")
            return {'batch_id': None, 'direction': None, 'layer': None}
    
    def recover_batches(self, saved_state, mt5_positions, trade_manager):
        """Intelligent batch recovery - merge saved state with MT5 reality"""
        try:
            recovered_batches = {}
            
            # Group MT5 positions by batch
            mt5_batches = {}
            for pos in mt5_positions:
                if pos['batch_id'] and pos['direction']:
                    batch_key = f"{pos['symbol']}_{pos['direction']}"
                    if batch_key not in mt5_batches:
                        mt5_batches[batch_key] = []
                    mt5_batches[batch_key].append(pos)
            
            logger.info(f"🔍 MT5 Analysis: Found {len(mt5_batches)} active batches")
            
            # Process each saved batch
            for batch_key, saved_batch in saved_state.get('batches', {}).items():
                logger.info(f"\n🔄 Recovering batch: {batch_key}")
                
                # Check if this batch still exists in MT5
                if batch_key in mt5_batches:
                    mt5_batch_positions = mt5_batches[batch_key]
                    
                    # Reconstruct batch from MT5 positions
                    recovered_batch = self.reconstruct_batch_from_mt5(
                        saved_batch, mt5_batch_positions, trade_manager
                    )
                    
                    if recovered_batch:
                        recovered_batches[batch_key] = recovered_batch
                        
                        # Check for missed martingale opportunities
                        self.check_missed_layers(recovered_batch, mt5_batch_positions)
                        
                        logger.info(f"✅ Recovered: {batch_key} with {len(recovered_batch.trades)} active trades")
                    else:
                        logger.warning(f"⚠️ Failed to reconstruct: {batch_key}")
                else:
                    logger.info(f"🎯 Completed: {batch_key} (no MT5 positions found)")
            
            # Check for orphaned MT5 positions (not in saved state)
            self.check_orphaned_positions(mt5_batches, saved_state, trade_manager)
            
            # Update trade manager
            trade_manager.martingale_batches = recovered_batches
            trade_manager.next_batch_id = saved_state.get('next_batch_id', 1)
            trade_manager.total_trades = saved_state.get('total_trades', 0)
            trade_manager.initial_balance = saved_state.get('initial_balance')
            
            return recovered_batches
            
        except Exception as e:
            logger.error(f"❌ Error in batch recovery: {e}")
            return {}
    
    def reconstruct_batch_from_mt5(self, saved_batch, mt5_positions, trade_manager):
        """Reconstruct a batch from MT5 positions and saved data"""
        try:
            # Create new batch object
            batch = MartingaleBatch(
                symbol=saved_batch['symbol'],
                direction=saved_batch['direction'],
                initial_sl_distance=saved_batch['initial_sl_distance'],
                entry_price=saved_batch['initial_entry_price']
            )
            
            # Restore batch properties
            batch.batch_id = saved_batch['batch_id']
            batch.created_time = datetime.fromisoformat(saved_batch['created_time'])
            batch.last_layer_time = datetime.fromisoformat(saved_batch['last_layer_time'])
            
            # Reconstruct trades from MT5 positions
            batch.trades = []
            batch.current_layer = 0
            batch.total_volume = 0
            batch.total_invested = 0
            
            # Sort MT5 positions by layer
            sorted_positions = sorted(mt5_positions, key=lambda x: x.get('layer', 0))
            
            for pos in sorted_positions:
                trade = {
                    'order_id': pos['ticket'],
                    'layer': pos.get('layer', 1),
                    'volume': pos['volume'],
                    'entry_price': pos['price_open'],
                    'tp': pos['tp'],
                    'sl': pos['sl'],
                    'entry_time': datetime.fromtimestamp(pos['time']),
                    'enhanced_comment': pos['comment'],
                    'symbol': pos['symbol'],
                    'direction': saved_batch['direction']
                }
                
                batch.trades.append(trade)
                batch.total_volume += trade['volume']
                batch.total_invested += trade['volume'] * trade['entry_price']
                batch.current_layer = max(batch.current_layer, trade['layer'])
            
            # Recalculate breakeven
            if batch.total_volume > 0:
                batch.breakeven_price = batch.total_invested / batch.total_volume
            
            # Update TP for all trades to ensure consistency
            current_tp = batch.calculate_adaptive_batch_tp()
            if current_tp:
                batch.update_all_tps_with_retry(current_tp)
            
            logger.info(f"   Reconstructed: {batch.current_layer} layers, breakeven: {batch.breakeven_price:.5f}")
            
            return batch
            
        except Exception as e:
            logger.error(f"❌ Error reconstructing batch: {e}")
            return None
    
    def check_missed_layers(self, batch, mt5_positions):
        """Check if we missed any martingale opportunities while offline"""
        try:
            # Get current price
            tick = mt5.symbol_info_tick(batch.symbol)
            if not tick:
                return
            
            current_price = tick.bid if batch.direction == 'long' else tick.ask
            
            # Check if price has moved beyond next trigger levels
            max_possible_layer = min(batch.current_layer + 5, MAX_MARTINGALE_LAYERS)
            
            for potential_layer in range(batch.current_layer + 1, max_possible_layer + 1):
                distance_for_layer = batch.initial_sl_distance * (potential_layer - 1)
                
                if batch.direction == 'long':
                    trigger_price = batch.initial_entry_price - distance_for_layer
                    price_reached = current_price <= trigger_price
                else:
                    trigger_price = batch.initial_entry_price + distance_for_layer
                    price_reached = current_price >= trigger_price
                
                if price_reached:
                    pips_past = abs(current_price - trigger_price) / get_pip_size(batch.symbol)
                    logger.warning(f"⚠️ MISSED LAYER: {batch.symbol} Layer {potential_layer}")
                    logger.warning(f"   Trigger: {trigger_price:.5f}, Current: {current_price:.5f}")
                    logger.warning(f"   Price moved {pips_past:.1f} pips past trigger")
            
        except Exception as e:
            logger.error(f"❌ Error checking missed layers: {e}")
    
    def check_orphaned_positions(self, mt5_batches, saved_state, trade_manager):
        """Check for MT5 positions that aren't in our saved state"""
        try:
            saved_batch_keys = set(saved_state.get('batches', {}).keys())
            mt5_batch_keys = set(mt5_batches.keys())
            
            orphaned_batches = mt5_batch_keys - saved_batch_keys
            
            if orphaned_batches:
                logger.warning(f"🚨 ORPHANED POSITIONS DETECTED: {len(orphaned_batches)} batches")
                
                for batch_key in orphaned_batches:
                    positions = mt5_batches[batch_key]
                    logger.warning(f"   {batch_key}: {len(positions)} positions")
                    
                    # Try to reconstruct orphaned batch
                    symbol, direction = batch_key.split('_')
                    
                    # Create emergency batch
                    avg_entry = sum(pos['price_open'] * pos['volume'] for pos in positions) / sum(pos['volume'] for pos in positions)
                    
                    emergency_batch = MartingaleBatch(
                        symbol=symbol,
                        direction=direction,
                        initial_sl_distance=50 * get_pip_size(symbol),  # Estimate
                        entry_price=avg_entry
                    )
                    
                    emergency_batch.batch_id = trade_manager.next_batch_id
                    trade_manager.next_batch_id += 1
                    
                    # Add positions as trades
                    for pos in sorted(positions, key=lambda x: x.get('layer', 0)):
                        trade = {
                            'order_id': pos['ticket'],
                            'layer': pos.get('layer', 1),
                            'volume': pos['volume'],
                            'entry_price': pos['price_open'],
                            'tp': pos['tp'],
                            'sl': pos['sl'],
                            'entry_time': datetime.fromtimestamp(pos['time']),
                            'enhanced_comment': pos['comment'],
                            'symbol': symbol,
                            'direction': direction
                        }
                        emergency_batch.trades.append(trade)
                    
                    # Calculate totals
                    emergency_batch.total_volume = sum(t['volume'] for t in emergency_batch.trades)
                    emergency_batch.total_invested = sum(t['volume'] * t['entry_price'] for t in emergency_batch.trades)
                    emergency_batch.breakeven_price = emergency_batch.total_invested / emergency_batch.total_volume
                    emergency_batch.current_layer = len(emergency_batch.trades)
                    
                    # Add to trade manager
                    trade_manager.martingale_batches[batch_key] = emergency_batch
                    
                    logger.info(f"🚑 EMERGENCY RECOVERY: Created batch for {batch_key}")
        
        except Exception as e:
            logger.error(f"❌ Error checking orphaned positions: {e}")
    
    def try_backup_recovery(self, trade_manager):
        """Try to recover from backup file"""
        try:
            if os.path.exists(self.backup_file):
                logger.info("🔄 Attempting backup recovery...")
                original_file = self.data_file
                self.data_file = self.backup_file
                result = self.load_and_recover_state(trade_manager)
                self.data_file = original_file
                return result
            else:
                logger.warning("⚠️ No backup file available")
                return True  # Continue with fresh start
                
        except Exception as e:
            logger.error(f"❌ Backup recovery failed: {e}")
            return True  # Continue with fresh start
class EnhancedTradeManager:
    def __init__(self):
        self.active_trades = []
        self.martingale_batches = {}  # {symbol_direction: MartingaleBatch}
        self.total_trades = 0
        self.emergency_stop_active = False
        self.initial_balance = None
        self.next_batch_id = 1
        
        # Initialize persistence system
        self.persistence = BotPersistence()
        
        # ✅ RECOVERY ON STARTUP
        logger.info("🔄 Attempting to recover previous state...")
        recovery_success = self.persistence.load_and_recover_state(self)
        if recovery_success:
            logger.info("✅ State recovery completed successfully")
        else:
            logger.warning("⚠️ State recovery failed - starting fresh")
        
    def can_trade(self, symbol):
        """Check if we can trade this symbol"""
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
            if margin_level < 200:  # Less than 200% margin level
                logger.warning(f"Low margin level: {margin_level:.1f}%")
                return False
            
            # Check free margin
            if account_info.margin_free < 1000:  # Less than $1000 free margin
                logger.warning(f"Low free margin: ${account_info.margin_free:.2f}")
                return False
            
            return True
            
        except Exception as e:
            logger.error(f"Error in can_trade: {e}")
            return False
    
    def has_position(self, symbol, direction):
        """Check if we already have a position for symbol+direction"""
        try:
            batch_key = f"{symbol}_{direction}"
            
            # Check if we have an active batch
            if batch_key in self.martingale_batches:
                batch = self.martingale_batches[batch_key]
                return len(batch.trades) > 0
                
            return False
        except Exception as e:
            logger.error(f"Error checking position for {symbol} {direction}: {e}")
            return False
    
    def get_or_create_batch(self, symbol, direction, sl_distance, entry_price):
        """Get existing batch or create new one"""
        try:
            batch_key = f"{symbol}_{direction}"
            
            if batch_key not in self.martingale_batches:
                # Create new batch
                batch = MartingaleBatch(symbol, direction, sl_distance, entry_price)
                batch.batch_id = self.next_batch_id
                self.next_batch_id += 1
                self.martingale_batches[batch_key] = batch
                logger.info(f"Created new martingale batch: {batch_key} (ID: {batch.batch_id})")
                
            return self.martingale_batches[batch_key]
        except Exception as e:
            logger.error(f"Error creating/getting batch for {symbol} {direction}: {e}")
            return None
    
    def add_trade(self, trade_info):
        """Add trade to tracking and batch management"""
        try:
            self.total_trades += 1
            trade_info['trade_id'] = self.total_trades
            trade_info['entry_time'] = datetime.now()
            
            # Add to batch if martingale enabled
            if MARTINGALE_ENABLED:
                symbol = trade_info['symbol']
                direction = trade_info['direction']
                sl_distance = trade_info.get('sl_distance', 0)
                entry_price = trade_info['entry_price']
                
                batch = self.get_or_create_batch(symbol, direction, sl_distance, entry_price)
                if batch:
                    batch.add_trade(trade_info)
                    
                    # Calculate and update batch TP with adaptive system
                    try:
                        new_tp = batch.calculate_adaptive_batch_tp()
                        if new_tp:
                            logger.info(f"Calculated adaptive batch TP: {new_tp:.5f}")
                            # Update TP for all trades in batch (including the new one)
                            batch.update_all_tps_with_retry(new_tp)
                    except Exception as e:
                        logger.error(f"Error updating batch TP: {e}")
            
            self.active_trades.append(trade_info)
            
            # 💾 SAVE STATE AFTER EVERY TRADE EXECUTION
            try:
                self.persistence.save_bot_state(self)
                logger.debug("State saved successfully")
            except Exception as e:
                logger.error(f"Failed to save state: {e}")
            
            return True
            
        except Exception as e:
            logger.error(f"Error adding trade: {e}")
            return False
    
    def sync_with_mt5_positions(self):
        """Synchronize trade manager with actual MT5 positions"""
        try:
            positions = mt5.positions_get()
            if positions is None:
                positions = []
            
            # Filter our positions
            our_positions = [pos for pos in positions if pos.magic == MAGIC_NUMBER]
            
            logger.debug(f"Syncing with {len(our_positions)} MT5 positions")
            
            # Check each batch against MT5 reality
            for batch_key, batch in list(self.martingale_batches.items()):
                if not batch.trades:
                    continue
                
                # Find which trades still exist in MT5
                existing_trades = []
                for trade in batch.trades:
                    order_id = trade.get('order_id')
                    if any(pos.ticket == order_id for pos in our_positions):
                        existing_trades.append(trade)
                    else:
                        logger.warning(f"Trade {order_id} no longer exists in MT5")
                
                # Update batch with existing trades
                if existing_trades != batch.trades:
                    logger.info(f"Updating {batch_key}: {len(batch.trades)} → {len(existing_trades)} trades")
                    batch.trades = existing_trades
                    
                    # Recalculate batch totals
                    if batch.trades:
                        batch.total_volume = sum(t['volume'] for t in batch.trades)
                        batch.total_invested = sum(t['volume'] * t['entry_price'] for t in batch.trades)
                        batch.breakeven_price = batch.total_invested / batch.total_volume if batch.total_volume > 0 else 0
                    else:
                        # Batch is empty - remove it
                        logger.info(f"Batch {batch_key} completed - all trades closed")
                        del self.martingale_batches[batch_key]
            
            return True
            
        except Exception as e:
            logger.error(f"Error syncing with MT5: {e}")
            return False
    
    def check_martingale_opportunities_enhanced(self, current_prices):
        """Enhanced martingale checking with better error handling"""
        opportunities = []
        
        try:
            if not MARTINGALE_ENABLED or self.emergency_stop_active:
                return opportunities
            
            # First sync with MT5 to ensure accuracy
            self.sync_with_mt5_positions()
            
            for batch_key, batch in self.martingale_batches.items():
                try:
                    symbol = batch.symbol
                    
                    if symbol not in current_prices:
                        continue
                    
                    # Get current price for this direction
                    current_price = current_prices[symbol]['bid'] if batch.direction == 'long' else current_prices[symbol]['ask']
                    
                    # Check multiple potential trigger levels (in case we missed some)
                    for potential_layer in range(batch.current_layer + 1, min(batch.current_layer + 3, MAX_MARTINGALE_LAYERS + 1)):
                        distance_for_layer = batch.initial_sl_distance * (potential_layer - 1)
                        
                        if batch.direction == 'long':
                            trigger_price = batch.initial_entry_price - distance_for_layer
                            price_reached = current_price <= trigger_price
                        else:
                            trigger_price = batch.initial_entry_price + distance_for_layer
                            price_reached = current_price >= trigger_price
                        
                        if price_reached:
                            # Check if we can add this layer
                            time_since_last = (datetime.now() - batch.last_layer_time).total_seconds()
                            
                            if time_since_last >= 30:  # Reduced wait time
                                opportunities.append({
                                    'batch': batch,
                                    'symbol': symbol,
                                    'direction': batch.direction,
                                    'entry_price': current_price,
                                    'layer': potential_layer,
                                    'trigger_price': trigger_price,
                                    'distance_pips': abs(current_price - trigger_price) / get_pip_size(symbol)
                                })
                                
                                logger.info(f"📈 Martingale opportunity: {symbol} {batch.direction} Layer {potential_layer}")
                                logger.info(f"   Trigger: {trigger_price:.5f}, Current: {current_price:.5f}")
                                break  # Only add one layer at a time
                            else:
                                logger.debug(f"Layer {potential_layer} blocked by fast move protection ({time_since_last:.0f}s)")
                                
                except Exception as e:
                    logger.error(f"Error checking martingale for {batch_key}: {e}")
                    continue
            
            return opportunities
            
        except Exception as e:
            logger.error(f"Error in check_martingale_opportunities_enhanced: {e}")
            return []
    
    def monitor_batch_exits(self, current_prices):
        """Monitor batches for TP hits and manage exits"""
        try:
            for batch_key, batch in list(self.martingale_batches.items()):
                try:
                    if not batch.trades:
                        continue
                        
                    symbol = batch.symbol
                    if symbol not in current_prices:
                        continue
                        
                    # Check if any position from this batch was closed (TP hit)
                    positions = mt5.positions_get(symbol=symbol)
                    active_tickets = [pos.ticket for pos in positions if pos.magic == MAGIC_NUMBER] if positions else []
                    
                    batch_tickets = [trade.get('order_id') for trade in batch.trades if trade.get('order_id')]
                    missing_positions = [ticket for ticket in batch_tickets if ticket not in active_tickets]
                    
                    if missing_positions:
                        # Some or all positions were closed
                        if len(missing_positions) == len(batch_tickets):
                            # All positions closed - batch completed
                            logger.info(f"🎯 Batch completed: {batch_key} - All {len(batch_tickets)} positions closed")
                            
                            # Clean up batch
                            del self.martingale_batches[batch_key]
                            
                            # Remove from active trades
                            self.active_trades = [t for t in self.active_trades 
                                                if not (t['symbol'] == symbol and t['direction'] == batch.direction)]
                        else:
                            # Partial closure - update batch
                            logger.warning(f"Partial closure detected for {batch_key}")
                            # Remove closed trades from batch
                            batch.trades = [t for t in batch.trades if t.get('order_id') not in missing_positions]
                            
                            # Recalculate batch totals
                            if batch.trades:
                                batch.total_volume = sum(t['volume'] for t in batch.trades)
                                batch.total_invested = sum(t['volume'] * t['entry_price'] for t in batch.trades)
                                batch.breakeven_price = batch.total_invested / batch.total_volume if batch.total_volume > 0 else 0
                                
                except Exception as e:
                    logger.error(f"Error monitoring batch {batch_key}: {e}")
                    continue
                    
        except Exception as e:
            logger.error(f"Error in monitor_batch_exits: {e}")

# ===== SIGNAL GENERATION - SIMPLIFIED VERSION OF YOUR WORKING BOT =====
def execute_martingale_trade(opportunity, trade_manager):
    """Execute a martingale layer trade - FIXED"""
    batch = opportunity['batch']
    symbol = opportunity['symbol']
    direction = opportunity['direction']
    layer = opportunity['layer']
    
    # Create martingale signal with proper SL distance from batch
    martingale_signal = {
        'symbol': symbol,
        'direction': direction,
        'entry_price': opportunity['entry_price'],
        'sl': None,  # No SL for build-from-first
        'tp': None,  # Will be calculated by batch
        'sl_distance_pips': batch.initial_sl_distance / get_pip_size(symbol),  # ✅ FIXED: Use batch SL distance
        'risk_profile': PAIR_RISK_PROFILES.get(symbol, "High"),
        'is_initial': False,
        'layer': layer,
        'sl_distance': batch.initial_sl_distance  # ✅ FIXED: Add the actual distance value
    }
    
    logger.info(f"Executing martingale Layer {layer} for {symbol} {direction}")
    return execute_trade(martingale_signal, trade_manager)

def get_higher_timeframes(base_timeframe):
    """Get higher timeframes for multi-timeframe analysis"""
    timeframe_hierarchy = [
        mt5.TIMEFRAME_M1,
        mt5.TIMEFRAME_M5,
        mt5.TIMEFRAME_M15,
        mt5.TIMEFRAME_M30,
        mt5.TIMEFRAME_H1,
        mt5.TIMEFRAME_H4,
        mt5.TIMEFRAME_D1
    ]
    
    try:
        base_index = timeframe_hierarchy.index(base_timeframe)
        # Return next 2 higher timeframes for confirmation
        higher_tfs = []
        for i in range(base_index + 1, min(base_index + 3, len(timeframe_hierarchy))):
            higher_tfs.append(timeframe_hierarchy[i])
        return higher_tfs
    except ValueError:
        return [mt5.TIMEFRAME_M15, mt5.TIMEFRAME_H1]  # Default

def analyze_symbol_multi_timeframe(symbol, base_timeframe):
    """Analyze symbol across multiple timeframes"""
    timeframes = [base_timeframe] + get_higher_timeframes(base_timeframe)
    
    analyses = {}
    
    for tf in timeframes:
        df = get_historical_data(symbol, tf, 500)
        if df is None or len(df) < 50:
            continue
            
        df = calculate_indicators(df)
        if df is None or 'adx' not in df.columns:
            continue
        
        latest = df.iloc[-1]
        prev = df.iloc[-2]
        
        # Determine trend direction
        ema_direction = "Up" if latest['ema20'] > prev['ema20'] else "Down"
        
        # Trend strength based on ADX
        if latest['adx'] > 30:
            trend_strength = "Strong"
        elif latest['adx'] > 20:
            trend_strength = "Medium"
        else:
            trend_strength = "Weak"
        
        trend = f"{trend_strength} {ema_direction}ward"
        
        analyses[tf] = {
            'trend': trend,
            'ema_direction': ema_direction,
            'adx': latest['adx'],
            'rsi': latest['rsi'],
            'close': latest['close'],
            'ema20': latest['ema20']
        }
    
    return analyses

def generate_enhanced_signals(pairs, trade_manager):
    """Generate signals with multi-timeframe confirmation"""
    signals = []
    
    for symbol in pairs:
        if not trade_manager.can_trade(symbol):
            continue
            
        # Skip if we already have positions in both directions
        if (trade_manager.has_position(symbol, 'long') and 
            trade_manager.has_position(symbol, 'short')):
            continue
        
        # Multi-timeframe analysis
        analyses = analyze_symbol_multi_timeframe(symbol, GLOBAL_TIMEFRAME)
        
        if not analyses or GLOBAL_TIMEFRAME not in analyses:
            continue
        
        primary_analysis = analyses[GLOBAL_TIMEFRAME]
        
        # Get risk profile and parameters
        risk_profile = PAIR_RISK_PROFILES.get(symbol, "High")
        params = PARAM_SETS[risk_profile]
        
        # Get primary timeframe data for detailed analysis
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
        
        # Multi-timeframe confirmation
        higher_timeframes = get_higher_timeframes(GLOBAL_TIMEFRAME)
        aligned_timeframes = 0
        
        for tf in higher_timeframes[:1]:  # Check first higher timeframe
            if tf in analyses:
                higher_analysis = analyses[tf]
                if primary_analysis['ema_direction'] == higher_analysis['ema_direction']:
                    aligned_timeframes += 1
        
        # Require at least 1 higher timeframe alignment for stronger signals
        if aligned_timeframes < 1:
            continue
        
        # Current price relative to EMA
        close_to_ema = abs(latest['close'] - latest['ema20']) / latest['ema20'] < params['ema_buffer_pct']
        
        # Signal generation for both directions
        for direction in ['long', 'short']:
            # Skip if we already have position in this direction
            if trade_manager.has_position(symbol, direction):
                continue
            
            signal_valid = False
            
            if direction == 'long':
                # Long signal conditions
                bullish_trend = primary_analysis['ema_direction'] == 'Up'
                rsi_condition = (prev['rsi'] < params['rsi_oversold'] and 
                               latest['rsi'] > params['rsi_oversold'])
                price_action = latest['close'] > latest['open']  # Bullish candle
                
                signal_valid = bullish_trend and close_to_ema and (rsi_condition or price_action)
                
            else:  # short
                # Short signal conditions  
                bearish_trend = primary_analysis['ema_direction'] == 'Down'
                rsi_condition = (prev['rsi'] > params['rsi_overbought'] and 
                               latest['rsi'] < params['rsi_overbought'])
                price_action = latest['close'] < latest['open']  # Bearish candle
                
                signal_valid = bearish_trend and close_to_ema and (rsi_condition or price_action)
            
            if signal_valid:
                # Calculate entry, SL, TP
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
                
                if sl_distance_pips >= 10 and tp_distance_pips >= 10:  # Minimum distances
                    signals.append({
                        'symbol': symbol,
                        'direction': direction,
                        'entry_price': entry_price,
                        'sl': sl,  # Will be used for distance calculation only
                        'tp': tp,
                        'atr': atr,
                        'adx_value': latest['adx'],
                        'rsi': latest['rsi'],
                        'sl_distance_pips': sl_distance_pips,
                        'tp_distance_pips': tp_distance_pips,
                        'risk_profile': risk_profile,
                        'timestamp': datetime.now(),
                        'timeframes_aligned': aligned_timeframes + 1,  # Primary + aligned
                        'is_initial': True
                    })
    
    return signals

def execute_trade(signal, trade_manager):
    """Execute trade order - FIXED VERSION"""
    symbol = signal['symbol']
    direction = signal['direction']
    
    # Validate symbol
    if not mt5.symbol_select(symbol, True):
        logger.error(f"Failed to select symbol {symbol}")
        return False
    
    # Get symbol info for validation
    symbol_info = mt5.symbol_info(symbol)
    if symbol_info is None:
        logger.error(f"Failed to get symbol info for {symbol}")
        return False
    
    # Get current prices
    tick = mt5.symbol_info_tick(symbol)
    if tick is None:
        logger.error(f"Failed to get tick data for {symbol}")
        return False
    
    # Check spread
    spread = tick.ask - tick.bid
    spread_pips = spread / get_pip_size(symbol)
    max_spread = {'BTCUSD': 50, 'XAUUSD': 10, 'US500': 50}.get(symbol, 5)
    
    if spread_pips > max_spread:
        logger.warning(f"Spread too high for {symbol}: {spread_pips:.1f} pips")
        return False
    
    # Determine order type and price
    if direction == 'long':
        order_type = mt5.ORDER_TYPE_BUY
        price = tick.ask
    else:
        order_type = mt5.ORDER_TYPE_SELL
        price = tick.bid
    
    # Calculate position size
    account_info = mt5.account_info()
    if account_info is None:
        return False
    
    risk_profile = signal['risk_profile']
    params = PARAM_SETS[risk_profile]
    
    # Reduce risk if in drawdown
    base_risk_pct = params['risk_per_trade_pct']
    if trade_manager.initial_balance:
        current_dd = ((trade_manager.initial_balance - account_info.equity) / trade_manager.initial_balance) * 100
        if current_dd > 5:  # If more than 5% drawdown
            base_risk_pct *= 0.5  # Halve the risk
            logger.info(f"Reducing risk due to {current_dd:.1f}% drawdown")
    
    risk_amount = account_info.balance * (base_risk_pct / 100)
    
    # Check if this is initial trade or martingale layer
    is_martingale = not signal.get('is_initial', True)
    layer = signal.get('layer', 1)
    
    # Generate enhanced comment for trade identification
    if is_martingale:
        # Get batch info for martingale trades
        batch_key = f"{symbol}_{direction}"
        if batch_key in trade_manager.martingale_batches:
            batch = trade_manager.martingale_batches[batch_key]
            batch_id = batch.batch_id
            actual_layer = batch.current_layer + 1
        else:
            batch_id = 99  # Fallback
            actual_layer = layer
    else:
        # For initial trades, create new batch ID
        batch_id = trade_manager.next_batch_id
        actual_layer = 1
    
    # Enhanced comment format: BM01_BTCUSD_B01 (Batch01_Symbol_Buy/SellLayer01)
    batch_prefix = f"BM{batch_id:02d}"
    direction_code = "B" if direction == 'long' else "S"
    layer_suffix = f"{direction_code}{actual_layer:02d}"
    enhanced_comment = f"{batch_prefix}_{symbol}_{layer_suffix}"
    
    if is_martingale:
        # Calculate martingale position size
        batch_key = f"{symbol}_{direction}"
        if batch_key in trade_manager.martingale_batches:
            batch = trade_manager.martingale_batches[batch_key]
            base_volume = batch.trades[0]['volume'] if batch.trades else 0.01
            position_size = calculate_position_size(
                symbol, signal['sl_distance_pips'], risk_amount, 
                is_martingale=True, base_volume=base_volume, layer=layer
            )
        else:
            position_size = calculate_position_size(symbol, signal['sl_distance_pips'], risk_amount)
    else:
        position_size = calculate_position_size(symbol, signal['sl_distance_pips'], risk_amount)
    
    if position_size <= 0:
        logger.error(f"Invalid position size: {position_size}")
        return False
    
    # ✅ NO SL - Build from first approach
    sl = None  # No stop loss on individual trades
    
    # TP will be calculated by batch management
    tp = signal.get('tp')  # Initial TP, will be updated by batch
    
    # Create order request - NO SL
    request = {
        "action": mt5.TRADE_ACTION_DEAL,
        "symbol": symbol,
        "volume": float(position_size),
        "type": order_type,
        "price": float(price),
        # "sl": None,  # ✅ NO SL
        "tp": float(tp) if tp else None,
        "deviation": 20,
        "magic": MAGIC_NUMBER,
        "comment": enhanced_comment,
        "type_time": mt5.ORDER_TIME_GTC,
        "type_filling": mt5.ORDER_FILLING_IOC,
    }
    
    # Remove None values from request
    request = {k: v for k, v in request.items() if v is not None}
    
    logger.info(f"Order request: {symbol} {direction} Layer {layer} - {position_size} lots")
    if tp:
        logger.info(f"  Price: {price:.5f}, NO SL, TP: {tp:.5f}")
    else:
        logger.info(f"  Price: {price:.5f}, NO SL, TP: None")
    
    # ✅ SPECIAL HANDLING FOR US500 - Try different order filling methods
    if symbol == 'US500':
        filling_methods = [mt5.ORDER_FILLING_IOC, mt5.ORDER_FILLING_FOK, mt5.ORDER_FILLING_RETURN]
        logger.info(f"US500 detected - will try {len(filling_methods)} filling methods")
        
        for i, filling_method in enumerate(filling_methods):
            logger.info(f"US500 Attempt {i+1}: Using filling method {filling_method}")
            request["type_filling"] = filling_method
            
            result = mt5.order_send(request)
            
            if result and result.retcode == mt5.TRADE_RETCODE_DONE:
                logger.info(f"✅ US500 trade executed with filling method {filling_method}")
                
                # ✅ FIXED: Safe SL distance calculation
                sl_distance_value = signal.get('sl_distance')
                if sl_distance_value is None:
                    # Calculate from sl_distance_pips if available
                    sl_distance_pips = signal.get('sl_distance_pips', 20)  # Default fallback
                    sl_distance_value = sl_distance_pips * get_pip_size(symbol)
                
                # Add to trade manager with SL distance for martingale calculations
                trade_info = {
                    'symbol': symbol,
                    'direction': direction,
                    'entry_price': result.price,
                    'sl': None,  # No SL
                    'tp': tp,
                    'volume': result.volume,
                    'order_id': result.order,
                    'sl_distance': sl_distance_value,  # ✅ FIXED: Safe value
                    'layer': layer,
                    'is_martingale': is_martingale
                }
                
                trade_manager.add_trade(trade_info)
                return True
            else:
                error_code = result.retcode if result else "None"
                logger.warning(f"US500 Attempt {i+1} failed: {error_code}")
                time.sleep(0.2)
        
        logger.error(f"All US500 filling methods failed")
        return False
    
    # Execute order with retry logic for other symbols
    for attempt in range(3):
        result = mt5.order_send(request)
        
        if result is None:
            logger.error(f"Attempt {attempt+1}: Order send returned None")
            continue
            
        if result.retcode == mt5.TRADE_RETCODE_DONE:
            logger.info(f"✅ Trade executed: {symbol} {direction} Layer {layer} - {position_size} lots at {price}")
            
            # ✅ FIXED: Safe SL distance calculation
            sl_distance_value = signal.get('sl_distance')
            if sl_distance_value is None:
                # Calculate from sl_distance_pips if available
                sl_distance_pips = signal.get('sl_distance_pips', 20)  # Default fallback
                sl_distance_value = sl_distance_pips * get_pip_size(symbol)
            
            # Add to trade manager
            trade_info = {
                'symbol': symbol,
                'direction': direction,
                'entry_price': result.price,
                'sl': None,  # No SL
                'tp': tp,
                'volume': result.volume,
                'order_id': result.order,
                'sl_distance': sl_distance_value,  # ✅ FIXED: Safe value
                'layer': layer,
                'is_martingale': is_martingale
            }
            
            trade_manager.add_trade(trade_info)
            return True
        
        # Handle specific error codes
        elif result.retcode == mt5.TRADE_RETCODE_INVALID_VOLUME:
            logger.warning(f"Attempt {attempt+1}: Invalid volume {request['volume']}")
            
            # Get detailed symbol info for debugging
            symbol_info = mt5.symbol_info(symbol)
            if symbol_info:
                logger.info(f"Symbol {symbol} volume specs:")
                logger.info(f"  Min: {symbol_info.volume_min}")
                logger.info(f"  Max: {symbol_info.volume_max}")
                logger.info(f"  Step: {symbol_info.volume_step}")
            
            # Try different volume approaches
            if attempt == 0:
                # First retry: use exactly minimum volume
                new_volume = symbol_info.volume_min if symbol_info else 0.01
                logger.info(f"Retry 1: Using minimum volume {new_volume}")
            elif attempt == 1:
                # Second retry: try a common safe volume
                new_volume = 0.1
                logger.info(f"Retry 2: Using safe volume {new_volume}")
            else:
                # Third retry: try even smaller
                new_volume = 0.01
                logger.info(f"Retry 3: Using smallest volume {new_volume}")
            
            request["volume"] = float(new_volume)
            
        elif result.retcode == mt5.TRADE_RETCODE_INVALID_STOPS:
            logger.warning(f"Attempt {attempt+1}: Invalid stops, removing TP")
            request.pop("tp", None)  # Remove TP and try again
                
        elif result.retcode == mt5.TRADE_RETCODE_NO_MONEY:
            logger.error("Insufficient funds")
            return False
            
        else:
            logger.error(f"Attempt {attempt+1}: Order failed with retcode: {result.retcode}")
            
        time.sleep(0.5)  # Wait before retry
    
    logger.error(f"All order attempts failed for {symbol}")
    return False



# ===== IMPROVED MAIN ROBOT FUNCTION WITH BETTER ERROR HANDLING =====
def run_simplified_robot():
    """Run the simplified trading robot with enhanced error handling"""
    logger.info("="*60)
    logger.info("SIMPLIFIED BM TRADING ROBOT STARTED - ENHANCED VERSION")
    logger.info("="*60)
    logger.info(f"Primary Timeframe: M5")
    logger.info(f"Pairs: {len(PAIRS)}")
    logger.info(f"Martingale: {MARTINGALE_ENABLED}")
    logger.info("="*60)
    
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
    
    # Initialize trade manager - ENHANCED VERSION WITH RECOVERY
    trade_manager = EnhancedTradeManager()
    
    try:
        cycle_count = 0
        consecutive_errors = 0  # ✅ Track consecutive errors
        
        while True:
            try:
                cycle_count += 1
                current_time = datetime.now()
                
                logger.info(f"\n{'='*50}")
                logger.info(f"Analysis Cycle #{cycle_count} at {current_time}")
                logger.info(f"{'='*50}")
                
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
                
                # Generate signals with multi-timeframe confirmation
                try:
                    signals = generate_enhanced_signals(PAIRS, trade_manager)
                    logger.info(f"Generated {len(signals)} enhanced signals")
                except Exception as e:
                    logger.error(f"Error generating signals: {e}")
                    signals = []
                
                # Execute signals
                for signal in signals:
                    try:
                        if not trade_manager.can_trade(signal['symbol']):
                            continue
                        
                        logger.info(f"\n🎯 Enhanced Signal: {signal['symbol']} {signal['direction'].upper()}")
                        logger.info(f"   Entry: {signal['entry_price']:.5f}")
                        logger.info(f"   SL Distance: {signal['sl_distance_pips']:.1f} pips (for martingale)")
                        logger.info(f"   Initial TP: {signal['tp']:.5f} ({signal['tp_distance_pips']:.1f} pips)")
                        logger.info(f"   ADX: {signal['adx_value']:.1f}, RSI: {signal['rsi']:.1f}")
                        logger.info(f"   Timeframes Aligned: {signal['timeframes_aligned']}")
                        logger.info(f"   🚫 NO SL - Build-from-first approach")
                        
                        if execute_trade(signal, trade_manager):
                            logger.info("✅ Enhanced trade executed successfully")
                        else:
                            logger.error("❌ Trade execution failed")
                            
                    except Exception as e:
                        logger.error(f"Error executing signal for {signal.get('symbol', 'Unknown')}: {e}")
                        continue
                
                # Check for martingale opportunities with enhanced detection
                if MARTINGALE_ENABLED and not trade_manager.emergency_stop_active:
                    try:
                        martingale_opportunities = trade_manager.check_martingale_opportunities_enhanced(current_prices)
                        
                        for opportunity in martingale_opportunities:
                            try:
                                logger.info(f"\n🔄 Martingale Opportunity: {opportunity['symbol']} {opportunity['direction'].upper()}")
                                logger.info(f"   Layer: {opportunity['layer']}")
                                logger.info(f"   Trigger: {opportunity['trigger_price']:.5f}")
                                logger.info(f"   Current: {opportunity['entry_price']:.5f}")
                                logger.info(f"   Distance: {opportunity['distance_pips']:.1f} pips")
                                
                                if execute_martingale_trade(opportunity, trade_manager):
                                    logger.info("✅ Martingale layer executed successfully")
                                    
                                    # Update batch TP after adding layer with adaptive system
                                    batch = opportunity['batch']
                                    try:
                                        new_tp = batch.calculate_adaptive_batch_tp()
                                        if new_tp:
                                            logger.info(f"🔄 Updating batch TP to {new_tp:.5f}")
                                            batch.update_all_tps_with_retry(new_tp)
                                    except Exception as e:
                                        logger.error(f"Error updating batch TP after martingale: {e}")
                                else:
                                    logger.error("❌ Martingale execution failed")
                                    
                            except Exception as e:
                                logger.error(f"Error executing martingale for {opportunity.get('symbol', 'Unknown')}: {e}")
                                continue
                                
                    except Exception as e:
                        logger.error(f"Error checking martingale opportunities: {e}")
                
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
                
                # Show enhanced account status with batch information
                try:
                    account_info = mt5.account_info()
                    if account_info:
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
                        
                        # Enhanced batch status
                        active_batches = len([b for b in trade_manager.martingale_batches.values() if b.trades])
                        if active_batches > 0:
                            logger.info(f"\n🔄 Martingale Batches: {active_batches} active")
                            for batch_key, batch in trade_manager.martingale_batches.items():
                                if batch.trades:
                                    logger.info(f"   {batch_key}: Layer {batch.current_layer}/{MAX_MARTINGALE_LAYERS}")
                                    logger.info(f"     Volume: {batch.total_volume:.2f}, Breakeven: {batch.breakeven_price:.5f}")
                                    try:
                                        next_trigger = batch.get_next_trigger_price()
                                        logger.info(f"     Next trigger: {next_trigger:.5f}")
                                        
                                        # Show current TP
                                        if batch.trades and batch.trades[0].get('tp'):
                                            current_tp = batch.trades[0]['tp']
                                            logger.info(f"     Current TP: {current_tp:.5f}")
                                    except Exception as e:
                                        logger.error(f"Error getting batch info for {batch_key}: {e}")
                        else:
                            logger.info(f"\n🎯 No active martingale batches - ready for new signals")
                            
                except Exception as e:
                    logger.error(f"Error displaying account status: {e}")
                
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
                logger.info("\n🛑 Robot stopped by user")
                raise  # Re-raise to exit main loop
                
            except Exception as e:
                consecutive_errors += 1
                logger.error(f"\n❌ Error in main cycle #{cycle_count}: {e}")
                logger.error(f"Consecutive errors: {consecutive_errors}")
                
                # Emergency state save on error
                try:
                    trade_manager.persistence.save_bot_state(trade_manager)
                    logger.info("💾 Emergency state saved")
                except Exception as save_error:
                    logger.error(f"Failed to save emergency state: {save_error}")
                
                # If too many consecutive errors, stop the robot
                if consecutive_errors >= 10:
                    logger.critical(f"🚨 Too many consecutive errors ({consecutive_errors}) - stopping robot")
                    break
                
                # Import traceback for detailed error info
                import traceback
                logger.error(f"Detailed error info:\n{traceback.format_exc()}")
                
                # Wait before retrying
                error_sleep = min(consecutive_errors * 30, 300)  # Max 5 minutes
                logger.info(f"⏰ Waiting {error_sleep}s before retry...")
                time.sleep(error_sleep)
                
    except KeyboardInterrupt:
        logger.info("\n🛑 Robot stopped by user")
    except Exception as e:
        logger.error(f"\n❌ Fatal error in main robot: {e}")
        import traceback
        traceback.print_exc()
    finally:
        # Final cleanup and state save
        try:
            logger.info("🔄 Performing final cleanup...")
            trade_manager.persistence.save_bot_state(trade_manager)
            logger.info("💾 Final state saved successfully")
        except Exception as e:
            logger.error(f"Error during final cleanup: {e}")
        
        try:
            mt5.shutdown()
            logger.info("MT5 connection closed")
        except Exception as e:
            logger.error(f"Error closing MT5: {e}")
            
# ===== ADDITIONAL UTILITY FUNCTIONS FOR ROBUSTNESS =====

def safe_get_pip_size(symbol):
    """Safe version of get_pip_size with error handling"""
    try:
        return get_pip_size(symbol)
    except Exception as e:
        logger.error(f"Error getting pip size for {symbol}: {e}")
        # Return sensible defaults
        if 'JPY' in symbol.upper():
            return 0.01
        elif symbol.upper() in ['US500', 'NAS100', 'SPX500']:
            return 0.1
        elif symbol.upper() in ['XAUUSD', 'GOLD']:
            return 0.1
        elif symbol.upper() in ['BTCUSD', 'ETHUSD', 'XRPUSD']:
            return 1.0
        else:
            return 0.0001

def safe_normalize_volume(symbol, volume):
    """Safe version of normalize_volume with error handling"""
    try:
        return normalize_volume(symbol, volume)
    except Exception as e:
        logger.error(f"Error normalizing volume for {symbol}: {e}")
        # Return safe minimum volume
        return 0.01

def safe_calculate_position_size(symbol, stop_loss_pips, risk_amount, is_martingale=False, base_volume=None, layer=1):
    """Safe version of calculate_position_size with error handling"""
    try:
        return calculate_position_size(symbol, stop_loss_pips, risk_amount, is_martingale, base_volume, layer)
    except Exception as e:
        logger.error(f"Error calculating position size for {symbol}: {e}")
        # Return safe minimum volume
        return 0.01

# ===== ENHANCED MARTINGALE BATCH WITH BETTER ERROR HANDLING =====
class SafeMartingaleBatch(MartingaleBatch):
    """Enhanced MartingaleBatch with better error handling"""
    
    def calculate_adaptive_batch_tp(self, market_volatility_pips=None):
        """Adaptive TP with error handling"""
        try:
            return super().calculate_adaptive_batch_tp(market_volatility_pips)
        except Exception as e:
            logger.error(f"Error calculating adaptive TP for {self.symbol}: {e}")
            # Return a safe fallback TP
            try:
                pip_size = safe_get_pip_size(self.symbol)
                fallback_pips = 10  # 10 pip profit as emergency fallback
                
                if self.direction == 'long':
                    return self.breakeven_price + (fallback_pips * pip_size)
                else:
                    return self.breakeven_price - (fallback_pips * pip_size)
            except Exception as fallback_error:
                logger.error(f"Even fallback TP calculation failed: {fallback_error}")
                return None
    
    def update_all_tps_with_retry(self, new_tp, max_attempts=3):
        """Update TP with enhanced error handling"""
        try:
            return super().update_all_tps_with_retry(new_tp, max_attempts)
        except Exception as e:
            logger.error(f"Error updating TPs for {self.symbol}: {e}")
            return False
    
    def should_add_layer(self, current_price, fast_move_threshold_seconds=30):
        """Layer addition check with error handling"""
        try:
            return super().should_add_layer(current_price, fast_move_threshold_seconds)
        except Exception as e:
            logger.error(f"Error checking layer addition for {self.symbol}: {e}")
            return False

if __name__ == "__main__":
    run_simplified_robot()