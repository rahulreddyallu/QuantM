"""
Advanced Quantitative Trading Signal Bot
Combines pattern recognition, technical analysis and signal generation
with detailed reporting and risk management
"""

import pandas as pd
import pandas_ta as ta
import numpy as np
import datetime
import threading
import uuid
import re
import time
import json
import logging
import asyncio
import telegram
from telegram.ext import Updater
import requests
from upstox_client.api_client import ApiClient
from upstox_client.api.login_api import LoginApi
from upstox_client.api.market_quote_api import MarketQuoteApi

# Global initialization locks and state tracking
_INITIALIZATION_LOCK = threading.Lock()
_BOT_INITIALIZED = False
_LOGGING_INITIALIZED = False
_BOT_INSTANCE_UUID = str(uuid.uuid4())
_INIT_TIMESTAMP = None
_BOT_SETUP_COMPLETE = False
_BOT_RUNNING = False

# Configuration
class Config:
    """Configuration settings for the trading bot"""
    
    # API Keys
    UPSTOX_API_KEY = "ad55de1b-c7d1-4adc-b559-3830bf1efd72"
    UPSTOX_API_SECRET = "969nyjgapm"
    UPSTOX_REDIRECT_URI = "https://localhost"
    UPSTOX_CODE = "eyJ0eXAiOiJKV1QiLCJrZXlfaWQiOiJza192MS4wIiwiYWxnIjoiSFMyNTYifQ.eyJzdWIiOiI0TEFGUDkiLCJqdGkiOiI2ODEzOWU2N2NiOWRhMDZiZGU2MDFiNzAiLCJpc011bHRpQ2xpZW50IjpmYWxzZSwiaXNQbHVzUGxhbiI6ZmFsc2UsImlhdCI6MTc0NjExNjE5OSwiaXNzIjoidWRhcGktZ2F0ZXdheS1zZXJ2aWNlIiwiZXhwIjoxNzQ2MTM2ODAwfQ.LwX5Qi_mWBq8nNvCfSGm8tGM_Fv49gK78ej_fAzswYU"
    
    # Telegram notification settings
    ENABLE_TELEGRAM_ALERTS = True
    TELEGRAM_BOT_TOKEN = "7209852741:AAEf-_f6TeZK1-_R55yq365iU_54rk95y-c"
    TELEGRAM_CHAT_ID = "936205208"
    ENABLE_DAILY_REPORT = True
    
    # Analysis parameters
    SHORT_TERM_LOOKBACK = 120  # Days for short-term analysis
    LONG_TERM_LOOKBACK = 365   # Days for long-term analysis
    
    INTERVALS = {
        "short_term": "1D",   # Daily candles
        "long_term": "1W"     # Weekly candles
    }
    
    # Stock list to analyze (instrument keys)
    STOCK_LIST = [
        "NSE_EQ|INE009A01021",  # Example: Infosys
        "NSE_EQ|INE062A01020"   # Example: TCS
        # Add more stocks here
    ]
    
    # Signal strength threshold
    MINIMUM_SIGNAL_STRENGTH = 6
    
    # Indicator parameters
    INDICATORS = {
        "moving_averages": {
            "sma_mid": 50,
            "sma_long": 200,
            "ema_short": 9,
            "ema_long": 21
        },
        "macd": {
            "fast_period": 12,
            "slow_period": 26,
            "signal_period": 9
        },
        "supertrend": {
            "period": 10,
            "multiplier": 3
        },
        "parabolic_sar": {
            "acceleration_factor": 0.02,
            "max_acceleration_factor": 0.2
        },
        "aroon": {
            "period": 14,
            "uptrend_threshold": 70,
            "downtrend_threshold": 30
        },
        "rsi": {
            "period": 14,
            "oversold": 30,
            "overbought": 70
        },
        "stochastic": {
            "k_period": 14,
            "d_period": 3,
            "oversold": 20,
            "overbought": 80
        },
        "roc": {
            "period": 10
        },
        "bollinger_bands": {
            "period": 20,
            "std_dev": 2
        },
        "atr": {
            "period": 14,
            "multiplier": 2
        },
        "alligator": {
            "jaw_period": 13,
            "teeth_period": 8,
            "lips_period": 5
        },
        "cpr": {
            "use_previous_day": True
        },
        "volume_ratios": {
            "high_volume_threshold": 1.5,
            "low_volume_threshold": 0.5
        },
        "reward_risk": {
            "target_multiplier": 1.5,
            "stop_multiplier": 1.0,
            "min_rrr": 1.5
        }
    }
    
    # Pattern recognition settings
    CANDLESTICK_PATTERNS = {
        "bullish_engulfing": True,
        "bearish_engulfing": True,
        "doji": True,
        "hammer": True,
        "shooting_star": True,
        "morning_star": True,
        "evening_star": True,
        "harami": True,
        "piercing_pattern": True,
        "dark_cloud_cover": True,
        "three_white_soldiers": True,
        "three_black_crows": True,
        "marubozu": True,
        "spinning_top": True
    }
    
    CHART_PATTERNS = {
        "head_and_shoulders": True,
        "inverse_head_and_shoulders": True,
        "double_top": True,
        "double_bottom": True,
        "triple_top": True,
        "triple_bottom": True,
        "cup_and_handle": True,
        "wedge": True,
        "rectangle": True,
        "flag": True,
        "pennant": True,
        "rounding_bottom": True,
        "rounding_top": True
    }
    
    # Telegram message template
    SIGNAL_MESSAGE_TEMPLATE = """
🔔 *TRADING SIGNAL ALERT* 🔔

*{stock_name}* \\({stock_symbol}\\)
Current Price: {current_price}
Industry: {industry}

*{signal_type} SIGNAL* {'⭐' * signal_strength}
Timeframe: {timeframe}

*KEY INDICATORS:*
{primary_indicators}

*PATTERNS DETECTED:*
{patterns}

*RISK MANAGEMENT:*
Stop Loss: {stop_loss}
Target: {target_price}
Risk:Reward Ratio: 1:2

{trend_strength}

{buy_sell_summary}

*Detailed Analysis:*
{detailed_analysis}

Signal generated at: {timestamp_short}
"""


# Custom exceptions for better error handling
class TradingBotError(Exception):
    """Base exception for all trading bot errors"""
    def __init__(self, message="Trading Bot error occurred", cause=None):
        self.cause = cause
        if cause:
            message = f"{message} caused by {type(cause).__name__}: {cause}"
        super().__init__(message)


class DataFetchError(TradingBotError):
    """Exception raised for errors when fetching data"""
    def __init__(self, symbol, message="Failed to fetch data", cause=None):
        self.symbol = symbol
        super().__init__(f"{message} for {symbol}", cause)


class PatternDetectionError(TradingBotError):
    """Exception raised for errors in pattern detection"""
    def __init__(self, pattern, message="Failed to detect pattern", cause=None):
        self.pattern = pattern
        super().__init__(f"{message} '{pattern}'", cause)


class InvalidConfigurationError(TradingBotError):
    """Exception raised for configuration errors"""
    def __init__(self, parameter, message="Invalid configuration", cause=None):
        self.parameter = parameter
        super().__init__(f"{message}: {parameter}", cause)


class APIConnectionError(TradingBotError):
    """Exception raised for API connection issues"""
    def __init__(self, api_name, message="Failed to connect to API", cause=None):
        self.api_name = api_name
        super().__init__(f"{message} {api_name}", cause)


class EmptyDataError(DataFetchError):
    """Exception raised when fetched data is empty"""
    def __init__(self, symbol, message="No data returned"):
        super().__init__(symbol, message)


def setup_logging():
    """Setup logging configuration for the trading bot"""
    global _LOGGING_INITIALIZED
    
    if _LOGGING_INITIALIZED:
        return
        
    with _INITIALIZATION_LOCK:
        if _LOGGING_INITIALIZED:
            return
            
        # Configure logging
        logging.basicConfig(
            level=logging.INFO,
            format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
            handlers=[
                logging.FileHandler("trading_bot.log"),
                logging.StreamHandler()
            ]
        )
        
        # Create logger
        logger = logging.getLogger("TradingBot")
        logger.setLevel(logging.INFO)
        
        # Log initialization
        logger.info(f"Logging initialized. Bot instance UUID: {_BOT_INSTANCE_UUID}")
        
        _LOGGING_INITIALIZED = True
        
    return logging.getLogger("TradingBot")


def escape_telegram_markdown(text):
    """Escape special characters for Telegram MarkdownV2 formatting."""
    if text is None:
        return "N/A"
    
    # Characters that need escaping in MarkdownV2
    special_chars = '_*[]()~`>#+-=|{}.!'
    
    # Escape each special character with a backslash
    result = ""
    for char in str(text):
        if char in special_chars:
            result += f"\\{char}"
        else:
            result += char
    
    return result


class UpstoxClient:
    """Client for interacting with Upstox API for trading and data fetching"""
    
    def __init__(self, config):
        """Initialize Upstox client with API credentials"""
        self.config = config
        self.logger = logging.getLogger("TradingBot.UpstoxClient")
        self.api_key = config.UPSTOX_API_KEY
        self.api_secret = config.UPSTOX_API_SECRET
        self.redirect_uri = config.UPSTOX_REDIRECT_URI
        self.code = config.UPSTOX_CODE
        self.access_token = None
        self.client = None
        
    def authenticate(self):
        """Authenticate with Upstox API"""
        try:
            # Initialize API client
            api_client = ApiClient()
            login_api = LoginApi(api_client)
            
            # Get authorization URL
            client_id = self.api_key
            redirect_uri = self.redirect_uri
            api_version = "v2"
            
            auth_url = login_api.authorize(client_id, redirect_uri, api_version)
            self.logger.info(f"Authorization URL: {auth_url}")
            
            # Set access token from config code
            api_client.configuration.access_token = self.code
            self.client = MarketQuoteApi(api_client)
            
            # Test the connection
            try:
                profile = self.client.get_profile()
                self.logger.info(f"Authentication successful for user: {profile['data']['user_name']}")
                return True
            except Exception as e:
                self.logger.error(f"Failed to validate access token: {str(e)}")
                # If token is invalid, try to refresh it
                return self._refresh_token()
            
        except Exception as e:
            self.logger.error(f"Authentication error: {str(e)}")
            raise APIConnectionError("Upstox", "Failed to authenticate with", e)
    
    def _refresh_token(self):
        """Refresh the access token if needed"""
        try:
            # Generate and set access token using authorization code
            url = "https://api.upstox.com/v2/login/authorization/token"
            headers = {
                'accept': 'application/json',
                'Api-Version': '2.0',
                'Content-Type': 'application/x-www-form-urlencoded'
            }
            data = {
                'code': self.code,
                'client_id': self.api_key,
                'client_secret': self.api_secret,
                'redirect_uri': self.redirect_uri,
                'grant_type': 'authorization_code'
            }
            
            response = requests.post(url, headers=headers, data=data)
            response_data = json.loads(response.text)
            
            if 'access_token' not in response_data:
                self.logger.error(f"Authentication failed: {response.text}")
                return False
            
            # Store access token
            self.access_token = response_data['access_token']
            
            # Create client with access token
            api_client = ApiClient()
            api_client.configuration.access_token = self.access_token
            self.client = MarketQuoteApi(api_client)
            
            self.logger.info("Authentication refreshed successfully")
            return True
            
        except Exception as e:
            self.logger.error(f"Token refresh error: {str(e)}")
            raise APIConnectionError("Upstox", "Failed to refresh token for", e)
    
    def get_historical_data(self, instrument_key, interval, from_date, to_date):
        """
        Get historical OHLCV data from Upstox
        
        Args:
            instrument_key: Instrument identifier
            interval: Time interval (1D, 1W, etc.)
            from_date: Start date (YYYY-MM-DD)
            to_date: End date (YYYY-MM-DD)
        
        Returns:
            DataFrame with OHLCV data
        """
        try:
            # Convert dates to epoch
            from_epoch = int(time.mktime(datetime.datetime.strptime(from_date, "%Y-%m-%d").timetuple()))
            to_epoch = int(time.mktime(datetime.datetime.strptime(to_date, "%Y-%m-%d").timetuple()))
            
            # Make API request
            historical_data = self.client.historical_candle_data(
                instrument_key=instrument_key,
                interval=interval,
                to_date=to_epoch,
                from_date=from_epoch
            )
            
            # Extract candle data
            candles = historical_data['data']['candles']
            
            # Create DataFrame
            df = pd.DataFrame(candles, columns=['timestamp', 'open', 'high', 'low', 'close', 'volume'])
            df['timestamp'] = pd.to_datetime(df['timestamp'], unit='s')
            df.set_index('timestamp', inplace=True)
            
            # Check if data is empty
            if len(df) == 0:
                raise EmptyDataError(instrument_key)
                
            return df
            
        except Exception as e:
            self.logger.error(f"Error getting historical data: {str(e)}")
            raise DataFetchError(instrument_key, "Failed to fetch historical data", e)

    def get_instrument_details(self, instrument_key):
        """Get instrument details from Upstox"""
        try:
            # Get market quote for the instrument
            market_quote = self.client.get_market_quote_full(instrument_key)
            
            # Extract basic instrument details from response
            instrument_details = {
                'name': market_quote['data']['company_name'],
                'tradingsymbol': market_quote['data']['symbol'],
                'exchange': market_quote['data']['exchange'],
                'last_price': market_quote['data']['last_price'],
                'change': market_quote['data'].get('net_change', 0),
                'change_percent': market_quote['data'].get('net_change_percentage', 0)
            }
            
            return instrument_details
            
        except Exception as e:
            self.logger.error(f"Error getting instrument details: {str(e)}")
            raise DataFetchError(instrument_key, "Failed to fetch instrument details", e)


class CandlestickPatterns:
    """Complete candlestick pattern detection with precise validation criteria"""
    
    def __init__(self, df, params=None):
        """
        Initialize with OHLCV DataFrame and parameters
        
        Args:
            df: DataFrame with OHLCV data (index=timestamp, columns=[open, high, low, close, volume])
            params: Dictionary of parameters for pattern detection
        """
        self.df = df.copy()
        self.params = params or {}
        self.patterns = {}
        
        # Calculate candle dimensions
        self._calculate_candle_dimensions()
        
        # Calculate trend context
        self._calculate_trend_context()
    
    def _calculate_candle_dimensions(self):
        """Calculate candle body and shadow dimensions for pattern detection"""
        # Body and range calculations
        self.df['body'] = abs(self.df['close'] - self.df['open'])
        self.df['range'] = self.df['high'] - self.df['low']
        
        # Upper and lower shadows
        self.df['upper_shadow'] = self.df.apply(
            lambda x: x['high'] - max(x['open'], x['close']), axis=1
        )
        self.df['lower_shadow'] = self.df.apply(
            lambda x: min(x['open'], x['close']) - x['low'], axis=1
        )
        
        # Body percentage of the range
        self.df['body_pct'] = self.df['body'] / self.df['range']
        
        # Determine if candle is bullish or bearish
        self.df['bullish'] = self.df['close'] > self.df['open']
    
    def _calculate_trend_context(self):
        """Calculate trend context for improved pattern validation"""
        # Calculate short-term trend (5-day SMA direction)
        self.df['sma5'] = self.df['close'].rolling(window=5).mean()
        self.df['trend_short'] = 0
        self.df.loc[self.df['sma5'] > self.df['sma5'].shift(1), 'trend_short'] = 1
        self.df.loc[self.df['sma5'] < self.df['sma5'].shift(1), 'trend_short'] = -1
        
        # Calculate medium-term trend (20-day SMA direction)
        self.df['sma20'] = self.df['close'].rolling(window=20).mean()
        self.df['trend_medium'] = 0
        self.df.loc[self.df['sma20'] > self.df['sma20'].shift(1), 'trend_medium'] = 1
        self.df.loc[self.df['sma20'] < self.df['sma20'].shift(1), 'trend_medium'] = -1
    
    def detect_marubozu(self):
        """Detect Marubozu candlestick pattern"""
        # A marubozu has almost no upper and lower shadows
        threshold = self.params.get('marubozu_threshold', 0.05)
        
        bullish_marubozu = (
            self.df['bullish'] &
            (self.df['upper_shadow'] / self.df['range'] < threshold) &
            (self.df['lower_shadow'] / self.df['range'] < threshold) &
            (self.df['body_pct'] > 0.95)
        )
        
        bearish_marubozu = (
            ~self.df['bullish'] &
            (self.df['upper_shadow'] / self.df['range'] < threshold) &
            (self.df['lower_shadow'] / self.df['range'] < threshold) &
            (self.df['body_pct'] > 0.95)
        )
        
        self.df['bullish_marubozu'] = bullish_marubozu
        self.df['bearish_marubozu'] = bearish_marubozu
        
        # Store latest results
        if bullish_marubozu.iloc[-1]:
            self.patterns['bullish_marubozu'] = {
                'signal': 1,
                'strength': 3
            }
        elif bearish_marubozu.iloc[-1]:
            self.patterns['bearish_marubozu'] = {
                'signal': -1,
                'strength': 3
            }
    
    def detect_doji(self):
        """Detect Doji candlestick pattern"""
        # A doji has a very small body compared to the range
        threshold = self.params.get('doji_threshold', 0.05)
        
        doji = (self.df['body_pct'] < threshold)
        
        # Specific types of doji
        # Dragonfly doji: long lower shadow, almost no upper shadow
        dragonfly_doji = (
            doji &
            (self.df['lower_shadow'] > 0.6 * self.df['range']) &
            (self.df['upper_shadow'] < 0.1 * self.df['range'])
        )
        
        # Gravestone doji: long upper shadow, almost no lower shadow
        gravestone_doji = (
            doji &
            (self.df['upper_shadow'] > 0.6 * self.df['range']) &
            (self.df['lower_shadow'] < 0.1 * self.df['range'])
        )
        
        # Long-legged doji: significant upper and lower shadows
        long_legged_doji = (
            doji &
            (self.df['upper_shadow'] > 0.3 * self.df['range']) &
            (self.df['lower_shadow'] > 0.3 * self.df['range'])
        )
        
        self.df['doji'] = doji
        self.df['dragonfly_doji'] = dragonfly_doji
        self.df['gravestone_doji'] = gravestone_doji
        self.df['long_legged_doji'] = long_legged_doji
        
        # Store latest results
        # Doji signals depend on the preceding trend
        if doji.iloc[-1]:
            # Basic doji in a downtrend is potentially bullish
            if self.df['trend_short'].iloc[-1] < 0:
                self.patterns['doji'] = {
                    'signal': 1,
                    'strength': 2,
                    'type': 'basic'
                }
            # Basic doji in an uptrend is potentially bearish
            elif self.df['trend_short'].iloc[-1] > 0:
                self.patterns['doji'] = {
                    'signal': -1,
                    'strength': 2,
                    'type': 'basic'
                }
        
        if dragonfly_doji.iloc[-1]:
            # Dragonfly doji is bullish, especially in a downtrend
            self.patterns['dragonfly_doji'] = {
                'signal': 1,
                'strength': 3
            }
        
        if gravestone_doji.iloc[-1]:
            # Gravestone doji is bearish, especially in an uptrend
            self.patterns['gravestone_doji'] = {
                'signal': -1,
                'strength': 3
            }
        
        if long_legged_doji.iloc[-1]:
            # Long-legged doji indicates indecision
            # Signal depends on the prevailing trend
            if self.df['trend_short'].iloc[-1] < 0:
                self.patterns['long_legged_doji'] = {
                    'signal': 1,
                    'strength': 2
                }
            elif self.df['trend_short'].iloc[-1] > 0:
                self.patterns['long_legged_doji'] = {
                    'signal': -1,
                    'strength': 2
                }
    
    def detect_spinning_tops(self):
        """Detect Spinning Top candlestick pattern"""
        # A spinning top has a small body with upper and lower shadows longer than the body
        body_threshold = self.params.get('spinning_top_body_threshold', 0.3)
        shadow_threshold = self.params.get('spinning_top_shadow_threshold', 0.3)
        
        spinning_top = (
            (self.df['body_pct'] < body_threshold) &
            (self.df['upper_shadow'] > self.df['body']) &
            (self.df['lower_shadow'] > self.df['body']) &
            (self.df['upper_shadow'] > shadow_threshold * self.df['range']) &
            (self.df['lower_shadow'] > shadow_threshold * self.df['range'])
        )
        
        self.df['spinning_top'] = spinning_top
        
        # Store latest results
        if spinning_top.iloc[-1]:
            # Spinning top signals depend on the preceding trend
            if self.df['trend_short'].iloc[-1] < 0:
                self.patterns['spinning_top'] = {
                    'signal': 1,
                    'strength': 1  # Weaker signal
                }
            elif self.df['trend_short'].iloc[-1] > 0:
                self.patterns['spinning_top'] = {
                    'signal': -1,
                    'strength': 1  # Weaker signal
                }
    
    def detect_paper_umbrella(self):
        """Detect Paper Umbrella (Hammer or Hanging Man) patterns"""
        # A paper umbrella has a small body at the top with a long lower shadow
        # and little to no upper shadow
        body_threshold = self.params.get('paper_umbrella_body_threshold', 0.3)
        lower_shadow_threshold = self.params.get('paper_umbrella_lower_shadow_threshold', 0.6)
        upper_shadow_threshold = self.params.get('paper_umbrella_upper_shadow_threshold', 0.1)
        
        paper_umbrella = (
            (self.df['body_pct'] < body_threshold) &
            (self.df['lower_shadow'] > lower_shadow_threshold * self.df['range']) &
            (self.df['upper_shadow'] < upper_shadow_threshold * self.df['range'])
        )
        
        self.df['paper_umbrella'] = paper_umbrella
        
        # Hammer vs Hanging Man is determined by the trend context
        self.df['hammer'] = paper_umbrella & (self.df['trend_short'] < 0)
        self.df['hanging_man'] = paper_umbrella & (self.df['trend_short'] > 0)
        
        # Store latest results
        if self.df['hammer'].iloc[-1]:
            self.patterns['hammer'] = {
                'signal': 1,
                'strength': 3
            }
        
        if self.df['hanging_man'].iloc[-1]:
            self.patterns['hanging_man'] = {
                'signal': -1,
                'strength': 3
            }
    
    def detect_shooting_star(self):
        """Detect Shooting Star candlestick pattern"""
        # A shooting star has a small body at the bottom with a long upper shadow
        # and little to no lower shadow
        body_threshold = self.params.get('shooting_star_body_threshold', 0.3)
        upper_shadow_threshold = self.params.get('shooting_star_upper_shadow_threshold', 0.6)
        lower_shadow_threshold = self.params.get('shooting_star_lower_shadow_threshold', 0.1)
        
        shooting_star = (
            (self.df['body_pct'] < body_threshold) &
            (self.df['upper_shadow'] > upper_shadow_threshold * self.df['range']) &
            (self.df['lower_shadow'] < lower_shadow_threshold * self.df['range']) &
            (self.df['trend_short'] > 0)  # Should appear in an uptrend
        )
        
        self.df['shooting_star'] = shooting_star
        
        # Store latest results
        if shooting_star.iloc[-1]:
            self.patterns['shooting_star'] = {
                'signal': -1,
                'strength': 3
            }
    
    def detect_engulfing(self):
        """Detect Bullish and Bearish Engulfing patterns"""
        # Bullish Engulfing: current candle is bullish and completely engulfs previous bearish candle
        bullish_engulfing = (
            self.df['bullish'] &  # Current candle is bullish
            (~self.df['bullish'].shift(1)) &  # Previous candle is bearish
            (self.df['open'] <= self.df['close'].shift(1)) &  # Current open is <= previous close
            (self.df['close'] >= self.df['open'].shift(1))  # Current close is >= previous open
        )
        
        # Bearish Engulfing: current candle is bearish and completely engulfs previous bullish candle
        bearish_engulfing = (
            (~self.df['bullish']) &  # Current candle is bearish
            (self.df['bullish'].shift(1)) &  # Previous candle is bullish
            (self.df['open'] >= self.df['close'].shift(1)) &  # Current open is >= previous close
            (self.df['close'] <= self.df['open'].shift(1))  # Current close is <= previous open
        )
        
        self.df['bullish_engulfing'] = bullish_engulfing
        self.df['bearish_engulfing'] = bearish_engulfing
        
        # Store latest results
        if bullish_engulfing.iloc[-1]:
            self.patterns['bullish_engulfing'] = {
                'signal': 1,
                'strength': 4
            }
        
        if bearish_engulfing.iloc[-1]:
            self.patterns['bearish_engulfing'] = {
                'signal': -1,
                'strength': 4
            }
    
    def detect_harami(self):
        """Detect Bullish and Bearish Harami patterns"""
        # Bullish Harami: small bullish candle contained within previous large bearish candle
        bullish_harami = (
            self.df['bullish'] &  # Current candle is bullish
            (~self.df['bullish'].shift(1)) &  # Previous candle is bearish
            (self.df['open'] > self.df['close'].shift(1)) &  # Current open is > previous close
            (self.df['close'] < self.df['open'].shift(1)) &  # Current close is < previous open
            (self.df['body'].shift(1) > 1.5 * self.df['body'])  # Previous body is significantly larger
        )
        
        # Bearish Harami: small bearish candle contained within previous large bullish candle
        bearish_harami = (
            (~self.df['bullish']) &  # Current candle is bearish
            (self.df['bullish'].shift(1)) &  # Previous candle is bullish
            (self.df['open'] < self.df['close'].shift(1)) &  # Current open is < previous close
            (self.df['close'] > self.df['open'].shift(1)) &  # Current close is > previous open
            (self.df['body'].shift(1) > 1.5 * self.df['body'])  # Previous body is significantly larger
        )
        
        self.df['bullish_harami'] = bullish_harami
        self.df['bearish_harami'] = bearish_harami
        
        # Store latest results
        if bullish_harami.iloc[-1]:
            self.patterns['bullish_harami'] = {
                'signal': 1,
                'strength': 3
            }
        
        if bearish_harami.iloc[-1]:
            self.patterns['bearish_harami'] = {
                'signal': -1,
                'strength': 3
            }
    
    def detect_piercing_pattern(self):
        """Detect Piercing Pattern (bullish reversal)"""
        piercing_pattern = (
            (~self.df['bullish'].shift(1)) &  # Previous candle is bearish
            (self.df['bullish']) &  # Current candle is bullish
            (self.df['open'] < self.df['low'].shift(1)) &  # Current open below previous low
            (self.df['close'] > (self.df['open'].shift(1) + self.df['close'].shift(1)) / 2) &  # Close above midpoint
            (self.df['close'] < self.df['open'].shift(1))  # Close below previous open
        )
        
        self.df['piercing_pattern'] = piercing_pattern
        
        # Store latest results
        if piercing_pattern.iloc[-1]:
            self.patterns['piercing_pattern'] = {
                'signal': 1,
                'strength': 3
            }
    
    def detect_dark_cloud_cover(self):
        """Detect Dark Cloud Cover (bearish reversal)"""
        dark_cloud_cover = (
            (self.df['bullish'].shift(1)) &  # Previous candle is bullish
            (~self.df['bullish']) &  # Current candle is bearish
            (self.df['open'] > self.df['high'].shift(1)) &  # Current open above previous high
            (self.df['close'] < (self.df['open'].shift(1) + self.df['close'].shift(1)) / 2) &  # Close below midpoint
            (self.df['close'] > self.df['open'].shift(1))  # Close above previous open
        )
        
        self.df['dark_cloud_cover'] = dark_cloud_cover
        
        # Store latest results
        if dark_cloud_cover.iloc[-1]:
            self.patterns['dark_cloud_cover'] = {
                'signal': -1,
                'strength': 3
            }
    
    def detect_morning_star(self):
        """Detect Morning Star (bullish reversal)"""
        # First day: bearish candle
        # Second day: small body (doji or spinning top) with gap down
        # Third day: bullish candle that closes above midpoint of first day
        
        morning_star = (
            (~self.df['bullish'].shift(2)) &  # First day bearish
            (self.df['body_pct'].shift(1) < 0.3) &  # Second day small body
            (self.df['high'].shift(1) < self.df['close'].shift(2)) &  # Gap down
            (self.df['bullish']) &  # Third day bullish
            (self.df['close'] > (self.df['open'].shift(2) + self.df['close'].shift(2)) / 2)  # Close above midpoint
        )
        
        self.df['morning_star'] = morning_star
        
        # Store latest results
        if morning_star.iloc[-1]:
            self.patterns['morning_star'] = {
                'signal': 1,
                'strength': 4
            }
    
    def detect_evening_star(self):
        """Detect Evening Star (bearish reversal)"""
        # First day: bullish candle
        # Second day: small body (doji or spinning top) with gap up
        # Third day: bearish candle that closes below midpoint of first day
        
        evening_star = (
            (self.df['bullish'].shift(2)) &  # First day bullish
            (self.df['body_pct'].shift(1) < 0.3) &  # Second day small body
            (self.df['low'].shift(1) > self.df['close'].shift(2)) &  # Gap up
            (~self.df['bullish']) &  # Third day bearish
            (self.df['close'] < (self.df['open'].shift(2) + self.df['close'].shift(2)) / 2)  # Close below midpoint
        )
        
        self.df['evening_star'] = evening_star
        
        # Store latest results
        if evening_star.iloc[-1]:
            self.patterns['evening_star'] = {
                'signal': -1,
                'strength': 4
            }
    
    def detect_three_white_soldiers(self):
        """Detect Three White Soldiers (bullish continuation/reversal)"""
        # Three consecutive bullish candles, each closing higher than the previous
        # Each candle opens within the previous candle's body
        # Each candle closes near its high
        
        three_white_soldiers = (
            (self.df['bullish']) &  # Current day bullish
            (self.df['bullish'].shift(1)) &  # Previous day bullish
            (self.df['bullish'].shift(2)) &  # Day before previous bullish
            (self.df['close'] > self.df['close'].shift(1)) &  # Current close higher than previous
            (self.df['close'].shift(1) > self.df['close'].shift(2)) &  # Previous close higher than before
            (self.df['open'] > self.df['open'].shift(1)) &  # Current open higher than previous
            (self.df['open'].shift(1) > self.df['open'].shift(2)) &  # Previous open higher than before
            (self.df['open'] < self.df['close'].shift(1)) &  # Current open within previous body
            (self.df['open'].shift(1) < self.df['close'].shift(2)) &  # Previous open within the body before
            ((self.df['high'] - self.df['close']) < 0.2 * self.df['body']) &  # Close near high (current)
            ((self.df['high'].shift(1) - self.df['close'].shift(1)) < 0.2 * self.df['body'].shift(1)) &  # Previous
            ((self.df['high'].shift(2) - self.df['close'].shift(2)) < 0.2 * self.df['body'].shift(2))  # Before
        )
        
        self.df['three_white_soldiers'] = three_white_soldiers
        
        # Store latest results
        if three_white_soldiers.iloc[-1]:
            self.patterns['three_white_soldiers'] = {
                'signal': 1,
                'strength': 5
            }
    
    def detect_three_black_crows(self):
        """Detect Three Black Crows (bearish continuation/reversal)"""
        # Three consecutive bearish candles, each closing lower than the previous
        # Each candle opens within the previous candle's body
        # Each candle closes near its low
        
        three_black_crows = (
            (~self.df['bullish']) &  # Current day bearish
            (~self.df['bullish'].shift(1)) &  # Previous day bearish
            (~self.df['bullish'].shift(2)) &  # Day before previous bearish
            (self.df['close'] < self.df['close'].shift(1)) &  # Current close lower than previous
            (self.df['close'].shift(1) < self.df['close'].shift(2)) &  # Previous close lower than before
            (self.df['open'] < self.df['open'].shift(1)) &  # Current open lower than previous
            (self.df['open'].shift(1) < self.df['open'].shift(2)) &  # Previous open lower than before
            (self.df['open'] > self.df['close'].shift(1)) &  # Current open within previous body
            (self.df['open'].shift(1) > self.df['close'].shift(2)) &  # Previous open within the body before
            ((self.df['close'] - self.df['low']) < 0.2 * self.df['body']) &  # Close near low (current)
            ((self.df['close'].shift(1) - self.df['low'].shift(1)) < 0.2 * self.df['body'].shift(1)) &  # Previous
            ((self.df['close'].shift(2) - self.df['low'].shift(2)) < 0.2 * self.df['body'].shift(2))  # Before
        )
        
        self.df['three_black_crows'] = three_black_crows
        
        # Store latest results
        if three_black_crows.iloc[-1]:
            self.patterns['three_black_crows'] = {
                'signal': -1,
                'strength': 5
            }
    
    def detect_all_patterns(self):
        """Detect all candlestick patterns"""
        self.detect_marubozu()
        self.detect_doji()
        self.detect_spinning_tops()
        self.detect_paper_umbrella()
        self.detect_shooting_star()
        self.detect_engulfing()
        self.detect_harami()
        self.detect_piercing_pattern()
        self.detect_dark_cloud_cover()
        self.detect_morning_star()
        self.detect_evening_star()
        self.detect_three_white_soldiers()
        self.detect_three_black_crows()
        
        return self.patterns
    
    def get_latest_patterns(self):
        """Get the latest detected patterns"""
        if not self.patterns:
            self.detect_all_patterns()
        
        return self.patterns
    
    def get_pattern_signals(self):
        """Get trading signals from detected patterns"""
        if not self.patterns:
            self.detect_all_patterns()
        
        buy_patterns = []
        sell_patterns = []
        
        for pattern_name, pattern_data in self.patterns.items():
            if pattern_data['signal'] == 1:
                buy_patterns.append({
                    'pattern': pattern_name.replace('_', ' ').title(),
                    'strength': pattern_data['strength']
                })
            elif pattern_data['signal'] == -1:
                sell_patterns.append({
                    'pattern': pattern_name.replace('_', ' ').title(),
                    'strength': pattern_data['strength']
                })
        
        return {
            'buy': buy_patterns,
            'sell': sell_patterns
        }


class TechnicalIndicators:
    """Calculate and interpret technical indicators for trading signals"""
    
    def __init__(self, df, params=None):
        """
        Initialize Technical Analysis with OHLCV DataFrame
        
        Args:
            df: DataFrame with OHLCV data (index=timestamp, columns=[open, high, low, close, volume])
            params: Dictionary of parameters for indicator calculation
        """
        # Ensure column names are lowercase
        self.df = df.copy()
        self.df.columns = [col.lower() for col in self.df.columns]
        
        # Ensure we have the required columns
        required_columns = ['open', 'high', 'low', 'close', 'volume']
        for col in required_columns:
            if col not in self.df.columns:
                raise ValueError(f"DataFrame must contain {col} column")
        
        # Set parameters
        self.params = params or {}
        
        # Initialize results dictionary
        self.indicators = {}
    
    def calculate_all(self):
        """Calculate all technical indicators"""
        self.calculate_moving_averages()
        self.calculate_macd()
        self.calculate_rsi()
        self.calculate_stochastic()
        self.calculate_bollinger_bands()
        self.calculate_supertrend()
        self.calculate_parabolic_sar()
        self.calculate_atr()
        self.calculate_adx()
        self.calculate_obv()
        self.calculate_vwap()
        self.calculate_aroon()
        self.calculate_stochastic_rsi()
        self.calculate_fibonacci_retracement()
        
        # Add the new indicators here
        self.calculate_volume_ratio()
        self.calculate_atr_bands()
        self.calculate_alligator()
        self.calculate_cpr()
        self.calculate_reward_risk_ratio()
        
        return self.indicators
        
    def calculate_volume_ratio(self):
        """
        Calculate volume relative to moving averages
        
        Returns:
            Dictionary with volume ratio values
        """
        # Skip if Volume data is not available
        if 'Volume' not in self.df.columns:
            return {}
            
        # Define periods for volume moving averages
        periods = [10, 20, 50]
        high_volume_threshold = 1.5
        low_volume_threshold = 0.5
            
        # Calculate volume moving averages
        for period in periods:
            self.df[f'volume_ma{period}'] = self.df['Volume'].rolling(window=period).mean()
            
        # Calculate volume ratio (current volume / average volume)
        for period in periods:
            # Vectorized calculation with handling for zero values
            self.df[f'volume_ratio_{period}'] = np.where(
                self.df[f'volume_ma{period}'] > 0,
                self.df['Volume'] / self.df[f'volume_ma{period}'],
                0
            )
            
        # Flag high and low volume (based on 20-period MA)
        self.df['high_volume'] = self.df['Volume'] > (self.df['volume_ma20'] * high_volume_threshold)
        self.df['low_volume'] = self.df['Volume'] < (self.df['volume_ma20'] * low_volume_threshold)
        
        # Volume increasing or decreasing (5-day volume trend)
        self.df['volume_ma5'] = self.df['Volume'].rolling(window=5).mean()
        self.df['volume_increasing'] = self.df['volume_ma5'] > self.df['volume_ma5'].shift(3)
        
        # Price/volume confirmation
        self.df['price_up_volume_up'] = (self.df['Close'] > self.df['Close'].shift(1)) & self.df['high_volume']
        self.df['price_down_volume_up'] = (self.df['Close'] < self.df['Close'].shift(1)) & self.df['high_volume']
        
        # Save to results
        self.indicators['volume_analysis'] = {
            'signal': 1 if self.df['price_up_volume_up'].iloc[-1] else 
                    -1 if self.df['price_down_volume_up'].iloc[-1] else 0,
            'values': {
                'volume_ratio_20': round(self.df['volume_ratio_20'].iloc[-1], 2) if not pd.isna(self.df['volume_ratio_20'].iloc[-1]) else None,
                'high_volume': self.df['high_volume'].iloc[-1],
                'volume_increasing': self.df['volume_increasing'].iloc[-1],
                'price_volume_confirmed': self.df['price_up_volume_up'].iloc[-1] or self.df['price_down_volume_up'].iloc[-1]
            }
        }
        
        return {
            'volume_ratio_20': self.df['volume_ratio_20'],
            'high_volume': self.df['high_volume'],
            'volume_increasing': self.df['volume_increasing']
        }
        
    def calculate_reward_risk_ratio(self):
        """
        Calculate reward to risk ratio based on ATR for dynamic targets and stops
        
        Returns:
            Dictionary with RRR values for long and short trades
        """
        # Get parameters
        target_multiplier = 1.5  # Default target multiplier
        stop_multiplier = 1.0    # Default stop multiplier
        min_rrr = 1.5           # Default minimum RRR
        
        # Ensure ATR is calculated
        if 'atr' not in self.df.columns:
            self.calculate_atr()
            
        # Calculate potential stops and targets
        self.df['long_entry'] = self.df['Close']
        self.df['long_stop'] = self.df['Close'] - (self.df['atr'] * stop_multiplier)
        self.df['long_target'] = self.df['Close'] + (self.df['atr'] * target_multiplier)
        
        self.df['short_entry'] = self.df['Close']
        self.df['short_stop'] = self.df['Close'] + (self.df['atr'] * stop_multiplier)
        self.df['short_target'] = self.df['Close'] - (self.df['atr'] * target_multiplier)
        
        # Calculate reward:risk ratios (handle division by zero)
        long_risk = self.df['long_entry'] - self.df['long_stop']
        short_risk = self.df['short_stop'] - self.df['short_entry']
        
        self.df['long_rrr'] = np.where(
            long_risk > 0,
            (self.df['long_target'] - self.df['long_entry']) / long_risk,
            0
        )
        
        self.df['short_rrr'] = np.where(
            short_risk > 0,
            (self.df['short_entry'] - self.df['short_target']) / short_risk,
            0
        )
        
        # Check if RRR meets minimum threshold
        self.df['long_rrr_valid'] = self.df['long_rrr'] >= min_rrr
        self.df['short_rrr_valid'] = self.df['short_rrr'] >= min_rrr
        
        # Save to results
        self.indicators['reward_risk'] = {
            'signal': 0,  # RRR doesn't generate direct signals
            'values': {
                'long_rrr': round(self.df['long_rrr'].iloc[-1], 2),
                'short_rrr': round(self.df['short_rrr'].iloc[-1], 2),
                'long_rrr_valid': self.df['long_rrr_valid'].iloc[-1],
                'short_rrr_valid': self.df['short_rrr_valid'].iloc[-1],
                'long_stop': round(self.df['long_stop'].iloc[-1], 2),
                'long_target': round(self.df['long_target'].iloc[-1], 2),
                'short_stop': round(self.df['short_stop'].iloc[-1], 2),
                'short_target': round(self.df['short_target'].iloc[-1], 2)
            }
        }
        
        return {
            'long_rrr': self.df['long_rrr'],
            'short_rrr': self.df['short_rrr'],
            'long_rrr_valid': self.df['long_rrr_valid'],
            'short_rrr_valid': self.df['short_rrr_valid']
        }
        
    def calculate_atr_bands(self):
        """
        Calculate ATR Bands using vectorized operations
        
        Returns:
            Dictionary with upper and lower ATR bands
        """
        # Get parameters from config
        period = self.params.get('atr', {}).get('period', 14)
        multiplier = self.params.get('atr', {}).get('multiplier', 2)
        
        # Ensure ATR is calculated
        if 'atr' not in self.df.columns:
            self.calculate_atr()
        
        # Calculate moving average
        self.df['atr_ma'] = self.df['Close'].rolling(window=period).mean()
        
        # Calculate ATR bands
        self.df['atr_upper'] = self.df['atr_ma'] + (self.df['atr'] * multiplier)
        self.df['atr_lower'] = self.df['atr_ma'] - (self.df['atr'] * multiplier)
        
        # Generate signals
        self.df['atr_upper_break'] = self.df['Close'] > self.df['atr_upper']
        self.df['atr_lower_break'] = self.df['Close'] < self.df['atr_lower']
        
        # ATR band penetration signals
        self.df['atr_band_buy'] = (
            (self.df['Close'] > self.df['atr_upper']) & 
            (self.df['Close'].shift(1) <= self.df['atr_upper'].shift(1))
        )
        
        self.df['atr_band_sell'] = (
            (self.df['Close'] < self.df['atr_lower']) & 
            (self.df['Close'].shift(1) >= self.df['atr_lower'].shift(1))
        )
        
        # Save to results
        self.indicators['atr_bands'] = {
            'signal': 1 if self.df['atr_band_buy'].iloc[-1] else 
                    -1 if self.df['atr_band_sell'].iloc[-1] else 0,
            'values': {
                'atr_upper': round(self.df['atr_upper'].iloc[-1], 2),
                'atr_lower': round(self.df['atr_lower'].iloc[-1], 2),
                'atr_ma': round(self.df['atr_ma'].iloc[-1], 2),
                'upper_break': self.df['atr_upper_break'].iloc[-1],
                'lower_break': self.df['atr_lower_break'].iloc[-1]
            }
        }
        
        return {
            'atr_upper': self.df['atr_upper'],
            'atr_lower': self.df['atr_lower']
        }
        
    def calculate_alligator(self):
        """
        Calculate Alligator indicator using vectorized operations
        
        Returns:
            Dictionary with jaw, teeth, and lips lines
        """
        # Default values for alligator periods
        jaw = 13
        teeth = 8
        lips = 5
        
        # Calculate the median price
        self.df['median_price'] = (self.df['High'] + self.df['Low']) / 2
        
        # Calculate the three lines
        self.df['alligator_jaw'] = self.df['median_price'].rolling(window=jaw).mean().shift(8)
        self.df['alligator_teeth'] = self.df['median_price'].rolling(window=teeth).mean().shift(5)
        self.df['alligator_lips'] = self.df['median_price'].rolling(window=lips).mean().shift(3)
        
        # Determine if Alligator is sleeping (lines are intertwined)
        max_line = self.df[['alligator_jaw', 'alligator_teeth', 'alligator_lips']].max(axis=1)
        min_line = self.df[['alligator_jaw', 'alligator_teeth', 'alligator_lips']].min(axis=1)
        
        # If the difference between max and min is small, Alligator is sleeping
        self.df['alligator_sleeping'] = (max_line - min_line) < (self.df['Close'] * 0.01)  # 1% of price
        
        # Determine buy/sell signal
        self.df['alligator_buy'] = (
            ~self.df['alligator_sleeping'] &
            (self.df['Close'] > self.df['alligator_lips']) &
            (self.df['alligator_lips'] > self.df['alligator_teeth']) &
            (self.df['alligator_teeth'] > self.df['alligator_jaw'])
        )
        
        self.df['alligator_sell'] = (
            ~self.df['alligator_sleeping'] &
            (self.df['Close'] < self.df['alligator_lips']) &
            (self.df['alligator_lips'] < self.df['alligator_teeth']) &
            (self.df['alligator_teeth'] < self.df['alligator_jaw'])
        )
        
        # Define the feeding phase
        self.df['alligator_feeding'] = (
            ~self.df['alligator_sleeping'] &
            (
                (self.df['alligator_buy'] & (self.df['Close'] > self.df['Close'].shift(1))) |
                (self.df['alligator_sell'] & (self.df['Close'] < self.df['Close'].shift(1)))
            )
        )
        
        # Save to results
        self.indicators['alligator'] = {
            'signal': 1 if self.df['alligator_buy'].iloc[-1] else 
                    -1 if self.df['alligator_sell'].iloc[-1] else 0,
            'values': {
                'jaw': round(self.df['alligator_jaw'].iloc[-1], 2) if not pd.isna(self.df['alligator_jaw'].iloc[-1]) else None,
                'teeth': round(self.df['alligator_teeth'].iloc[-1], 2) if not pd.isna(self.df['alligator_teeth'].iloc[-1]) else None,
                'lips': round(self.df['alligator_lips'].iloc[-1], 2) if not pd.isna(self.df['alligator_lips'].iloc[-1]) else None,
                'sleeping': self.df['alligator_sleeping'].iloc[-1],
                'feeding': self.df['alligator_feeding'].iloc[-1]
            }
        }
        
        return {
            'jaw': self.df['alligator_jaw'],
            'teeth': self.df['alligator_teeth'],
            'lips': self.df['alligator_lips']
        }
    
    def calculate_cpr(self):
        """
        Calculate Central Pivot Range
        
        Returns:
            Dictionary with pivot, TC, and BC values
        """
        # Calculate the previous day's data
        self.df['prev_high'] = self.df['High'].shift(1)
        self.df['prev_low'] = self.df['Low'].shift(1)
        self.df['prev_close'] = self.df['Close'].shift(1)
        
        # Calculate pivot points
        self.df['pivot'] = (self.df['prev_high'] + self.df['prev_low'] + self.df['prev_close']) / 3
        self.df['bc'] = (self.df['prev_high'] + self.df['prev_low']) / 2
        self.df['tc'] = (self.df['pivot'] - self.df['bc']) + self.df['pivot']
        
        # Calculate traditional support and resistance levels
        self.df['r1'] = (2 * self.df['pivot']) - self.df['prev_low']
        self.df['s1'] = (2 * self.df['pivot']) - self.df['prev_high']
        self.df['r2'] = self.df['pivot'] + (self.df['prev_high'] - self.df['prev_low'])
        self.df['s2'] = self.df['pivot'] - (self.df['prev_high'] - self.df['prev_low'])
        
        # Calculate CPR width (indication of volatility/range)
        self.df['cpr_width'] = self.df['tc'] - self.df['bc']
        
        # Handle division by zero for percentage calculation
        self.df['cpr_width_pct'] = np.where(
            self.df['pivot'] > 0,
            100 * self.df['cpr_width'] / self.df['pivot'],
            0  # Default to 0 when pivot is zero
        )
        
        # Price position relative to CPR
        self.df['above_cpr'] = self.df['Close'] > self.df['tc']
        self.df['below_cpr'] = self.df['Close'] < self.df['bc']
        self.df['inside_cpr'] = (self.df['Close'] >= self.df['bc']) & (self.df['Close'] <= self.df['tc'])
        
        # CPR breakout signals
        self.df['cpr_breakout_up'] = (
            (self.df['Close'] > self.df['tc']) & 
            (self.df['Close'].shift(1) <= self.df['tc'].shift(1))
        )
        
        self.df['cpr_breakout_down'] = (
            (self.df['Close'] < self.df['bc']) & 
            (self.df['Close'].shift(1) >= self.df['bc'].shift(1))
        )
        
        self.indicators['cpr'] = {
            'signal': 1 if self.df['cpr_breakout_up'].iloc[-1] else 
                    -1 if self.df['cpr_breakout_down'].iloc[-1] else 0,
            'values': {
                'pivot': round(self.df['pivot'].iloc[-1], 2),
                'bc': round(self.df['bc'].iloc[-1], 2),
                'tc': round(self.df['tc'].iloc[-1], 2),
                'above_cpr': self.df['above_cpr'].iloc[-1],
                'below_cpr': self.df['below_cpr'].iloc[-1],
                'inside_cpr': self.df['inside_cpr'].iloc[-1]
            }
        }
        
        return {
            'pivot': self.df['pivot'],
            'bc': self.df['bc'],
            'tc': self.df['tc']
        }
    
    def calculate_moving_averages(self):
        """Calculate Simple and Exponential Moving Averages"""
        ma_params = self.params.get('moving_averages', {
            "sma_mid": 50,
            "sma_long": 200,
            "ema_short": 9,
            "ema_long": 21
        })
        
        # Calculate SMAs
        self.df['sma_mid'] = ta.sma(self.df['close'], length=ma_params['sma_mid'])
        self.df['sma_long'] = ta.sma(self.df['close'], length=ma_params['sma_long'])
        
        # Calculate EMAs
        self.df['ema_short'] = ta.ema(self.df['close'], length=ma_params['ema_short'])
        self.df['ema_long'] = ta.ema(self.df['close'], length=ma_params['ema_long'])
        
        # Generate signals
        self.df['ema_crossover'] = 0
        self.df.loc[self.df['ema_short'] > self.df['ema_long'], 'ema_crossover'] = 1
        self.df.loc[self.df['ema_short'] < self.df['ema_long'], 'ema_crossover'] = -1
        
        # Detect crossovers
        self.df['ema_buy_signal'] = ((self.df['ema_crossover'].shift(1) == -1) & 
                                    (self.df['ema_crossover'] == 1)).astype(int)
        self.df['ema_sell_signal'] = ((self.df['ema_crossover'].shift(1) == 1) & 
                                    (self.df['ema_crossover'] == -1)).astype(int)
        
        # Golden Cross / Death Cross (SMA 50 and 200)
        self.df['golden_cross'] = ((self.df['sma_mid'].shift(1) <= self.df['sma_long'].shift(1)) & 
                                   (self.df['sma_mid'] > self.df['sma_long'])).astype(int)
        self.df['death_cross'] = ((self.df['sma_mid'].shift(1) >= self.df['sma_long'].shift(1)) & 
                                 (self.df['sma_mid'] < self.df['sma_long'])).astype(int)
        
        # Save to results
        current_ema_signal = 0
        if self.df['ema_buy_signal'].iloc[-1] == 1:
            current_ema_signal = 1
        elif self.df['ema_sell_signal'].iloc[-1] == 1:
            current_ema_signal = -1
        
        # Check for golden/death cross
        if self.df['golden_cross'].iloc[-1] == 1:
            current_ema_signal = 1
        elif self.df['death_cross'].iloc[-1] == 1:
            current_ema_signal = -1
        
        # Check price relative to moving averages
        price_above_ema_short = self.df['close'].iloc[-1] > self.df['ema_short'].iloc[-1]
        price_above_ema_long = self.df['close'].iloc[-1] > self.df['ema_long'].iloc[-1]
        price_above_sma_mid = self.df['close'].iloc[-1] > self.df['sma_mid'].iloc[-1]
        price_above_sma_long = self.df['close'].iloc[-1] > self.df['sma_long'].iloc[-1]
        
        self.indicators['moving_averages'] = {
            'signal': current_ema_signal,
            'values': {
                'ema_short': round(self.df['ema_short'].iloc[-1], 2),
                'ema_long': round(self.df['ema_long'].iloc[-1], 2),
                'sma_mid': round(self.df['sma_mid'].iloc[-1], 2) if not pd.isna(self.df['sma_mid'].iloc[-1]) else None,
                'sma_long': round(self.df['sma_long'].iloc[-1], 2) if not pd.isna(self.df['sma_long'].iloc[-1]) else None,
                'price_above_ema_short': price_above_ema_short,
                'price_above_ema_long': price_above_ema_long,
                'price_above_sma_mid': price_above_sma_mid,
                'price_above_sma_long': price_above_sma_long,
                'golden_cross': self.df['golden_cross'].iloc[-1] == 1,
                'death_cross': self.df['death_cross'].iloc[-1] == 1
            }
        }
    
    def calculate_macd(self):
        """Calculate MACD (Moving Average Convergence Divergence)"""
        macd_params = self.params.get('macd', {
            "fast_period": 12,
            "slow_period": 26,
            "signal_period": 9
        })
        
        # Calculate MACD with pandas-ta
        macd = ta.macd(
            self.df['close'], 
            fast=macd_params['fast_period'], 
            slow=macd_params['slow_period'], 
            signal=macd_params['signal_period']
        )
        
        # Add MACD components to dataframe
        # Handle possible different naming conventions
        macd_columns = macd.columns.tolist()
        
        # Find the appropriate columns
        macd_line_col = next((col for col in macd_columns if 'MACD_' in col), None)
        signal_line_col = next((col for col in macd_columns if 'MACDs_' in col), None)
        histogram_col = next((col for col in macd_columns if 'MACDh_' in col), None)
        
        # Fallback to positional access if pattern matching fails
        if not all([macd_line_col, signal_line_col, histogram_col]) and len(macd_columns) >= 3:
            macd_line_col = macd_columns[0]
            signal_line_col = macd_columns[1]
            histogram_col = macd_columns[2]
        
        self.df['macd_line'] = macd[macd_line_col]
        self.df['signal_line'] = macd[signal_line_col]
        self.df['macd_histogram'] = macd[histogram_col]
        
        # Generate signals
        self.df['macd_crossover'] = 0
        self.df.loc[self.df['macd_line'] > self.df['signal_line'], 'macd_crossover'] = 1
        self.df.loc[self.df['macd_line'] < self.df['signal_line'], 'macd_crossover'] = -1
        
        # Detect crossovers
        self.df['macd_buy_signal'] = ((self.df['macd_crossover'].shift(1) == -1) & 
                                     (self.df['macd_crossover'] == 1)).astype(int)
        self.df['macd_sell_signal'] = ((self.df['macd_crossover'].shift(1) == 1) & 
                                      (self.df['macd_crossover'] == -1)).astype(int)
        
        # Histogram direction
        self.df['macd_hist_direction'] = 0
        self.df.loc[self.df['macd_histogram'] > self.df['macd_histogram'].shift(1), 'macd_hist_direction'] = 1
        self.df.loc[self.df['macd_histogram'] < self.df['macd_histogram'].shift(1), 'macd_hist_direction'] = -1
        
        # Detect histogram direction change
        self.df['macd_hist_direction_change'] = ((self.df['macd_hist_direction'].shift(1) != self.df['macd_hist_direction']) & 
                                               (self.df['macd_hist_direction'] != 0)).astype(int)
        
        # Save to results
        current_macd_signal = 0
        if self.df['macd_buy_signal'].iloc[-1] == 1:
            current_macd_signal = 1
        elif self.df['macd_sell_signal'].iloc[-1] == 1:
            current_macd_signal = -1
        
        # Check histogram direction for stronger signals
        hist_increasing = self.df['macd_histogram'].iloc[-1] > self.df['macd_histogram'].iloc[-2]
        hist_decreasing = self.df['macd_histogram'].iloc[-1] < self.df['macd_histogram'].iloc[-2]
        
        # Increase signal strength if histogram confirms
        if current_macd_signal == 1 and hist_increasing:
            current_macd_signal = 1
        elif current_macd_signal == -1 and hist_decreasing:
            current_macd_signal = -1
        
        self.indicators['macd'] = {
            'signal': current_macd_signal,
            'values': {
                'macd_line': round(self.df['macd_line'].iloc[-1], 4),
                'signal_line': round(self.df['signal_line'].iloc[-1], 4),
                'histogram': round(self.df['macd_histogram'].iloc[-1], 4),
                'histogram_direction': 'Increasing' if hist_increasing else 'Decreasing',
                'macd_line_direction': 'Increasing' if self.df['macd_line'].iloc[-1] > self.df['macd_line'].iloc[-2] else 'Decreasing'
            }
        }
    
    def calculate_rsi(self):
        """Calculate Relative Strength Index (RSI)"""
        rsi_params = self.params.get('rsi', {
            "period": 14,
            "oversold": 30,
            "overbought": 70
        })
        
        # Calculate RSI using pandas-ta
        self.df['rsi'] = ta.rsi(
            close=self.df['close'],
            length=rsi_params['period']
        )
        
        # Generate signals
        self.df['rsi_buy_signal'] = (self.df['rsi'] < rsi_params['oversold']).astype(int)
        self.df['rsi_sell_signal'] = (self.df['rsi'] > rsi_params['overbought']).astype(int)
        
        # RSI bullish/bearish divergence detection
        # Price making lower lows but RSI making higher lows (bullish)
        price_lower_low = (self.df['close'] < self.df['close'].shift(1)) & (self.df['close'].shift(1) < self.df['close'].shift(2))
        rsi_higher_low = (self.df['rsi'] > self.df['rsi'].shift(1)) & (self.df['rsi'].shift(1) > self.df['rsi'].shift(2))
        self.df['rsi_bullish_divergence'] = (price_lower_low & rsi_higher_low).astype(int)
        
        # Price making higher highs but RSI making lower highs (bearish)
        price_higher_high = (self.df['close'] > self.df['close'].shift(1)) & (self.df['close'].shift(1) > self.df['close'].shift(2))
        rsi_lower_high = (self.df['rsi'] < self.df['rsi'].shift(1)) & (self.df['rsi'].shift(1) < self.df['rsi'].shift(2))
        self.df['rsi_bearish_divergence'] = (price_higher_high & rsi_lower_high).astype(int)
        
        # Save to results
        current_rsi_signal = 0
        if self.df['rsi_buy_signal'].iloc[-1] == 1:
            current_rsi_signal = 1
        elif self.df['rsi_sell_signal'].iloc[-1] == 1:
            current_rsi_signal = -1
        
        # Add divergence signals
        if self.df['rsi_bullish_divergence'].iloc[-1] == 1:
            current_rsi_signal = 1
        elif self.df['rsi_bearish_divergence'].iloc[-1] == 1:
            current_rsi_signal = -1
        
        self.indicators['rsi'] = {
            'signal': current_rsi_signal,
            'values': {
                'rsi': round(self.df['rsi'].iloc[-1], 2),
                'oversold_threshold': rsi_params['oversold'],
                'overbought_threshold': rsi_params['overbought'],
                'is_oversold': self.df['rsi'].iloc[-1] < rsi_params['oversold'],
                'is_overbought': self.df['rsi'].iloc[-1] > rsi_params['overbought'],
                'bullish_divergence': self.df['rsi_bullish_divergence'].iloc[-1] == 1,
                'bearish_divergence': self.df['rsi_bearish_divergence'].iloc[-1] == 1
            }
        }
    
    def calculate_stochastic(self):
        """Calculate Stochastic Oscillator"""
        stoch_params = self.params.get('stochastic', {
            "k_period": 14,
            "d_period": 3,
            "oversold": 20,
            "overbought": 80
        })
        
        # Calculate Stochastic using pandas-ta
        stoch = ta.stoch(
            high=self.df['high'],
            low=self.df['low'],
            close=self.df['close'],
            k=stoch_params['k_period'],
            d=stoch_params['d_period']
        )
        
        # Find appropriate columns
        stoch_columns = stoch.columns.tolist()
        k_col = next((col for col in stoch_columns if 'STOCHk_' in col), None)
        d_col = next((col for col in stoch_columns if 'STOCHd_' in col), None)
        
        # Fallback to positional access if pattern matching fails
        if not all([k_col, d_col]) and len(stoch_columns) >= 2:
            k_col = stoch_columns[0]
            d_col = stoch_columns[1]
        
        self.df['stoch_k'] = stoch[k_col]
        self.df['stoch_d'] = stoch[d_col]
        
        # Generate signals
        # Buy signal: K crosses above D in oversold region
        buy_condition = ((self.df['stoch_k'] > self.df['stoch_d']) &  # K above D
                         (self.df['stoch_k'].shift(1) <= self.df['stoch_d'].shift(1)) &  # Crossover
                         (self.df['stoch_k'] < stoch_params['oversold'] + 10))  # In oversold region
        
        # Sell signal: K crosses below D in overbought region
        sell_condition = ((self.df['stoch_k'] < self.df['stoch_d']) &  # K below D
                          (self.df['stoch_k'].shift(1) >= self.df['stoch_d'].shift(1)) &  # Crossover
                          (self.df['stoch_k'] > stoch_params['overbought'] - 10))  # In overbought region
        
        self.df['stoch_buy_signal'] = buy_condition.astype(int)
        self.df['stoch_sell_signal'] = sell_condition.astype(int)
        
        # Save to results
        current_stoch_signal = 0
        if self.df['stoch_buy_signal'].iloc[-1] == 1:
            current_stoch_signal = 1
        elif self.df['stoch_sell_signal'].iloc[-1] == 1:
            current_stoch_signal = -1
        
        # Check for strong oversold/overbought conditions
        is_oversold = (self.df['stoch_k'].iloc[-1] < stoch_params['oversold']) & (self.df['stoch_d'].iloc[-1] < stoch_params['oversold'])
        is_overbought = (self.df['stoch_k'].iloc[-1] > stoch_params['overbought']) & (self.df['stoch_d'].iloc[-1] > stoch_params['overbought'])
        
        is_rising = self.df['stoch_k'].iloc[-1] > self.df['stoch_k'].iloc[-2]
        is_falling = self.df['stoch_k'].iloc[-1] < self.df['stoch_k'].iloc[-2]
        
        # Adjust signal for oversold/overbought with direction
        if is_oversold and is_rising:
            current_stoch_signal = 1
        elif is_overbought and is_falling:
            current_stoch_signal = -1
        
        self.indicators['stochastic'] = {
            'signal': current_stoch_signal,
            'values': {
                'k': round(self.df['stoch_k'].iloc[-1], 2),
                'd': round(self.df['stoch_d'].iloc[-1], 2),
                'oversold': stoch_params['oversold'],
                'overbought': stoch_params['overbought'],
                'is_oversold': is_oversold,
                'is_overbought': is_overbought,
                'is_rising': is_rising,
                'is_falling': is_falling
            }
        }
    
    def calculate_bollinger_bands(self):
        """Calculate Bollinger Bands"""
        bb_params = self.params.get('bollinger_bands', {
            "period": 20,
            "std_dev": 2
        })
        
        # Calculate Bollinger Bands using pandas-ta
        bbands = ta.bbands(
            close=self.df['close'],
            length=bb_params['period'],
            std=bb_params['std_dev']
        )
        
        # Find appropriate columns
        bbands_columns = bbands.columns.tolist()
        lower_band_col = next((col for col in bbands_columns if 'BBL_' in col), None)
        middle_band_col = next((col for col in bbands_columns if 'BBM_' in col), None)
        upper_band_col = next((col for col in bbands_columns if 'BBU_' in col), None)
        
        # Fallback to positional if needed
        if not all([lower_band_col, middle_band_col, upper_band_col]) and len(bbands_columns) >= 3:
            lower_band_col = bbands_columns[0]
            middle_band_col = bbands_columns[1]
            upper_band_col = bbands_columns[2]
        
        self.df['bb_lower'] = bbands[lower_band_col]
        self.df['bb_middle'] = bbands[middle_band_col]
        self.df['bb_upper'] = bbands[upper_band_col]
        
        # Calculate %B (position within bands)
        self.df['bb_pct_b'] = (self.df['close'] - self.df['bb_lower']) / (self.df['bb_upper'] - self.df['bb_lower'])
        
        # Calculate bandwidth (indicator of volatility)
        self.df['bb_bandwidth'] = (self.df['bb_upper'] - self.df['bb_lower']) / self.df['bb_middle']
        
        # Generate signals
        # Buy signal: Price touches or breaks lower band and RSI is oversold
        if 'rsi' not in self.df.columns:
            self.calculate_rsi()
            
        rsi_params = self.params.get('rsi', {"oversold": 30, "overbought": 70})
        
        buy_condition = ((self.df['close'] <= self.df['bb_lower']) & 
                         (self.df['rsi'] < rsi_params['oversold'] + 5))
        
        # Sell signal: Price touches or breaks upper band and RSI is overbought
        sell_condition = ((self.df['close'] >= self.df['bb_upper']) & 
                         (self.df['rsi'] > rsi_params['overbought'] - 5))
        
        self.df['bb_buy_signal'] = buy_condition.astype(int)
        self.df['bb_sell_signal'] = sell_condition.astype(int)
        
        # Save to results
        current_bb_signal = 0
        if self.df['bb_buy_signal'].iloc[-1] == 1:
            current_bb_signal = 1
        elif self.df['bb_sell_signal'].iloc[-1] == 1:
            current_bb_signal = -1
        
        # Add squeeze detection (low volatility)
        current_bandwidth = self.df['bb_bandwidth'].iloc[-1]
        historical_bandwidth = self.df['bb_bandwidth'].iloc[-20:-1]
        is_squeeze = current_bandwidth < historical_bandwidth.quantile(0.2)
        
        self.indicators['bollinger_bands'] = {
            'signal': current_bb_signal,
            'values': {
                'middle': round(self.df['bb_middle'].iloc[-1], 2),
                'upper': round(self.df['bb_upper'].iloc[-1], 2),
                'lower': round(self.df['bb_lower'].iloc[-1], 2),
                'percent_b': round(self.df['bb_pct_b'].iloc[-1], 2),
                'bandwidth': round(self.df['bb_bandwidth'].iloc[-1], 2),
                'is_squeeze': is_squeeze,
                'price_position': 'Above Upper' if self.df['close'].iloc[-1] > self.df['bb_upper'].iloc[-1] else
                                  'Below Lower' if self.df['close'].iloc[-1] < self.df['bb_lower'].iloc[-1] else
                                  'Within Bands'
            }
        }
    
    def calculate_supertrend(self):
        """Calculate Supertrend indicator"""
        st_params = self.params.get('supertrend', {
            "period": 10,
            "multiplier": 3
        })
        
        try:
            # Calculate Supertrend using pandas-ta
            supertrend = ta.supertrend(
                high=self.df['high'],
                low=self.df['low'],
                close=self.df['close'],
                length=st_params['period'],
                multiplier=st_params['multiplier']
            )
            
            # Find appropriate columns
            st_columns = supertrend.columns.tolist()
            
            # Use first two columns (typically values and direction)
            if len(st_columns) >= 2:
                supert_column = st_columns[0]
                direction_column = st_columns[1]
                
                self.df['supertrend'] = supertrend[supert_column]
                self.df['supertrend_direction'] = supertrend[direction_column]
                
                # Check the values in direction column to determine convention
                direction_values = supertrend[direction_column].unique()
                
                # Generate signals based on direction convention
                if 1 in direction_values and -1 in direction_values:
                    # Standard convention: 1 = bullish, -1 = bearish
                    self.df['supertrend_buy_signal'] = ((self.df['supertrend_direction'].shift(1) == -1) & 
                                                     (self.df['supertrend_direction'] == 1)).astype(int)
                    self.df['supertrend_sell_signal'] = ((self.df['supertrend_direction'].shift(1) == 1) & 
                                                      (self.df['supertrend_direction'] == -1)).astype(int)
                    
                    is_bullish = self.df['supertrend_direction'].iloc[-1] == 1
                
                elif True in direction_values and False in direction_values:
                    # Alternative convention: True = bullish, False = bearish
                    self.df['supertrend_buy_signal'] = ((self.df['supertrend_direction'].shift(1) == False) & 
                                                     (self.df['supertrend_direction'] == True)).astype(int)
                    self.df['supertrend_sell_signal'] = ((self.df['supertrend_direction'].shift(1) == True) & 
                                                      (self.df['supertrend_direction'] == False)).astype(int)
                    
                    is_bullish = self.df['supertrend_direction'].iloc[-1] == True
                
                else:
                    # Can't determine convention, use price relative to supertrend value
                    self.df['supertrend_buy_signal'] = ((self.df['close'].shift(1) < self.df['supertrend'].shift(1)) & 
                                                     (self.df['close'] > self.df['supertrend'])).astype(int)
                    self.df['supertrend_sell_signal'] = ((self.df['close'].shift(1) > self.df['supertrend'].shift(1)) & 
                                                      (self.df['close'] < self.df['supertrend'])).astype(int)
                    
                    is_bullish = self.df['close'].iloc[-1] > self.df['supertrend'].iloc[-1]
                
                # Save to results
                current_supertrend_signal = 0
                if self.df['supertrend_buy_signal'].iloc[-1] == 1:
                    current_supertrend_signal = 1
                elif self.df['supertrend_sell_signal'].iloc[-1] == 1:
                    current_supertrend_signal = -1
                
                self.indicators['supertrend'] = {
                    'signal': current_supertrend_signal,
                    'values': {
                        'supertrend': round(self.df['supertrend'].iloc[-1], 2),
                        'direction': 'Bullish' if is_bullish else 'Bearish',
                        'is_bullish': is_bullish
                    }
                }
            else:
                self.indicators['supertrend'] = {
                    'signal': 0,
                    'values': {
                        'supertrend': None,
                        'direction': 'Unknown',
                        'is_bullish': None
                    }
                }
        except Exception as e:
            logging.warning(f"Error calculating Supertrend: {e}")
            self.indicators['supertrend'] = {
                'signal': 0,
                'values': {
                    'supertrend': None,
                    'direction': 'Error',
                    'is_bullish': None
                }
            }
    
    def calculate_parabolic_sar(self):
        """Calculate Parabolic SAR indicator"""
        psar_params = self.params.get('parabolic_sar', {
            "acceleration_factor": 0.02,
            "max_acceleration_factor": 0.2
        })
        
        try:
            # Calculate PSAR using pandas-ta
            psar = ta.psar(
                high=self.df['high'],
                low=self.df['low'],
                close=self.df['close'],
                af=psar_params['acceleration_factor'],
                max_af=psar_params['max_acceleration_factor']
            )
            
            # Get column names
            psar_columns = psar.columns.tolist()
            
            # Find appropriate columns
            psar_long_col = next((col for col in psar_columns if 'PSARl_' in col), None)
            psar_short_col = next((col for col in psar_columns if 'PSARs_' in col), None)
            
            # Fallback to positional if needed
            if not all([psar_long_col, psar_short_col]) and len(psar_columns) >= 2:
                psar_long_col = psar_columns[0]
                psar_short_col = psar_columns[1]
            
            # Add PSAR components to dataframe
            self.df['psar_long'] = psar[psar_long_col]  # PSARl for long positions
            self.df['psar_short'] = psar[psar_short_col]  # PSARs for short positions
            
            # Determine trend direction
            self.df['psar_bull'] = self.df['close'] > self.df['psar_short']
            
            # Generate signals
            self.df['psar_buy_signal'] = ((self.df['psar_bull'].shift(1) == False) & 
                                       (self.df['psar_bull'] == True)).astype(int)
            self.df['psar_sell_signal'] = ((self.df['psar_bull'].shift(1) == True) & 
                                        (self.df['psar_bull'] == False)).astype(int)
            
            # Save to results
            current_psar_signal = 0
            if self.df['psar_buy_signal'].iloc[-1] == 1:
                current_psar_signal = 1
            elif self.df['psar_sell_signal'].iloc[-1] == 1:
                current_psar_signal = -1
            
            # Get current PSAR value (either long or short depending on trend)
            current_psar = self.df['psar_long'].iloc[-1] if not pd.isna(self.df['psar_long'].iloc[-1]) else self.df['psar_short'].iloc[-1]
            
            self.indicators['parabolic_sar'] = {
                'signal': current_psar_signal,
                'values': {
                    'psar': round(current_psar, 2),
                    'trend': 'Bullish' if self.df['psar_bull'].iloc[-1] else 'Bearish',
                    'is_bullish': self.df['psar_bull'].iloc[-1]
                }
            }
        except Exception as e:
            logging.warning(f"Error calculating Parabolic SAR: {e}")
            self.indicators['parabolic_sar'] = {
                'signal': 0,
                'values': {
                    'psar': None,
                    'trend': 'Error',
                    'is_bullish': None
                }
            }
    
    def calculate_atr(self):
        """Calculate Average True Range (ATR)"""
        atr_params = self.params.get('atr', {
            "period": 14,
            "multiplier": 2
        })
        
        # Calculate ATR using pandas-ta
        self.df['atr'] = ta.atr(
            high=self.df['high'],
            low=self.df['low'],
            close=self.df['close'],
            length=atr_params['period']
        )
        
        # Calculate ATR percentage (relative to price)
        self.df['atr_pct'] = 100 * self.df['atr'] / self.df['close']
        
        # Calculate stop loss levels based on ATR
        multiplier = atr_params['multiplier']
        self.df['atr_buy_stop'] = self.df['close'] - (self.df['atr'] * multiplier)
        self.df['atr_sell_stop'] = self.df['close'] + (self.df['atr'] * multiplier)
        
        # Calculate potential reward levels (2x risk)
        self.df['atr_buy_target'] = self.df['close'] + (self.df['atr'] * multiplier * 2)
        self.df['atr_sell_target'] = self.df['close'] - (self.df['atr'] * multiplier * 2)
        
        # Save to results
        self.indicators['atr'] = {
            'signal': 0,  # ATR doesn't generate buy/sell signals directly
            'values': {
                'atr': round(self.df['atr'].iloc[-1], 2),
                'atr_pct': round(self.df['atr_pct'].iloc[-1], 2),
                'buy_stop': round(self.df['atr_buy_stop'].iloc[-1], 2),
                'sell_stop': round(self.df['atr_sell_stop'].iloc[-1], 2),
                'buy_target': round(self.df['atr_buy_target'].iloc[-1], 2),
                'sell_target': round(self.df['atr_sell_target'].iloc[-1], 2)
            }
        }
    
    def calculate_adx(self):
        """Calculate Average Directional Index (ADX)"""
        adx_params = self.params.get('adx', {"period": 14, "threshold": 25})
        
        # Calculate ADX using pandas-ta
        adx_result = ta.adx(
            high=self.df['high'],
            low=self.df['low'],
            close=self.df['close'],
            length=adx_params['period']
        )
        
        # Extract components
        adx_columns = adx_result.columns.tolist()
        
        adx_col = next((col for col in adx_columns if 'ADX_' in col), None)
        dmp_col = next((col for col in adx_columns if 'DMP_' in col), None)
        dmn_col = next((col for col in adx_columns if 'DMN_' in col), None)
        
        # Fallback to positional
        if not all([adx_col, dmp_col, dmn_col]) and len(adx_columns) >= 3:
            adx_col = adx_columns[0]
            dmp_col = adx_columns[1]
            dmn_col = adx_columns[2]
        
        # Add ADX components to dataframe
        self.df['adx'] = adx_result[adx_col]
        self.df['dmp'] = adx_result[dmp_col]  # Positive directional movement
        self.df['dmn'] = adx_result[dmn_col]  # Negative directional movement
        
        # Generate signals
        # ADX determines trend strength, DI+ and DI- determine direction
        adx_threshold = adx_params['threshold']
        
        # Strong uptrend: ADX > threshold and DI+ > DI-
        strong_uptrend = (self.df['adx'] > adx_threshold) & (self.df['dmp'] > self.df['dmn'])
        
        # Strong downtrend: ADX > threshold and DI- > DI+
        strong_downtrend = (self.df['adx'] > adx_threshold) & (self.df['dmn'] > self.df['dmp'])
        
        # Crossovers
        di_crossover_buy = (self.df['dmp'] > self.df['dmn']) & (self.df['dmp'].shift(1) <= self.df['dmn'].shift(1))
        di_crossover_sell = (self.df['dmp'] < self.df['dmn']) & (self.df['dmp'].shift(1) >= self.df['dmn'].shift(1))
        
        self.df['adx_strong_uptrend'] = strong_uptrend.astype(int)
        self.df['adx_strong_downtrend'] = strong_downtrend.astype(int)
        self.df['adx_di_crossover_buy'] = di_crossover_buy.astype(int)
        self.df['adx_di_crossover_sell'] = di_crossover_sell.astype(int)
        
        # Determine current signal
        current_adx_signal = 0
        if self.df['adx_di_crossover_buy'].iloc[-1] == 1:
            current_adx_signal = 1
        elif self.df['adx_di_crossover_sell'].iloc[-1] == 1:
            current_adx_signal = -1
        elif self.df['adx_strong_uptrend'].iloc[-1] == 1:
            current_adx_signal = 1
        elif self.df['adx_strong_downtrend'].iloc[-1] == 1:
            current_adx_signal = -1
        
        self.indicators['adx'] = {
            'signal': current_adx_signal,
            'values': {
                'adx': round(self.df['adx'].iloc[-1], 2),
                'dmp': round(self.df['dmp'].iloc[-1], 2),
                'dmn': round(self.df['dmn'].iloc[-1], 2),
                'strong_uptrend': self.df['adx_strong_uptrend'].iloc[-1] == 1,
                'strong_downtrend': self.df['adx_strong_downtrend'].iloc[-1] == 1,
                'trend_strength': 'Strong' if self.df['adx'].iloc[-1] > adx_threshold else 
                                 'Moderate' if self.df['adx'].iloc[-1] > adx_threshold*0.7 else 'Weak',
                'trend_direction': 'Bullish' if self.df['dmp'].iloc[-1] > self.df['dmn'].iloc[-1] else 'Bearish'
            }
        }
    
    def calculate_obv(self):
        """Calculate On-Balance Volume (OBV)"""
        # Calculate OBV using pandas-ta
        self.df['obv'] = ta.obv(
            close=self.df['close'],
            volume=self.df['volume']
        )
        
        # Calculate OBV moving average
        self.df['obv_ma'] = self.df['obv'].rolling(14).mean()
        
        # Determine if OBV is rising or falling
        self.df['obv_rising'] = self.df['obv'] > self.df['obv'].shift(1)
        self.df['obv_falling'] = self.df['obv'] < self.df['obv'].shift(1)
        
        # Price/OBV divergence
        # Bullish divergence: Price making lower lows but OBV making higher lows
        price_lower_low = (self.df['close'] < self.df['close'].shift(1)) & (self.df['close'].shift(1) < self.df['close'].shift(2))
        obv_higher_low = (self.df['obv'] > self.df['obv'].shift(1)) & (self.df['obv'].shift(1) > self.df['obv'].shift(2))
        self.df['obv_bullish_divergence'] = (price_lower_low & obv_higher_low).astype(int)
        
        # Bearish divergence: Price making higher highs but OBV making lower highs
        price_higher_high = (self.df['close'] > self.df['close'].shift(1)) & (self.df['close'].shift(1) > self.df['close'].shift(2))
        obv_lower_high = (self.df['obv'] < self.df['obv'].shift(1)) & (self.df['obv'].shift(1) < self.df['obv'].shift(2))
        self.df['obv_bearish_divergence'] = (price_higher_high & obv_lower_high).astype(int)
        
        # Generate signal
        current_obv_signal = 0
        if self.df['obv_bullish_divergence'].iloc[-1] == 1:
            current_obv_signal = 1
        elif self.df['obv_bearish_divergence'].iloc[-1] == 1:
            current_obv_signal = -1
        
        # Check if OBV is above/below its MA
        if self.df['obv'].iloc[-1] > self.df['obv_ma'].iloc[-1] and self.df['obv'].iloc[-1] > self.df['obv'].iloc[-5]:
            current_obv_signal = 1
        elif self.df['obv'].iloc[-1] < self.df['obv_ma'].iloc[-1] and self.df['obv'].iloc[-1] < self.df['obv'].iloc[-5]:
            current_obv_signal = -1
        
        self.indicators['obv'] = {
            'signal': current_obv_signal,
            'values': {
                'obv': int(self.df['obv'].iloc[-1]),
                'obv_ma': int(self.df['obv_ma'].iloc[-1]) if not pd.isna(self.df['obv_ma'].iloc[-1]) else None,
                'rising': self.df['obv_rising'].iloc[-1],
                'falling': self.df['obv_falling'].iloc[-1],
                'bullish_divergence': self.df['obv_bullish_divergence'].iloc[-1] == 1,
                'bearish_divergence': self.df['obv_bearish_divergence'].iloc[-1] == 1
            }
        }
    
    def calculate_vwap(self):
        """Calculate Volume Weighted Average Price (VWAP)"""
        # Calculate typical price
        self.df['typical_price'] = (self.df['high'] + self.df['low'] + self.df['close']) / 3
        self.df['tp_volume'] = self.df['typical_price'] * self.df['volume']
        
        # Calculate cumulative values
        self.df['cumulative_tp_volume'] = self.df['tp_volume'].cumsum()
        self.df['cumulative_volume'] = self.df['volume'].cumsum()
        
        # Calculate VWAP
        self.df['vwap'] = self.df['cumulative_tp_volume'] / self.df['cumulative_volume']
        
        # Generate signals
        self.df['vwap_above'] = self.df['close'] > self.df['vwap']
        self.df['vwap_below'] = self.df['close'] < self.df['vwap']
        
        # Crossovers
        self.df['vwap_cross_above'] = (self.df['close'] > self.df['vwap']) & (self.df['close'].shift(1) <= self.df['vwap'].shift(1))
        self.df['vwap_cross_below'] = (self.df['close'] < self.df['vwap']) & (self.df['close'].shift(1) >= self.df['vwap'].shift(1))
        
        # High volume crossovers
        high_volume = self.df['volume'] > self.df['volume'].rolling(20).mean() * 1.5
        self.df['vwap_cross_above_vol'] = self.df['vwap_cross_above'] & high_volume
        self.df['vwap_cross_below_vol'] = self.df['vwap_cross_below'] & high_volume
        
        # Determine current signal
        current_vwap_signal = 0
        if self.df['vwap_cross_above_vol'].iloc[-1]:
            current_vwap_signal = 1
        elif self.df['vwap_cross_below_vol'].iloc[-1]:
            current_vwap_signal = -1
        
        # Regular crossover without volume confirmation
        if current_vwap_signal == 0:
            if self.df['vwap_cross_above'].iloc[-1]:
                current_vwap_signal = 1
            elif self.df['vwap_cross_below'].iloc[-1]:
                current_vwap_signal = -1
        
        self.indicators['vwap'] = {
            'signal': current_vwap_signal,
            'values': {
                'vwap': round(self.df['vwap'].iloc[-1], 2),
                'price_to_vwap': round(self.df['close'].iloc[-1] / self.df['vwap'].iloc[-1], 2),
                'above_vwap': self.df['vwap_above'].iloc[-1],
                'below_vwap': self.df['vwap_below'].iloc[-1],
                'high_volume_cross': self.df['vwap_cross_above_vol'].iloc[-1] or self.df['vwap_cross_below_vol'].iloc[-1]
            }
        }
    
    def calculate_aroon(self):
        """Calculate Aroon indicator"""
        aroon_params = self.params.get('aroon', {
            "period": 14,
            "uptrend_threshold": 70,
            "downtrend_threshold": 30
        })
        
        # Calculate Aroon using pandas-ta
        aroon = ta.aroon(
            high=self.df['high'],
            low=self.df['low'],
            length=aroon_params['period']
        )
        
        # Find appropriate columns
        aroon_columns = aroon.columns.tolist()
        aroon_up_col = next((col for col in aroon_columns if 'AROONU_' in col), None)
        aroon_down_col = next((col for col in aroon_columns if 'AROOND_' in col), None)
        
        # Fallback to positional if needed
        if not all([aroon_up_col, aroon_down_col]) and len(aroon_columns) >= 2:
            aroon_up_col = aroon_columns[0]
            aroon_down_col = aroon_columns[1]
        
        # Add Aroon components to dataframe
        self.df['aroon_up'] = aroon[aroon_up_col]
        self.df['aroon_down'] = aroon[aroon_down_col]
        
        # Generate signals
        # Strong uptrend: Aroon Up > threshold and Aroon Down < downtrend threshold
        strong_uptrend = (self.df['aroon_up'] > aroon_params['uptrend_threshold']) & (self.df['aroon_down'] < aroon_params['downtrend_threshold'])
        
        # Strong downtrend: Aroon Down > threshold and Aroon Up < downtrend threshold
        strong_downtrend = (self.df['aroon_down'] > aroon_params['uptrend_threshold']) & (self.df['aroon_up'] < aroon_params['downtrend_threshold'])
        
        # Crossovers
        aroon_cross_above = (self.df['aroon_up'] > self.df['aroon_down']) & (self.df['aroon_up'].shift(1) <= self.df['aroon_down'].shift(1))
        aroon_cross_below = (self.df['aroon_up'] < self.df['aroon_down']) & (self.df['aroon_up'].shift(1) >= self.df['aroon_down'].shift(1))
        
        self.df['aroon_strong_uptrend'] = strong_uptrend.astype(int)
        self.df['aroon_strong_downtrend'] = strong_downtrend.astype(int)
        self.df['aroon_cross_above'] = aroon_cross_above.astype(int)
        self.df['aroon_cross_below'] = aroon_cross_below.astype(int)
        
        # Determine current signal
        current_aroon_signal = 0
        if self.df['aroon_cross_above'].iloc[-1] == 1:
            current_aroon_signal = 1
        elif self.df['aroon_cross_below'].iloc[-1] == 1:
            current_aroon_signal = -1
        elif self.df['aroon_strong_uptrend'].iloc[-1] == 1:
            current_aroon_signal = 1
        elif self.df['aroon_strong_downtrend'].iloc[-1] == 1:
            current_aroon_signal = -1
        
        self.indicators['aroon'] = {
            'signal': current_aroon_signal,
            'values': {
                'aroon_up': round(self.df['aroon_up'].iloc[-1], 2),
                'aroon_down': round(self.df['aroon_down'].iloc[-1], 2),
                'strong_uptrend': self.df['aroon_strong_uptrend'].iloc[-1] == 1,
                'strong_downtrend': self.df['aroon_strong_downtrend'].iloc[-1] == 1,
                'trend': 'Strong Uptrend' if self.df['aroon_strong_uptrend'].iloc[-1] == 1 else
                         'Strong Downtrend' if self.df['aroon_strong_downtrend'].iloc[-1] == 1 else
                         'Bullish' if self.df['aroon_up'].iloc[-1] > self.df['aroon_down'].iloc[-1] else
                         'Bearish'
            }
        }
    
    def calculate_stochastic_rsi(self):
        """Calculate Stochastic RSI indicator"""
        stoch_rsi_params = self.params.get('stochastic_rsi', {
            "rsi_period": 14,
            "stoch_period": 14,
            "k_period": 3,
            "d_period": 3,
            "oversold": 20,
            "overbought": 80
        })
        
        try:
            # Ensure RSI is calculated
            if 'rsi' not in self.df.columns:
                self.calculate_rsi()
            
            # Calculate Stochastic RSI using pandas-ta
            stoch_rsi = ta.stochrsi(
                close=self.df['close'],
                length=stoch_rsi_params['rsi_period'],
                rsi_length=stoch_rsi_params['stoch_period'],
                k=stoch_rsi_params['k_period'],
                d=stoch_rsi_params['d_period']
            )
            
            # Find appropriate columns
            stoch_rsi_columns = stoch_rsi.columns.tolist()
            k_col = next((col for col in stoch_rsi_columns if '_K_' in col), None)
            d_col = next((col for col in stoch_rsi_columns if '_D_' in col), None)
            
            # Fallback to positional
            if not all([k_col, d_col]) and len(stoch_rsi_columns) >= 2:
                k_col = stoch_rsi_columns[0]
                d_col = stoch_rsi_columns[1]
            
            # Add components to dataframe
            self.df['stoch_rsi_k'] = stoch_rsi[k_col]
            self.df['stoch_rsi_d'] = stoch_rsi[d_col]
            
            # Generate signals
            oversold = stoch_rsi_params['oversold']
            overbought = stoch_rsi_params['overbought']
            
            # Oversold/overbought conditions
            self.df['stoch_rsi_oversold'] = (self.df['stoch_rsi_k'] < oversold)
            self.df['stoch_rsi_overbought'] = (self.df['stoch_rsi_k'] > overbought)
            
            # Crossovers
            self.df['stoch_rsi_cross_above'] = (self.df['stoch_rsi_k'] > self.df['stoch_rsi_d']) & (self.df['stoch_rsi_k'].shift(1) <= self.df['stoch_rsi_d'].shift(1))
            self.df['stoch_rsi_cross_below'] = (self.df['stoch_rsi_k'] < self.df['stoch_rsi_d']) & (self.df['stoch_rsi_k'].shift(1) >= self.df['stoch_rsi_d'].shift(1))
            
            # Strong signals: crossover in oversold/overbought regions
            self.df['stoch_rsi_strong_buy'] = self.df['stoch_rsi_cross_above'] & self.df['stoch_rsi_oversold']
            self.df['stoch_rsi_strong_sell'] = self.df['stoch_rsi_cross_below'] & self.df['stoch_rsi_overbought']
            
            # Determine current signal
            current_stoch_rsi_signal = 0
            if self.df['stoch_rsi_strong_buy'].iloc[-1]:
                current_stoch_rsi_signal = 1
            elif self.df['stoch_rsi_strong_sell'].iloc[-1]:
                current_stoch_rsi_signal = -1
            elif self.df['stoch_rsi_cross_above'].iloc[-1]:
                current_stoch_rsi_signal = 1
            elif self.df['stoch_rsi_cross_below'].iloc[-1]:
                current_stoch_rsi_signal = -1
            
            self.indicators['stochastic_rsi'] = {
                'signal': current_stoch_rsi_signal,
                'values': {
                    'k': round(self.df['stoch_rsi_k'].iloc[-1], 2),
                    'd': round(self.df['stoch_rsi_d'].iloc[-1], 2),
                    'oversold': self.df['stoch_rsi_oversold'].iloc[-1],
                    'overbought': self.df['stoch_rsi_overbought'].iloc[-1],
                    'strong_buy': self.df['stoch_rsi_strong_buy'].iloc[-1],
                    'strong_sell': self.df['stoch_rsi_strong_sell'].iloc[-1]
                }
            }
        except Exception as e:
            logging.warning(f"Error calculating Stochastic RSI: {e}")
            self.indicators['stochastic_rsi'] = {
                'signal': 0,
                'values': {
                    'k': None,
                    'd': None,
                    'oversold': False,
                    'overbought': False,
                    'strong_buy': False,
                    'strong_sell': False
                }
            }
    
    def calculate_fibonacci_retracement(self):
        """Calculate Fibonacci retracement levels"""
        # Get last N periods to find the trend
        lookback = min(100, len(self.df))
        recent_data = self.df.iloc[-lookback:]
        
        # Find highest high and lowest low
        highest_high = recent_data['high'].max()
        lowest_low = recent_data['low'].min()
        highest_high_idx = recent_data['high'].idxmax()
        lowest_low_idx = recent_data['low'].idxmin()
        
        # Determine if uptrend or downtrend based on which came first
        is_uptrend = recent_data.index.get_loc(lowest_low_idx) < recent_data.index.get_loc(highest_high_idx)
        
        # Calculate the price range
        price_range = highest_high - lowest_low
        
        # Calculate Fibonacci levels
        fib_levels = {}
        fib_values = [0, 0.236, 0.382, 0.5, 0.618, 0.786, 1]
        
        for level in fib_values:
            if is_uptrend:
                # In an uptrend, retracement levels go down from the high
                fib_levels[level] = highest_high - (price_range * level)
            else:
                # In a downtrend, retracement levels go up from the low
                fib_levels[level] = lowest_low + (price_range * level)
        
        # Calculate extension levels (1.272, 1.618, 2.0)
        extension_values = [1.272, 1.618, 2.0]
        for level in extension_values:
            if is_uptrend:
                # In an uptrend, extensions go up from the high
                fib_levels[level] = highest_high + (price_range * (level - 1))
            else:
                # In a downtrend, extensions go down from the low
                fib_levels[level] = lowest_low - (price_range * (level - 1))
        
        # Find nearest Fibonacci level to current price
        current_price = self.df['close'].iloc[-1]
        nearest_level = min(fib_levels.items(), key=lambda x: abs(x[1] - current_price))
        
        # Check if price is at a key Fibonacci level
        for level, value in fib_levels.items():
            fib_levels[level] = round(value, 2)
        
        # Generate signals
        fib_signal = 0
        if is_uptrend:
            # In uptrend, look for bounces off key retracement levels
            if 0.5 <= nearest_level[0] <= 0.618 and current_price > self.df['close'].iloc[-2]:
                fib_signal = 1  # Buy signal if bouncing off 50-61.8% retracement in uptrend
        else:
            # In downtrend, look for reversals off key retracement levels
            if 0.5 <= nearest_level[0] <= 0.618 and current_price < self.df['close'].iloc[-2]:
                fib_signal = -1  # Sell signal if reversing from 50-61.8% retracement in downtrend
        
        self.indicators['fibonacci'] = {
            'signal': fib_signal,
            'values': {
                'trend': 'Uptrend' if is_uptrend else 'Downtrend',
                'high': round(highest_high, 2),
                'low': round(lowest_low, 2),
                'fib_levels': fib_levels,
                'nearest_level': nearest_level[0],
                'nearest_level_price': round(nearest_level[1], 2)
            }
        }
    
    def get_signals(self):
        """Get trading signals from all calculated indicators"""
        # Ensure all indicators are calculated
        if not self.indicators:
            self.calculate_all()
        
        # Collect all signals
        signals = []
        
        # Process each indicator and its signal
        for indicator_name, indicator_data in self.indicators.items():
            signal_value = indicator_data.get('signal', 0)
            if signal_value != 0:
                # Map strength based on indicator type
                strength_map = {
                    'supertrend': 4,
                    'macd': 3,
                    'rsi': 2,
                    'bollinger_bands': 3,
                    'parabolic_sar': 3,
                    'moving_averages': 3,
                    'stochastic': 2,
                    'adx': 3,
                    'aroon': 3,
                    'obv': 2,
                    'vwap': 3,
                    'stochastic_rsi': 3,
                    'fibonacci': 2,
                    # New indicators with their signal strength
                    'alligator': 3,
                    'cpr': 3,
                    'atr_bands': 2,
                    'volume_analysis': 2,
                    'reward_risk': 2
                }
                
                # Get default strength or use 2 if not in map
                default_strength = strength_map.get(indicator_name, 2)
                
                # Create signal entry
                signal_entry = {
                    'indicator': indicator_name.replace('_', ' ').title(),
                    'signal': 'BUY' if signal_value > 0 else 'SELL',
                    'strength': default_strength
                }
                
                # Add description based on indicator
                if indicator_name == 'rsi':
                    if signal_value > 0:
                        signal_entry['description'] = f"RSI oversold ({indicator_data['values']['rsi']})"
                    else:
                        signal_entry['description'] = f"RSI overbought ({indicator_data['values']['rsi']})"
                
                elif indicator_name == 'macd':
                    if signal_value > 0:
                        signal_entry['description'] = "MACD line crossed above signal line"
                    else:
                        signal_entry['description'] = "MACD line crossed below signal line"
                
                elif indicator_name == 'supertrend':
                    if signal_value > 0:
                        signal_entry['description'] = "Price crossed above Supertrend"
                    else:
                        signal_entry['description'] = "Price crossed below Supertrend"
                
                elif indicator_name == 'bollinger_bands':
                    if signal_value > 0:
                        signal_entry['description'] = "Price at/below lower Bollinger Band"
                    else:
                        signal_entry['description'] = "Price at/above upper Bollinger Band"
                        
                elif indicator_name == 'alligator':
                    if signal_value > 0:
                        signal_entry['description'] = "Alligator in feeding phase (uptrend)"
                    else:
                        signal_entry['description'] = "Alligator in feeding phase (downtrend)"
                        
                elif indicator_name == 'cpr':
                    if signal_value > 0:
                        signal_entry['description'] = "Breakout above CPR top"
                    else:
                        signal_entry['description'] = "Breakout below CPR bottom"
                        
                elif indicator_name == 'atr_bands':
                    if signal_value > 0:
                        signal_entry['description'] = "Price broke above upper ATR band"
                    else:
                        signal_entry['description'] = "Price broke below lower ATR band"
                        
                elif indicator_name == 'volume_analysis':
                    if signal_value > 0:
                        signal_entry['description'] = "High volume on price increase (accumulation)"
                    else:
                        signal_entry['description'] = "High volume on price decrease (distribution)"
                        
                elif indicator_name == 'adx':
                    values = indicator_data.get('values', {})
                    trend_strength = values.get('trend_strength', 'Unknown')
                    trend_direction = values.get('trend_direction', 'Unknown')
                    signal_entry['description'] = f"ADX: {trend_strength} {trend_direction} trend"
                    
                elif indicator_name == 'aroon':
                    if signal_value > 0:
                        signal_entry['description'] = "Aroon Up crossed above Aroon Down"
                    else:
                        signal_entry['description'] = "Aroon Down crossed above Aroon Up"
                        
                elif indicator_name == 'stochastic':
                    if signal_value > 0:
                        signal_entry['description'] = "Stochastic %K crossed above %D in oversold region"
                    else:
                        signal_entry['description'] = "Stochastic %K crossed below %D in overbought region"
                        
                elif indicator_name == 'stochastic_rsi':
                    if signal_value > 0:
                        signal_entry['description'] = "Stochastic RSI indicates oversold conditions"
                    else:
                        signal_entry['description'] = "Stochastic RSI indicates overbought conditions"
                        
                elif indicator_name == 'parabolic_sar':
                    if signal_value > 0:
                        signal_entry['description'] = "Price crossed above Parabolic SAR"
                    else:
                        signal_entry['description'] = "Price crossed below Parabolic SAR"
                        
                elif indicator_name == 'vwap':
                    if signal_value > 0:
                        signal_entry['description'] = "Price crossed above VWAP with volume confirmation"
                    else:
                        signal_entry['description'] = "Price crossed below VWAP with volume confirmation"
                        
                elif indicator_name == 'obv':
                    if signal_value > 0:
                        signal_entry['description'] = "OBV rising, indicating buying pressure"
                    else:
                        signal_entry['description'] = "OBV falling, indicating selling pressure"
                        
                elif indicator_name == 'fibonacci':
                    if signal_value > 0:
                        signal_entry['description'] = "Price bouncing from key Fibonacci support level"
                    else:
                        signal_entry['description'] = "Price rejecting from key Fibonacci resistance level"
                        
                elif indicator_name == 'reward_risk':
                    values = indicator_data.get('values', {})
                    if signal_value > 0:
                        rrr = values.get('long_rrr', 0)
                        signal_entry['description'] = f"Favorable reward/risk ratio for long position ({rrr}:1)"
                    else:
                        rrr = values.get('short_rrr', 0)
                        signal_entry['description'] = f"Favorable reward/risk ratio for short position ({rrr}:1)"
                
                # Add signal to list
                signals.append(signal_entry)
        
        return signals
    
    def get_overall_signal(self):
        """Get the overall trading signal from all indicators"""
        # Get individual signals
        signals = self.get_signals()
        
        # Count buy and sell signals
        buy_signals = [s for s in signals if s['signal'] == 'BUY']
        sell_signals = [s for s in signals if s['signal'] == 'SELL']
        
        # Calculate weighted signal strength
        buy_strength = sum(s['strength'] for s in buy_signals)
        sell_strength = sum(s['strength'] for s in sell_signals)
        
        # Determine overall signal
        if buy_strength > sell_strength and buy_strength >= Config.MINIMUM_SIGNAL_STRENGTH:
            signal_type = 'BUY'
            strength = min(5, max(1, round(buy_strength / len(buy_signals)) if buy_signals else 0))
            confidence = min(10, max(1, round((buy_strength - sell_strength) / 2)))
            summary = f"Bullish signal with {len(buy_signals)} indicators confirming"
        elif sell_strength > buy_strength and sell_strength >= Config.MINIMUM_SIGNAL_STRENGTH:
            signal_type = 'SELL'
            strength = min(5, max(1, round(sell_strength / len(sell_signals)) if sell_signals else 0))
            confidence = min(10, max(1, round((sell_strength - buy_strength) / 2)))
            summary = f"Bearish signal with {len(sell_signals)} indicators confirming"
        else:
            signal_type = 'NEUTRAL'
            strength = 0
            confidence = 0
            summary = "No clear trading signal detected"
        
        # Get support and resistance levels
        support, resistance = self.get_support_resistance()
        
        # Calculate key levels and risk:reward ratio
        current_price = self.df['Close'].iloc[-1]
        atr_value = self.indicators.get('atr', {}).get('values', {}).get('atr', 0)
        
        # Check if we have RRR data
        rrr_data = self.indicators.get('reward_risk', {}).get('values', {})
        
        # Default stop loss and target based on ATR (if available)
        stop_loss = None
        target = None
        
        # Use RRR-based stops and targets if available
        if signal_type == 'BUY' and rrr_data.get('long_rrr_valid', False):
            stop_loss = rrr_data.get('long_stop')
            target = rrr_data.get('long_target')
        elif signal_type == 'SELL' and rrr_data.get('short_rrr_valid', False):
            stop_loss = rrr_data.get('short_stop')
            target = rrr_data.get('short_target')
        # Fall back to ATR method if RRR not available
        elif atr_value and atr_value > 0:
            if signal_type == 'BUY':
                stop_loss = current_price - (atr_value * 2)
                target = current_price + (atr_value * 4)  # 2:1 risk:reward
            elif signal_type == 'SELL':
                stop_loss = current_price + (atr_value * 2)
                target = current_price - (atr_value * 4)  # 2:1 risk:reward
        
        # Use support/resistance if no ATR or if they provide tighter stops
        if signal_type == 'BUY' and support:
            if not stop_loss or support > stop_loss:
                stop_loss = support
                target = current_price + (current_price - stop_loss) * 2  # 2:1 risk:reward
        elif signal_type == 'SELL' and resistance:
            if not stop_loss or resistance < stop_loss:
                stop_loss = resistance
                target = current_price - (stop_loss - current_price) * 2  # 2:1 risk:reward
        
        return {
            'signal': signal_type,
            'strength': strength,
            'confidence': confidence,
            'summary': summary,
            'buy_signals': len(buy_signals),
            'sell_signals': len(sell_signals),
            'current_price': current_price,
            'support': support,
            'resistance': resistance,
            'stop_loss': round(stop_loss, 2) if stop_loss else None,
            'target': round(target, 2) if target else None,
            'individual_signals': signals,
            'timestamp': datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        }
        
    def get_support_resistance(self):
        """Get nearest support and resistance levels"""
        # Get last 100 periods of data
        lookback = min(100, len(self.df))
        recent_data = self.df.iloc[-lookback:]
        
        # Function to detect swing highs and lows
        def find_swings(data, window=5):
            swing_highs = []
            swing_lows = []
            
            for i in range(window, len(data) - window):
                # Check for swing high (highest high in window)
                if data['high'].iloc[i] == data['high'].iloc[i-window:i+window+1].max():
                    swing_highs.append((data.index[i], data['high'].iloc[i]))
                
                # Check for swing low (lowest low in window)
                if data['low'].iloc[i] == data['low'].iloc[i-window:i+window+1].min():
                    swing_lows.append((data.index[i], data['low'].iloc[i]))
            
            return swing_highs, swing_lows
        
        # Find swing highs and lows
        swing_highs, swing_lows = find_swings(recent_data)
        
        # Get current price
        current_price = self.df['close'].iloc[-1]
        
        # Find nearest support (swing low below current price)
        supports = [price for _, price in swing_lows if price < current_price]
        nearest_support = max(supports) if supports else None
        
        # Find nearest resistance (swing high above current price)
        resistances = [price for _, price in swing_highs if price > current_price]
        nearest_resistance = min(resistances) if resistances else None
        
        return nearest_support, nearest_resistance
    
    def get_detailed_analysis(self):
        """Get a detailed analysis of all indicators"""
        # Ensure all indicators are calculated
        if not self.indicators:
            self.calculate_all()
        
        analysis = []
        
        # Current price
        current_price = self.df['close'].iloc[-1]
        analysis.append(f"Current Price: {current_price:.2f}")
        
        # Moving Averages
        if 'moving_averages' in self.indicators:
            ma = self.indicators['moving_averages']['values']
            ma_signal = self.indicators['moving_averages']['signal']
            
            # Trend based on moving averages
            if ma['price_above_sma_long']:
                analysis.append("TREND: Bullish (Price > 200 SMA)")
            else:
                analysis.append("TREND: Bearish (Price < 200 SMA)")
            
            # Moving average positioning
            analysis.append(f"EMA(9): {ma['ema_short']:.2f}, EMA(21): {ma['ema_long']:.2f}")
            analysis.append(f"SMA(50): {ma.get('sma_mid', 'N/A')}, SMA(200): {ma.get('sma_long', 'N/A')}")
            
            # Crossovers
            if ma['golden_cross']:
                analysis.append("⭐ GOLDEN CROSS: 50 SMA crossed above 200 SMA (Strong Bullish)")
            elif ma['death_cross']:
                analysis.append("⚠️ DEATH CROSS: 50 SMA crossed below 200 SMA (Strong Bearish)")
        
        # RSI
        if 'rsi' in self.indicators:
            rsi = self.indicators['rsi']['values']
            rsi_signal = self.indicators['rsi']['signal']
            
            rsi_value = rsi['rsi']
            if rsi['is_oversold']:
                analysis.append(f"RSI: {rsi_value:.2f} (OVERSOLD - Bullish)")
            elif rsi['is_overbought']:
                analysis.append(f"RSI: {rsi_value:.2f} (OVERBOUGHT - Bearish)")
            else:
                analysis.append(f"RSI: {rsi_value:.2f} (Neutral)")
            
            # Divergence
            if rsi['bullish_divergence']:
                analysis.append("⭐ BULLISH RSI DIVERGENCE detected")
            elif rsi['bearish_divergence']:
                analysis.append("⚠️ BEARISH RSI DIVERGENCE detected")
        
        # MACD
        if 'macd' in self.indicators:
            macd = self.indicators['macd']['values']
            macd_signal = self.indicators['macd']['signal']
            
            analysis.append(f"MACD: Line={macd['macd_line']:.4f}, Signal={macd['signal_line']:.4f}, Hist={macd['histogram']:.4f}")
            analysis.append(f"MACD Direction: {macd['macd_line_direction']}, Histogram: {macd['histogram_direction']}")
            
            if macd_signal == 1:
                analysis.append("⭐ MACD BULLISH CROSSOVER detected")
            elif macd_signal == -1:
                analysis.append("⚠️ MACD BEARISH CROSSOVER detected")
        
        # Supertrend
        if 'supertrend' in self.indicators:
            st = self.indicators['supertrend']['values']
            st_signal = self.indicators['supertrend']['signal']
            
            analysis.append(f"Supertrend: {st['direction']} (Value: {st.get('supertrend', 'N/A')})")
            
            if st_signal == 1:
                analysis.append("⭐ SUPERTREND turned BULLISH")
            elif st_signal == -1:
                analysis.append("⚠️ SUPERTREND turned BEARISH")
        
        # ADX
        if 'adx' in self.indicators:
            adx = self.indicators['adx']['values']
            adx_signal = self.indicators['adx']['signal']
            
            analysis.append(f"ADX: {adx['adx']:.2f} ({adx['trend_strength']} {adx['trend_direction']} trend)")
            analysis.append(f"DI+: {adx['dmp']:.2f}, DI-: {adx['dmn']:.2f}")
            
            if adx['strong_uptrend']:
                analysis.append("⭐ ADX indicates STRONG UPTREND")
            elif adx['strong_downtrend']:
                analysis.append("⚠️ ADX indicates STRONG DOWNTREND")
        
        # Bollinger Bands
        if 'bollinger_bands' in self.indicators:
            bb = self.indicators['bollinger_bands']['values']
            bb_signal = self.indicators['bollinger_bands']['signal']
            
            analysis.append(f"Bollinger Bands: Upper={bb['upper']:.2f}, Middle={bb['middle']:.2f}, Lower={bb['lower']:.2f}")
            analysis.append(f"BB Position: {bb['price_position']} (B%={bb['percent_b']:.2f})")
            
            if bb['is_squeeze']:
                analysis.append("⚠️ BOLLINGER BAND SQUEEZE (Low volatility - Breakout potential)")
            
            if bb_signal == 1:
                analysis.append("⭐ PRICE at/below LOWER BAND (Potential bullish reversal)")
            elif bb_signal == -1:
                analysis.append("⚠️ PRICE at/above UPPER BAND (Potential bearish reversal)")
        
        # Support/Resistance & Risk Management
        support, resistance = self.get_support_resistance()
        if support:
            support_pct = abs(current_price - support) / current_price * 100
            analysis.append(f"Nearest Support: {support:.2f} ({support_pct:.2f}% below price)")
        
        if resistance:
            resistance_pct = abs(resistance - current_price) / current_price * 100
            analysis.append(f"Nearest Resistance: {resistance:.2f} ({resistance_pct:.2f}% above price)")
        
        # Risk Management from ATR
        if 'atr' in self.indicators:
            atr = self.indicators['atr']['values']
            analysis.append(f"ATR: {atr['atr']:.2f} ({atr['atr_pct']:.2f}% of price)")
            analysis.append(f"Suggested Stop Loss (Buy): {atr['buy_stop']:.2f}")
            analysis.append(f"Suggested Target (Buy): {atr['buy_target']:.2f}")
            analysis.append(f"Suggested Stop Loss (Sell): {atr['sell_stop']:.2f}")
            analysis.append(f"Suggested Target (Sell): {atr['sell_target']:.2f}")
        
        return "\n".join(analysis)


class ChartPatterns:
    """Detect complex chart patterns in price data"""
    
    def __init__(self, df, params=None):
        """
        Initialize with OHLCV DataFrame and parameters
        
        Args:
            df: DataFrame with OHLCV data (index=timestamp, columns=[open, high, low, close, volume])
            params: Dictionary of parameters for pattern detection
        """
        self.df = df.copy()
        self.params = params or {}
        self.patterns = {}
        
        # Default parameters
        self.window_size = self.params.get('window_size', 5)
        self.threshold = self.params.get('threshold', 0.03)
        self.min_touches = self.params.get('min_touches', 3)
    
    def detect_all_patterns(self):
        """Detect all chart patterns"""
        self.detect_head_and_shoulders()
        self.detect_inverse_head_and_shoulders()
        self.detect_double_patterns()
        self.detect_triple_patterns()
        self.detect_wedges()
        self.detect_channels()
        self.detect_rectangle()
        self.detect_cup_and_handle()
        self.detect_flags()
        
        return self.patterns
    
    def find_swing_points(self, window_size=None):
        """Find swing highs and lows in the price data"""
        window = window_size or self.window_size
        
        # Find local maxima and minima
        swing_highs = []
        swing_lows = []
        
        # Iterate through data with window
        for i in range(window, len(self.df) - window):
            # Check for swing high
            if self.df['high'].iloc[i] == self.df['high'].iloc[i-window:i+window+1].max():
                swing_highs.append((i, self.df.index[i], self.df['high'].iloc[i]))
            
            # Check for swing low
            if self.df['low'].iloc[i] == self.df['low'].iloc[i-window:i+window+1].min():
                swing_lows.append((i, self.df.index[i], self.df['low'].iloc[i]))
        
        return swing_highs, swing_lows
    
    def detect_head_and_shoulders(self):
        """Detect Head and Shoulders pattern (bearish reversal)"""
        swing_highs, _ = self.find_swing_points()
        
        # Need at least 5 swing highs to detect
        if len(swing_highs) < 5:
            return
        
        # Check the last several swing highs
        for i in range(len(swing_highs) - 4):
            # Get 5 consecutive swing highs
            five_points = swing_highs[i:i+5]
            
            # Extract heights
            heights = [point[2] for point in five_points]
            
            # Check for pattern:
            # Point 1: left shoulder
            # Point 2: head
            # Point 3: right shoulder
            # In classic H&S, head is higher than shoulders, and shoulders are at similar heights
            if (heights[1] < heights[2] and
                heights[3] < heights[2] and
                abs(heights[1] - heights[3]) / heights[1] < self.threshold and
                five_points[4][0] - five_points[0][0] < len(self.df) * 0.3):  # Pattern should not be too wide
                
                # Get the neckline (connecting the lows between shoulders and head)
                idx1, date1, _ = five_points[0]  # Left shoulder
                idx2, date2, _ = five_points[2]  # Head
                idx3, date3, _ = five_points[4]  # Right shoulder
                
                # Find the lowest points between shoulders and head
                left_valley = self.df['low'].iloc[idx1:idx2].min()
                right_valley = self.df['low'].iloc[idx2:idx3].min()
                
                # Neckline would be flat or slightly tilted, connecting these valleys
                neckline_trend = (right_valley - left_valley) / (idx3 - idx1)
                
                # Check if price broke below the neckline after right shoulder
                current_price = self.df['close'].iloc[-1]
                expected_neckline_at_end = left_valley + neckline_trend * (len(self.df) - 1 - idx1)
                
                if current_price < expected_neckline_at_end:
                    # Pattern confirmed with neckline break
                    self.patterns['head_and_shoulders'] = {
                        'signal': -1,  # Bearish
                        'strength': 4,
                        'left_shoulder': date1,
                        'head': date2,
                        'right_shoulder': date3,
                        'neckline_start': left_valley,
                        'neckline_end': right_valley
                    }
                    return
    
    def detect_inverse_head_and_shoulders(self):
        """Detect Inverse Head and Shoulders pattern (bullish reversal)"""
        _, swing_lows = self.find_swing_points()
        
        # Need at least 5 swing lows to detect
        if len(swing_lows) < 5:
            return
        
        # Check the last several swing lows
        for i in range(len(swing_lows) - 4):
            # Get 5 consecutive swing lows
            five_points = swing_lows[i:i+5]
            
            # Extract prices
            prices = [point[2] for point in five_points]
            
            # Check for pattern:
            # Point 1: left shoulder
            # Point 2: head
            # Point 3: right shoulder
            # In inverse H&S, head is lower than shoulders, and shoulders are at similar heights
            if (prices[1] > prices[2] and
                prices[3] > prices[2] and
                abs(prices[1] - prices[3]) / prices[1] < self.threshold and
                five_points[4][0] - five_points[0][0] < len(self.df) * 0.3):  # Pattern should not be too wide
                
                # Get the neckline (connecting the highs between shoulders and head)
                idx1, date1, _ = five_points[0]  # Left shoulder
                idx2, date2, _ = five_points[2]  # Head
                idx3, date3, _ = five_points[4]  # Right shoulder
                
                # Find the highest points between shoulders and head
                left_peak = self.df['high'].iloc[idx1:idx2].max()
                right_peak = self.df['high'].iloc[idx2:idx3].max()
                
                # Neckline would be flat or slightly tilted, connecting these peaks
                neckline_trend = (right_peak - left_peak) / (idx3 - idx1)
                
                # Check if price broke above the neckline after right shoulder
                current_price = self.df['close'].iloc[-1]
                expected_neckline_at_end = left_peak + neckline_trend * (len(self.df) - 1 - idx1)
                
                if current_price > expected_neckline_at_end:
                    # Pattern confirmed with neckline break
                    self.patterns['inverse_head_and_shoulders'] = {
                        'signal': 1,  # Bullish
                        'strength': 4,
                        'left_shoulder': date1,
                        'head': date2,
                        'right_shoulder': date3,
                        'neckline_start': left_peak,
                        'neckline_end': right_peak
                    }
                    return
    
    def detect_double_patterns(self):
        """Detect Double Top and Double Bottom patterns"""
        swing_highs, swing_lows = self.find_swing_points()
        
        # Check for double top
        if len(swing_highs) >= 2:
            # Get the last two swing highs
            last_two_highs = swing_highs[-2:]
            
            # Extract heights and indices
            heights = [point[2] for point in last_two_highs]
            indices = [point[0] for point in last_two_highs]
            
            # Check if the two peaks are close enough in height
            if abs(heights[0] - heights[1]) / heights[0] < self.threshold:
                # Check if there's a significant valley between the peaks
                min_between = self.df['low'].iloc[indices[0]:indices[1]].min()
                
                # Calculate the neckline (support level)
                neckline = min_between
                
                # Check for breakout below neckline
                current_price = self.df['close'].iloc[-1]
                
                if current_price < neckline:
                    # Pattern confirmed with neckline break
                    self.patterns['double_top'] = {
                        'signal': -1,  # Bearish
                        'strength': 4,
                        'peaks': [last_two_highs[0][1], last_two_highs[1][1]],
                        'neckline': neckline
                    }
        
        # Check for double bottom
        if len(swing_lows) >= 2:
            # Get the last two swing lows
            last_two_lows = swing_lows[-2:]
            
            # Extract prices and indices
            prices = [point[2] for point in last_two_lows]
            indices = [point[0] for point in last_two_lows]
            
            # Check if the two bottoms are close enough in price
            if abs(prices[0] - prices[1]) / prices[0] < self.threshold:
                # Check if there's a significant peak between the bottoms
                max_between = self.df['high'].iloc[indices[0]:indices[1]].max()
                
                # Calculate the neckline (resistance level)
                neckline = max_between
                
                # Check for breakout above neckline
                current_price = self.df['close'].iloc[-1]
                
                if current_price > neckline:
                    # Pattern confirmed with neckline break
                    self.patterns['double_bottom'] = {
                        'signal': 1,  # Bullish
                        'strength': 4,
                        'bottoms': [last_two_lows[0][1], last_two_lows[1][1]],
                        'neckline': neckline
                    }
    
    def detect_triple_patterns(self):
        """Detect Triple Top and Triple Bottom patterns"""
        swing_highs, swing_lows = self.find_swing_points()
        
        # Check for triple top
        if len(swing_highs) >= 3:
            # Get the last three swing highs
            last_three_highs = swing_highs[-3:]
            
            # Extract heights
            heights = [point[2] for point in last_three_highs]
            indices = [point[0] for point in last_three_highs]
            
            # Check if all three peaks are close enough in height
            if (abs(heights[0] - heights[1]) / heights[0] < self.threshold and 
                abs(heights[1] - heights[2]) / heights[1] < self.threshold and
                abs(heights[0] - heights[2]) / heights[0] < self.threshold):
                
                # Check if there are significant valleys between the peaks
                min_between_1 = self.df['low'].iloc[indices[0]:indices[1]].min()
                min_between_2 = self.df['low'].iloc[indices[1]:indices[2]].min()
                
                # Calculate the neckline (support level)
                neckline = min(min_between_1, min_between_2)
                
                # Check for breakout below neckline
                current_price = self.df['close'].iloc[-1]
                
                if current_price < neckline:
                    # Pattern confirmed with neckline break
                    self.patterns['triple_top'] = {
                        'signal': -1,  # Bearish
                        'strength': 5,  # Stronger than double top
                        'peaks': [last_three_highs[0][1], last_three_highs[1][1], last_three_highs[2][1]],
                        'neckline': neckline
                    }
        
        # Check for triple bottom
        if len(swing_lows) >= 3:
            # Get the last three swing lows
            last_three_lows = swing_lows[-3:]
            
            # Extract prices
            prices = [point[2] for point in last_three_lows]
            indices = [point[0] for point in last_three_lows]
            
            # Check if all three bottoms are close enough in price
            if (abs(prices[0] - prices[1]) / prices[0] < self.threshold and 
                abs(prices[1] - prices[2]) / prices[1] < self.threshold and
                abs(prices[0] - prices[2]) / prices[0] < self.threshold):
                
                # Check if there are significant peaks between the bottoms
                max_between_1 = self.df['high'].iloc[indices[0]:indices[1]].max()
                max_between_2 = self.df['high'].iloc[indices[1]:indices[2]].max()
                
                # Calculate the neckline (resistance level)
                neckline = max(max_between_1, max_between_2)
                
                # Check for breakout above neckline
                current_price = self.df['close'].iloc[-1]
                
                if current_price > neckline:
                    # Pattern confirmed with neckline break
                    self.patterns['triple_bottom'] = {
                        'signal': 1,  # Bullish
                        'strength': 5,  # Stronger than double bottom
                        'bottoms': [last_three_lows[0][1], last_three_lows[1][1], last_three_lows[2][1]],
                        'neckline': neckline
                    }
    
    def detect_wedges(self):
        """Detect Rising and Falling Wedge patterns"""
        # Get enough swing points
        swing_highs, swing_lows = self.find_swing_points()
        
        # Need at least 3 swing points of each type
        if len(swing_highs) < 3 or len(swing_lows) < 3:
            return
        
        # Get the last three swing highs and lows
        last_three_highs = swing_highs[-3:]
        last_three_lows = swing_lows[-3:]
        
        # Extract indices and prices
        high_indices = [point[0] for point in last_three_highs]
        high_prices = [point[2] for point in last_three_highs]
        
        low_indices = [point[0] for point in last_three_lows]
        low_prices = [point[2] for point in last_three_lows]
        
        # Check if points align in time (interleaved)
        if len(high_indices) >= 2 and len(low_indices) >= 2:
            # Check for rising wedge (bearish)
            # In a rising wedge, both support and resistance lines slope upward,
            # but support line slopes more steeply
            rising_highs = all(high_prices[i] < high_prices[i+1] for i in range(len(high_prices)-1))
            rising_lows = all(low_prices[i] < low_prices[i+1] for i in range(len(low_prices)-1))
            
            if rising_highs and rising_lows:
                # Calculate slopes
                high_slope = (high_prices[-1] - high_prices[0]) / (high_indices[-1] - high_indices[0])
                low_slope = (low_prices[-1] - low_prices[0]) / (low_indices[-1] - low_indices[0])
                
                # In rising wedge, low slope should be steeper than high slope
                if low_slope > high_slope:
                    # Check for current price below the support line
                    current_idx = len(self.df) - 1
                    expected_support = low_prices[0] + low_slope * (current_idx - low_indices[0])
                    
                    if self.df['close'].iloc[-1] < expected_support:
                        # Pattern confirmed with break below support
                        self.patterns['rising_wedge'] = {
                            'signal': -1,  # Bearish
                            'strength': 4,
                            'high_points': [point[1] for point in last_three_highs],
                            'low_points': [point[1] for point in last_three_lows]
                        }
            
            # Check for falling wedge (bullish)
            # In a falling wedge, both support and resistance lines slope downward,
            # but resistance line slopes more steeply
            falling_highs = all(high_prices[i] > high_prices[i+1] for i in range(len(high_prices)-1))
            falling_lows = all(low_prices[i] > low_prices[i+1] for i in range(len(low_prices)-1))
            
            if falling_highs and falling_lows:
                # Calculate slopes (negative for downward)
                high_slope = (high_prices[-1] - high_prices[0]) / (high_indices[-1] - high_indices[0])
                low_slope = (low_prices[-1] - low_prices[0]) / (low_indices[-1] - low_indices[0])
                
                # In falling wedge, high slope should be steeper (more negative) than low slope
                if high_slope < low_slope:
                    # Check for current price above the resistance line
                    current_idx = len(self.df) - 1
                    expected_resistance = high_prices[0] + high_slope * (current_idx - high_indices[0])
                    
                    if self.df['close'].iloc[-1] > expected_resistance:
                        # Pattern confirmed with break above resistance
                        self.patterns['falling_wedge'] = {
                            'signal': 1,  # Bullish
                            'strength': 4,
                            'high_points': [point[1] for point in last_three_highs],
                            'low_points': [point[1] for point in last_three_lows]
                        }
    
    def detect_channels(self):
        """Detect ascending, descending and horizontal channels"""
        # Get enough swing points
        swing_highs, swing_lows = self.find_swing_points()
        
        # Need at least 3 swing points of each type
        if len(swing_highs) < 3 or len(swing_lows) < 3:
            return
        
        # Get the last few swing highs and lows
        last_highs = swing_highs[-3:]
        last_lows = swing_lows[-3:]
        
        # Extract indices and prices
        high_indices = [point[0] for point in last_highs]
        high_prices = [point[2] for point in last_highs]
        
        low_indices = [point[0] for point in last_lows]
        low_prices = [point[2] for point in last_lows]
        
        # Calculate slopes
        high_slope = (high_prices[-1] - high_prices[0]) / (high_indices[-1] - high_indices[0])
        low_slope = (low_prices[-1] - low_prices[0]) / (low_indices[-1] - low_indices[0])
        
        # Check if slopes are parallel (similar)
        if abs(high_slope - low_slope) / max(abs(high_slope), abs(low_slope), 0.0001) < 0.3:
            # Channel detected, now determine type and check for breakouts
            current_price = self.df['close'].iloc[-1]
            current_idx = len(self.df) - 1
            
            # Calculate expected upper and lower bounds at current index
            expected_upper = high_prices[0] + high_slope * (current_idx - high_indices[0])
            expected_lower = low_prices[0] + low_slope * (current_idx - low_indices[0])
            
            # Channel width (percentage)
            channel_width = (expected_upper - expected_lower) / expected_lower * 100
            
            # Determine channel type based on slope
            if high_slope > 0.0001:  # Positive slope (ascending)
                channel_type = 'ascending_channel'
                if current_price > expected_upper:
                    # Bullish breakout above ascending channel
                    self.patterns[channel_type] = {
                        'signal': 1,  # Very bullish
                        'strength': 5,
                        'breakout': 'above',
                        'width': channel_width
                    }
                elif current_price < expected_lower:
                    # Bearish breakdown below ascending channel
                    self.patterns[channel_type] = {
                        'signal': -1,  # Bearish
                        'strength': 3,
                        'breakout': 'below',
                        'width': channel_width
                    }
                else:
                    # Inside channel
                    position = (current_price - expected_lower) / (expected_upper - expected_lower)
                    signal = 1 if position < 0.3 else -1 if position > 0.7 else 0
                    self.patterns[channel_type] = {
                        'signal': signal,
                        'strength': 2,
                        'breakout': 'none',
                        'position': position,
                        'width': channel_width
                    }
                    
            elif high_slope < -0.0001:  # Negative slope (descending)
                channel_type = 'descending_channel'
                if current_price > expected_upper:
                    # Bullish breakout above descending channel
                    self.patterns[channel_type] = {
                        'signal': 1,  # Bullish
                        'strength': 4,
                        'breakout': 'above',
                        'width': channel_width
                    }
                elif current_price < expected_lower:
                    # Bearish breakdown below descending channel
                    self.patterns[channel_type] = {
                        'signal': -1,  # Very bearish
                        'strength': 5,
                        'breakout': 'below',
                        'width': channel_width
                    }
                else:
                    # Inside channel
                    position = (current_price - expected_lower) / (expected_upper - expected_lower)
                    signal = 1 if position < 0.3 else -1 if position > 0.7 else 0
                    self.patterns[channel_type] = {
                        'signal': signal,
                        'strength': 2,
                        'breakout': 'none',
                        'position': position,
                        'width': channel_width
                    }
                    
            else:  # Horizontal channel (trading range)
                channel_type = 'horizontal_channel'
                if current_price > expected_upper:
                    # Bullish breakout above horizontal channel
                    self.patterns[channel_type] = {
                        'signal': 1,  # Bullish
                        'strength': 4,
                        'breakout': 'above',
                        'width': channel_width
                    }
                elif current_price < expected_lower:
                    # Bearish breakdown below horizontal channel
                    self.patterns[channel_type] = {
                        'signal': -1,  # Bearish
                        'strength': 4,
                        'breakout': 'below',
                        'width': channel_width
                    }
                else:
                    # Inside channel
                    position = (current_price - expected_lower) / (expected_upper - expected_lower)
                    signal = 1 if position < 0.3 else -1 if position > 0.7 else 0
                    self.patterns[channel_type] = {
                        'signal': signal,
                        'strength': 2,
                        'breakout': 'none',
                        'position': position,
                        'width': channel_width
                    }
    
    def detect_rectangle(self):
        """Detect rectangle pattern (horizontal trading range)"""
        # Get enough swing points
        swing_highs, swing_lows = self.find_swing_points()
        
        # Need at least 2 swing points of each type
        if len(swing_highs) < 2 or len(swing_lows) < 2:
            return
        
        # Get the last few swing highs and lows
        high_prices = [point[2] for point in swing_highs[-3:]]
        low_prices = [point[2] for point in swing_lows[-3:]]
        
        # Check if the highs and lows are relatively horizontal (flat)
        high_range = max(high_prices) - min(high_prices)
        low_range = max(low_prices) - min(low_prices)
        
        avg_high = sum(high_prices) / len(high_prices)
        avg_low = sum(low_prices) / len(low_prices)
        
        # The price range should be relatively flat (horizontal)
        if high_range / avg_high < self.threshold and low_range / avg_low < self.threshold:
            # Rectangle pattern detected
            # Check for breakout/breakdown
            current_price = self.df['close'].iloc[-1]
            
            if current_price > avg_high + (high_range / 2):
                # Bullish breakout above rectangle
                self.patterns['rectangle'] = {
                    'signal': 1,  # Bullish
                    'strength': 4,
                    'breakout': 'above',
                    'upper': avg_high,
                    'lower': avg_low
                }
            elif current_price < avg_low - (low_range / 2):
                # Bearish breakdown below rectangle
                self.patterns['rectangle'] = {
                    'signal': -1,  # Bearish
                    'strength': 4,
                    'breakout': 'below',
                    'upper': avg_high,
                    'lower': avg_low
                }
            else:
                # Inside rectangle
                position = (current_price - avg_low) / (avg_high - avg_low)
                signal = 1 if position < 0.3 else -1 if position > 0.7 else 0
                self.patterns['rectangle'] = {
                    'signal': signal,
                    'strength': 2,
                    'breakout': 'none',
                    'position': position,
                    'upper': avg_high,
                    'lower': avg_low
                }
    
    def detect_cup_and_handle(self):
        """Detect Cup and Handle pattern (bullish continuation)"""
        # Get enough data points
        if len(self.df) < 60:
            return
        
        # Cup and handle requires a prior uptrend
        # Check if the trend before the cup is up
        lookback = min(100, len(self.df))
        early_section = self.df.iloc[-lookback:-lookback//2]
        prior_uptrend = early_section['close'].iloc[-1] > early_section['close'].iloc[0] * 1.05
        
        if not prior_uptrend:
            return
        
        # Find potential cup formation
        # Cup should be U-shaped, not V-shaped
        recent_section = self.df.iloc[-lookback//2:]
        
        # Find the lowest point (bottom of the cup)
        min_idx = recent_section['low'].idxmin()
        min_price = recent_section.loc[min_idx, 'low']
        
        # Get left and right edges of the cup
        left_section = self.df.loc[:min_idx]
        right_section = self.df.loc[min_idx:]
        
        # Find a recent high before the cup that matches the current level
        left_peak_idx = left_section['high'].idxmax()
        left_peak = left_section.loc[left_peak_idx, 'high']
        
        # Find the right peak (should be close to left peak in height)
        right_peak_candidates = right_section[right_section['high'] > min_price * 1.1]
        if len(right_peak_candidates) == 0:
            return
        
        right_peak_idx = right_peak_candidates.index[0]
        for idx in right_peak_candidates.index:
            if abs(right_section.loc[idx, 'high'] - left_peak) < abs(right_section.loc[right_peak_idx, 'high'] - left_peak):
                right_peak_idx = idx
        
        right_peak = right_section.loc[right_peak_idx, 'high']
        
        # Check if peaks are close enough
        if abs(left_peak - right_peak) / left_peak > self.threshold:
            return
        
        # Check for handle formation (small pullback after right peak)
        handle_section = self.df.loc[right_peak_idx:]
        if len(handle_section) < 5:
            return
        
        handle_low_idx = handle_section['low'].idxmin()
        handle_low = handle_section.loc[handle_low_idx, 'low']
        
        # Handle should be a shallow pullback
        max_handle_depth = (right_peak - min_price) * 0.5
        if right_peak - handle_low > max_handle_depth:
            return
        
        # Check if current price is breaking above the right peak (handle completion)
        current_price = self.df['close'].iloc[-1]
        
        if current_price > right_peak:
            # Pattern confirmed with breakout
            self.patterns['cup_and_handle'] = {
                'signal': 1,  # Bullish
                'strength': 4,
                'left_peak': self.df.index[self.df.index.get_loc(left_peak_idx)],
                'bottom': self.df.index[self.df.index.get_loc(min_idx)],
                'right_peak': self.df.index[self.df.index.get_loc(right_peak_idx)],
                'handle_low': self.df.index[self.df.index.get_loc(handle_low_idx)],
                'target': right_peak + (right_peak - min_price)  # Measured move based on cup depth
            }
    
    def detect_flags(self):
        """Detect bull and bear flag patterns"""
        # Need enough data points
        if len(self.df) < 20:
            return
        
        # Check for flag pole (sharp move)
        recent_data = self.df.iloc[-20:]
        
        # Split data into potential pole and flag sections
        pole_section = recent_data.iloc[:10]
        flag_section = recent_data.iloc[10:]
        
        # Calculate price moves
        pole_price_change = pole_section['close'].iloc[-1] - pole_section['close'].iloc[0]
        pole_price_change_pct = pole_price_change / pole_section['close'].iloc[0]
        
        # Significant price move for the pole (at least 5%)
        if abs(pole_price_change_pct) > 0.05:
            # Check flag pattern (consolidation)
            flag_high = flag_section['high'].max()
            flag_low = flag_section['low'].min()
            flag_range = flag_high - flag_low
            
            # Flag should have narrow range (consolidation)
            if flag_range / flag_section['close'].mean() < 0.1:
                # Check if flag is in the right direction
                if pole_price_change > 0:  # Bullish pole
                    # Bull flags should have slight downward or sideways slope
                    flag_price_change = flag_section['close'].iloc[-1] - flag_section['close'].iloc[0]
                    if flag_price_change <= 0:
                        # Check for breakout above flag
                        current_price = self.df['close'].iloc[-1]
                        if current_price > flag_high:
                            # Bull flag confirmed with breakout
                            self.patterns['bull_flag'] = {
                                'signal': 1,  # Bullish
                                'strength': 4,
                                'pole_start': recent_data.index[0],
                                'pole_end': pole_section.index[-1],
                                'flag_high': flag_high,
                                'flag_low': flag_low,
                                'target': current_price + abs(pole_price_change)  # Target based on pole height
                            }
                else:  # Bearish pole
                    # Bear flags should have slight upward or sideways slope
                    flag_price_change = flag_section['close'].iloc[-1] - flag_section['close'].iloc[0]
                    if flag_price_change >= 0:
                        # Check for breakdown below flag
                        current_price = self.df['close'].iloc[-1]
                        if current_price < flag_low:
                            # Bear flag confirmed with breakdown
                            self.patterns['bear_flag'] = {
                                'signal': -1,  # Bearish
                                'strength': 4,
                                'pole_start': recent_data.index[0],
                                'pole_end': pole_section.index[-1],
                                'flag_high': flag_high,
                                'flag_low': flag_low,
                                'target': current_price - abs(pole_price_change)  # Target based on pole height
                            }
    
    def get_latest_patterns(self):
        """Get the latest detected chart patterns"""
        if not self.patterns:
            self.detect_all_patterns()
        
        return self.patterns
    
    def get_pattern_signals(self):
        """Get trading signals from detected chart patterns"""
        if not self.patterns:
            self.detect_all_patterns()
        
        buy_patterns = []
        sell_patterns = []
        
        for pattern_name, pattern_data in self.patterns.items():
            if pattern_data['signal'] == 1:
                buy_patterns.append({
                    'pattern': pattern_name.replace('_', ' ').title(),
                    'strength': pattern_data['strength']
                })
            elif pattern_data['signal'] == -1:
                sell_patterns.append({
                    'pattern': pattern_name.replace('_', ' ').title(),
                    'strength': pattern_data['strength']
                })
        
        return {
            'buy': buy_patterns,
            'sell': sell_patterns
        }


class TelegramSender:
    """Send messages to Telegram"""
    
    def __init__(self, token, chat_id):
        """
        Initialize with Telegram bot token and chat ID
        
        Args:
            token: Telegram bot token
            chat_id: Telegram chat ID to send messages to
        """
        self.token = token
        self.chat_id = chat_id
        self.logger = logging.getLogger("TradingBot.TelegramSender")
    
    async def send_message(self, message, parse_mode='MarkdownV2'):
        """
        Send message to Telegram
        
        Args:
            message: Message text to send
            parse_mode: Message format (MarkdownV2, HTML, None)
            
        Returns:
            True if message was sent successfully, False otherwise
        """
        try:
            # Create the bot
            bot = telegram.Bot(token=self.token)
            
            # Send the message
            await bot.send_message(
                chat_id=self.chat_id,
                text=message,
                parse_mode=parse_mode
            )
            
            return True
        except Exception as e:
            self.logger.error(f"Error sending Telegram message: {e}")
            
            # Try again without parse mode if the error is related to formatting
            if "can't parse entities" in str(e) and parse_mode:
                try:
                    bot = telegram.Bot(token=self.token)
                    await bot.send_message(
                        chat_id=self.chat_id,
                        text=f"⚠️ Formatting error, sending without formatting:\n\n{message}",
                        parse_mode=None
                    )
                    return True
                except Exception as e2:
                    self.logger.error(f"Error sending unformatted message: {e2}")
            
            return False


class TradingSignalBot:
    """Main trading signal bot class"""
    
    def __init__(self, config=None):
        """
        Initialize the trading signal bot
        
        Args:
            config: Configuration object or None to use default
        """
        self.config = config or Config()
        self.logger = setup_logging()
        self.logger.info("Initializing Trading Signal Bot")
        
        # Initialize clients
        self.upstox = None
        self.telegram = None
    
    def initialize_clients(self):
        """Initialize API clients"""
        # Initialize Upstox client
        try:
            self.upstox = UpstoxClient(self.config)
            self.upstox.authenticate()
            self.logger.info("Upstox client initialized successfully")
        except Exception as e:
            self.logger.error(f"Error initializing Upstox client: {e}")
            raise APIConnectionError("Upstox", "Failed to initialize client", e)
        
        # Initialize Telegram sender
        if self.config.TELEGRAM_BOT_TOKEN and self.config.TELEGRAM_CHAT_ID:
            self.telegram = TelegramSender(
                token=self.config.TELEGRAM_BOT_TOKEN,
                chat_id=self.config.TELEGRAM_CHAT_ID
            )
            self.logger.info("Telegram sender initialized successfully")
        else:
            self.logger.warning("Telegram credentials not provided, notifications disabled")
    
    async def run(self):
        """Run the trading signal bot"""
        try:
            # Initialize clients
            self.initialize_clients()
            
            # Load stock list
            stock_list = self.config.STOCK_LIST
            if not stock_list:
                self.logger.error("No stocks in stock list, exiting")
                return False
            
            self.logger.info(f"Analyzing {len(stock_list)} stocks")
            
            # Set up time frame
            to_date = datetime.datetime.now().strftime("%Y-%m-%d")
            from_date = (datetime.datetime.now() - datetime.timedelta(days=self.config.SHORT_TERM_LOOKBACK)).strftime("%Y-%m-%d")
            
            # Analyze each stock
            for instrument_key in stock_list:
                try:
                    # Get stock details
                    stock_details = self.upstox.get_instrument_details(instrument_key)
                    stock_name = stock_details.get('name', 'Unknown')
                    stock_symbol = stock_details.get('tradingsymbol', instrument_key)
                    
                    self.logger.info(f"Analyzing {stock_name} ({stock_symbol})")
                    
                    # Set up interval
                    interval = self.config.INTERVALS['short_term']
                    
                    # Analyze stock
                    signal_data = await self._analyze_stock(
                        instrument_key=instrument_key,
                        stock_name=stock_name,
                        stock_symbol=stock_symbol,
                        interval=interval,
                        from_date=from_date,
                        to_date=to_date,
                        timeframe='Daily'
                    )
                    
                    # Check if a signal was generated and send it
                    if signal_data and signal_data.get('signal') != 'NEUTRAL':
                        signal_message = self._format_signal_message(
                            stock_name=stock_name,
                            stock_symbol=stock_symbol,
                            signals=signal_data,
                            timeframe='Daily'
                        )
                        
                        # Send message via Telegram
                        if self.telegram:
                            await self.telegram.send_message(signal_message)
                    
                except Exception as e:
                    self.logger.error(f"Error analyzing {instrument_key}: {e}")
            
            self.logger.info("Analysis completed successfully")
            return True
            
        except Exception as e:
            self.logger.error(f"Error running trading signal bot: {e}")
            return False
    
    async def _analyze_stock(self, instrument_key, stock_name, stock_symbol, interval, from_date, to_date, timeframe):
        """
        Analyze a stock for trading signals
        
        Args:
            instrument_key: Instrument identifier
            stock_name: Name of the stock
            stock_symbol: Symbol of the stock
            interval: Time interval (1D, 1W, etc.)
            from_date: Start date (YYYY-MM-DD)
            to_date: End date (YYYY-MM-DD)
            timeframe: Human-readable timeframe (Daily, Weekly)
            
        Returns:
            Dictionary with signal data or None if error
        """
        try:
            # Fetch historical data
            df = self.upstox.get_historical_data(
                instrument_key=instrument_key,
                interval=interval,
                from_date=from_date,
                to_date=to_date
            )
            
            if df.empty or len(df) < 20:
                self.logger.warning(f"Insufficient data for {stock_symbol}, skipping")
                return None
            
            # Perform technical analysis
            ta = TechnicalIndicators(df, self.config.INDICATORS)
            ta.calculate_all()
            
            # Get overall signal from technical indicators
            indicator_signals = ta.get_overall_signal()
            
            # Perform candlestick pattern analysis
            cp = CandlestickPatterns(df, self.config.CANDLESTICK_PATTERNS)
            pattern_signals = cp.get_pattern_signals()
            
            # Perform chart pattern analysis
            chart = ChartPatterns(df, self.config.CHART_PATTERNS)
            chart_signals = chart.get_pattern_signals()
            
            # Combine signals
            buy_signals = pattern_signals.get('buy', []) + chart_signals.get('buy', [])
            sell_signals = pattern_signals.get('sell', []) + chart_signals.get('sell', [])
            
            # Calculate total signal strength
            buy_strength = sum(signal['strength'] for signal in buy_signals)
            sell_strength = sum(signal['strength'] for signal in sell_signals)
            
            # Determine final signal
            signal_type = indicator_signals.get('signal', 'NEUTRAL')
            
            # Override if pattern signals are strong enough and agree with indicator signal
            if buy_strength > sell_strength and buy_strength >= 6:
                if signal_type == 'BUY' or signal_type == 'NEUTRAL':
                    signal_type = 'BUY'
                    indicator_signals['strength'] = max(indicator_signals.get('strength', 0), 
                                                    min(5, round(buy_strength / len(buy_signals)) if buy_signals else 0))
            elif sell_strength > buy_strength and sell_strength >= 6:
                if signal_type == 'SELL' or signal_type == 'NEUTRAL':
                    signal_type = 'SELL'
                    indicator_signals['strength'] = max(indicator_signals.get('strength', 0),
                                                    min(5, round(sell_strength / len(sell_signals)) if sell_signals else 0))
            
            # Update the signal
            indicator_signals['signal'] = signal_type
            indicator_signals['pattern_signals'] = {
                'buy': buy_signals,
                'sell': sell_signals
            }
            
            # Get detailed analysis
            indicator_signals['detailed_analysis'] = ta.get_detailed_analysis()
            
            # Add industry info if available
            indicator_signals['industry'] = 'N/A'
            
            return indicator_signals
            
        except Exception as e:
            self.logger.error(f"Error analyzing {stock_symbol}: {e}")
            return None
    
    def _format_signal_message(self, stock_name, stock_symbol, signals, timeframe):
        """
        Format a signal message for Telegram
        
        Args:
            stock_name: Name of the stock
            stock_symbol: Symbol of the stock
            signals: Signal data dictionary
            timeframe: Human-readable timeframe (Daily, Weekly)
            
        Returns:
            Formatted message text
        """
        # Escape text for Telegram's MarkdownV2 format
        stock_name_esc = escape_telegram_markdown(stock_name)
        stock_symbol_esc = escape_telegram_markdown(stock_symbol)
        
        # Signal type and strength
        signal_type = signals.get('signal', 'NEUTRAL')
        signal_strength = signals.get('strength', 0)
        
        # Current price
        current_price = signals.get('current_price', 0)
        current_price_esc = escape_telegram_markdown(f"{current_price:.2f}")
        
        # Industry info
        industry = signals.get('industry', 'N/A')
        industry_esc = escape_telegram_markdown(industry)
        
        # Support and resistance
        support = signals.get('support')
        resistance = signals.get('resistance')
        
        # Stop loss and target
        stop_loss = signals.get('stop_loss')
        target_price = signals.get('target')
        
        # Primary indicators
        primary_indicators = []
        for indicator_name, indicator_data in signals.get('indicators', {}).items():
            if indicator_data.get('signal', 0) != 0:
                indicator_direction = "Bullish" if indicator_data['signal'] > 0 else "Bearish"
                primary_indicators.append(f"{indicator_name.replace('_', ' ').title()}: {indicator_direction}")
        
        primary_indicators_text = "\n".join(primary_indicators) if primary_indicators else "No clear signals"
        primary_indicators_esc = escape_telegram_markdown(primary_indicators_text)
        
        # Pattern signals
        patterns = []
        if signal_type == 'BUY':
            pattern_signals = signals.get('pattern_signals', {}).get('buy', [])
        else:
            pattern_signals = signals.get('pattern_signals', {}).get('sell', [])
        
        for pattern in pattern_signals:
            patterns.append(f"{pattern['pattern']} ({'⭐' * min(pattern['strength'], 5)})")
        
        patterns_text = "\n".join(patterns) if patterns else "No significant patterns detected"
        patterns_esc = escape_telegram_markdown(patterns_text)
        
        # Support/resistance and stop loss text
        if signal_type == 'BUY':
            if support:
                support_text = f"Support: {support:.2f}"
            else:
                support_text = "Support: Not identified"
                
            if stop_loss:
                stop_loss_text = f"Stop Loss: {stop_loss:.2f}"
            else:
                stop_loss_text = "Stop Loss: Use ATR-based stop (2-3% below entry)"
        else:
            if resistance:
                support_text = f"Resistance: {resistance:.2f}"
            else:
                support_text = "Resistance: Not identified"
                
            if stop_loss:
                stop_loss_text = f"Stop Loss: {stop_loss:.2f}"
            else:
                stop_loss_text = "Stop Loss: Use ATR-based stop (2-3% above entry)"
        
        support_text_esc = escape_telegram_markdown(support_text)
        stop_loss_esc = escape_telegram_markdown(stop_loss_text)
        
        # Target price
        if target_price:
            target_text = f"Target: {target_price:.2f}"
        else:
            if signal_type == 'BUY':
                target_text = "Target: Aim for 2:1 risk-reward ratio"
            else:
                target_text = "Target: Aim for 2:1 risk-reward ratio"
        
        target_esc = escape_telegram_markdown(target_text)
        
        # Trend strength
        trend_strength = "Very Strong" if signal_strength >= 5 else "Strong" if signal_strength >= 4 else "Moderate" if signal_strength >= 3 else "Weak"
        trend_strength_esc = escape_telegram_markdown(f"Signal Strength: {trend_strength} ({signal_strength}/5)")
        
        # Buy/sell summary
        buy_count = len(signals.get('pattern_signals', {}).get('buy', []))
        sell_count = len(signals.get('pattern_signals', {}).get('sell', []))
        
        if signal_type == 'BUY':
            buy_sell_summary = f"Bullish signals: {buy_count}, Bearish signals: {sell_count}"
        else:
            buy_sell_summary = f"Bearish signals: {sell_count}, Bullish signals: {buy_count}"
            
        buy_sell_summary_esc = escape_telegram_markdown(buy_sell_summary)
        
        # Detailed analysis
        detailed_analysis = signals.get('detailed_analysis', '')
        detailed_analysis_esc = escape_telegram_markdown(detailed_analysis)
        
        # Current timestamp
        timestamp = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        timestamp_short = datetime.datetime.now().strftime("%b-%d %H:%M")
        timestamp_esc = escape_telegram_markdown(timestamp_short)
        
        # Format the message using template from config
        message_template = self.config.SIGNAL_MESSAGE_TEMPLATE
        
        message = message_template.format(
            stock_name=stock_name_esc,
            stock_symbol=stock_symbol_esc,
            current_price=current_price_esc,
            industry=industry_esc,
            signal_type=signal_type,
            signal_strength=signal_strength,
            timeframe=escape_telegram_markdown(timeframe),
            primary_indicators=primary_indicators_esc,
            patterns=patterns_esc,
            stop_loss=stop_loss_esc,
            target_price=target_esc,
            trend_strength=trend_strength_esc,
            buy_sell_summary=buy_sell_summary_esc,
            detailed_analysis=detailed_analysis_esc,
            timestamp_short=timestamp_esc
        )
        
        return message


# ===============================================================
# Main Application Entry Point
# ===============================================================

async def main():
    """Main async function to run the bot"""
    # Set up logging
    logger = setup_logging()
    logger.info(f"Starting Trading Signal Bot - Version 1.0.0")
    
    try:
        # Create and run the bot
        bot = TradingSignalBot()
        result = await bot.run()
        
        # Log result
        if result:
            logger.info("Bot execution completed successfully")
        else:
            logger.error("Bot execution failed")
        
        return result
    
    except Exception as e:
        logger.error(f"Unhandled exception in main: {e}")
        return False


if __name__ == "__main__":
    # Run the async main function
    try:
        import asyncio
        asyncio.run(main())
    except KeyboardInterrupt:
        print("\nBot terminated by user")
    except Exception as e:
        print(f"Error running bot: {e}")
