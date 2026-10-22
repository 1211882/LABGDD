from __future__ import annotations

import numpy as np
import pandas as pd

FEATURE_VERSION = "features_v1"
FEATURE_COLUMNS = [
    "open", "high", "low", "close", "volume", "return_1m", "log_return",
    "return_5m", "return_15m", "return_30m", "sma_5", "sma_15", "sma_30",
    "ema_5", "ema_15", "ema_30", "momentum_5", "volatility_5",
    "volatility_15", "volatility_30", "atr_14", "rsi_14", "macd",
    "macd_signal", "macd_histogram", "bollinger_upper", "bollinger_lower",
    "bollinger_bandwidth", "volume_change", "volume_mean_30", "relative_volume",
    "hour", "minute_of_day", "minutes_since_open", "sector_return_mean",
    "relative_to_sector", "sector_return_dispersion",
]


def engineer_features(dataframe: pd.DataFrame) -> pd.DataFrame:
    frame = dataframe.copy()
    frame["timestamp"] = pd.to_datetime(frame["timestamp"], utc=True)
    frame = frame.sort_values(["symbol", "trading_date", "timestamp"]).reset_index(drop=True)
    groups = frame.groupby(["symbol", "trading_date"], sort=False, group_keys=False)
    close = frame["close"]
    previous = groups["close"].shift(1)
    frame["return_1m"] = close / previous - 1
    frame["log_return"] = np.log(close / previous)
    for horizon in (5, 15, 30):
        frame[f"return_{horizon}m"] = close / groups["close"].shift(horizon) - 1
        frame[f"sma_{horizon}"] = groups["close"].transform(lambda value, h=horizon: value.rolling(h, min_periods=h).mean())
        frame[f"ema_{horizon}"] = groups["close"].transform(lambda value, h=horizon: value.ewm(span=h, adjust=False, min_periods=h).mean())
        frame[f"volatility_{horizon}"] = groups["close"].transform(lambda value, h=horizon: value.pct_change(fill_method=None).rolling(h, min_periods=h).std(ddof=1))
    frame["momentum_5"] = close - groups["close"].shift(5)

    prior_close = groups["close"].shift(1)
    true_range = pd.concat([
        frame["high"] - frame["low"],
        (frame["high"] - prior_close).abs(),
        (frame["low"] - prior_close).abs(),
    ], axis=1).max(axis=1)
    frame["atr_14"] = true_range.groupby([frame["symbol"], frame["trading_date"]]).transform(lambda value: value.rolling(14, min_periods=14).mean())

    delta = close - previous
    gain = delta.clip(lower=0)
    loss = -delta.clip(upper=0)
    keys = [frame["symbol"], frame["trading_date"]]
    avg_gain = gain.groupby(keys).transform(lambda value: value.ewm(alpha=1 / 14, adjust=False, min_periods=14).mean())
    avg_loss = loss.groupby(keys).transform(lambda value: value.ewm(alpha=1 / 14, adjust=False, min_periods=14).mean())
    rs = avg_gain / avg_loss.replace(0, np.nan)
    frame["rsi_14"] = 100 - (100 / (1 + rs))
    frame.loc[(avg_loss == 0) & (avg_gain > 0), "rsi_14"] = 100.0

    ema12 = groups["close"].transform(lambda value: value.ewm(span=12, adjust=False, min_periods=12).mean())
    ema26 = groups["close"].transform(lambda value: value.ewm(span=26, adjust=False, min_periods=26).mean())
    frame["macd"] = ema12 - ema26
    frame["macd_signal"] = frame["macd"].groupby(keys).transform(lambda value: value.ewm(span=9, adjust=False, min_periods=9).mean())
    frame["macd_histogram"] = frame["macd"] - frame["macd_signal"]

    middle = groups["close"].transform(lambda value: value.rolling(20, min_periods=20).mean())
    deviation = groups["close"].transform(lambda value: value.rolling(20, min_periods=20).std(ddof=1))
    frame["bollinger_upper"] = middle + 2 * deviation
    frame["bollinger_lower"] = middle - 2 * deviation
    frame["bollinger_bandwidth"] = (frame["bollinger_upper"] - frame["bollinger_lower"]) / middle
    frame["volume_change"] = frame["volume"] / groups["volume"].shift(1).replace(0, np.nan) - 1
    frame["volume_mean_30"] = groups["volume"].transform(lambda value: value.rolling(30, min_periods=30).mean())
    frame["relative_volume"] = frame["volume"] / frame["volume_mean_30"].replace(0, np.nan)

    local = frame["timestamp"].dt.tz_convert("America/New_York")
    frame["hour"] = local.dt.hour.astype("int16")
    frame["minute_of_day"] = (local.dt.hour * 60 + local.dt.minute).astype("int16")
    frame["minutes_since_open"] = (frame["minute_of_day"] - 570).astype("int16")
    sector_time = frame.groupby(["sector", "timestamp"])["return_1m"]
    frame["sector_return_mean"] = sector_time.transform("mean")
    frame["relative_to_sector"] = frame["return_1m"] - frame["sector_return_mean"]
    frame["sector_return_dispersion"] = sector_time.transform("std")
    return frame


def ml_ready(dataframe: pd.DataFrame) -> tuple[pd.DataFrame, dict[str, int]]:
    before = len(dataframe)
    result = dataframe.dropna(subset=FEATURE_COLUMNS).copy()
    return result, {"input_rows": before, "warmup_rows_excluded": before - len(result), "output_rows": len(result)}
