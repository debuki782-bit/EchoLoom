"""Per-message local time plus once-daily holiday and optional weather context."""

from __future__ import annotations

import ctypes
import os
import threading
from dataclasses import dataclass
from datetime import UTC, date, datetime, timedelta, timezone
from typing import Any
from zoneinfo import ZoneInfo, ZoneInfoNotFoundError

import httpx

from . import local_config
from .prompt_catalog import fill_runtime, runtime_templates

SHENZHEN_TZ = timezone(timedelta(hours=8), name="Asia/Shanghai")


def configured_timezone() -> tuple[str, Any]:
    """Return configured IANA timezone name and tzinfo (defaults to Asia/Shanghai when unset)."""
    tz_name = local_config.load_local_runtime_config().location.timezone
    if not tz_name:
        return "Asia/Shanghai", SHENZHEN_TZ
    if tz_name.upper() == "UTC":
        return "UTC", UTC
    try:
        return tz_name, ZoneInfo(tz_name)
    except (ZoneInfoNotFoundError, ValueError):
        return "Asia/Shanghai", SHENZHEN_TZ


@dataclass(frozen=True)
class HolidayPeriod:
    name: str
    start: date
    end: date
    official_schedule: bool = True


@dataclass(frozen=True)
class HolidayMarker:
    name: str
    day: date
    official_schedule: bool


# 2026 dates follow the State Council General Office's published adjustment plan.
_HOLIDAY_PERIODS = (
    HolidayPeriod("元旦", date(2026, 1, 1), date(2026, 1, 3)),
    HolidayPeriod("春节", date(2026, 2, 15), date(2026, 2, 23)),
    HolidayPeriod("清明节", date(2026, 4, 4), date(2026, 4, 6)),
    HolidayPeriod("劳动节", date(2026, 5, 1), date(2026, 5, 5)),
    HolidayPeriod("端午节", date(2026, 6, 19), date(2026, 6, 21)),
    HolidayPeriod("中秋节", date(2026, 9, 25), date(2026, 9, 27)),
    HolidayPeriod("国庆节", date(2026, 10, 1), date(2026, 10, 7)),
)
_ADJUSTED_WORKDAYS = {
    date(2026, 1, 4): "元旦调休",
    date(2026, 2, 14): "春节调休",
    date(2026, 2, 28): "春节调休",
    date(2026, 5, 9): "劳动节调休",
    date(2026, 9, 20): "国庆节调休",
    date(2026, 10, 10): "国庆节调休",
}
# 2027 statutory festival dates are known, but the State Council's days-off and
# make-up-work plan is not represented as confirmed until it is officially issued.
_FESTIVAL_MARKERS = (
    HolidayMarker("元旦", date(2027, 1, 1), False),
    HolidayMarker("春节", date(2027, 2, 6), False),
    HolidayMarker("清明节", date(2027, 4, 5), False),
    HolidayMarker("劳动节", date(2027, 5, 1), False),
    HolidayMarker("端午节", date(2027, 6, 9), False),
    HolidayMarker("中秋节", date(2027, 9, 15), False),
    HolidayMarker("国庆节", date(2027, 10, 1), False),
)


def estimate_message_datetime(capture_tick_ms: int, *,
                              now_utc: datetime | None = None,
                              now_tick_ms: int | None = None) -> datetime:
    """Map Hook GetTickCount64 milliseconds to an approximate UTC event time."""
    current_utc = now_utc or datetime.now(UTC)
    if current_utc.tzinfo is None:
        raise ValueError("now_utc must be timezone-aware")
    if now_tick_ms is None:
        now_tick_ms = _get_tick_count64_ms()
    if now_tick_ms is None or capture_tick_ms < 0 or capture_tick_ms > now_tick_ms + 2_000:
        # Non-Windows diagnostics and synthetic callbacks can only use receive time.
        return current_utc.astimezone(UTC)
    elapsed_ms = max(0, now_tick_ms - capture_tick_ms)
    return current_utc.astimezone(UTC) - timedelta(milliseconds=elapsed_ms)


def _get_tick_count64_ms() -> int | None:
    if os.name != "nt":
        return None
    kernel32 = ctypes.WinDLL("kernel32", use_last_error=True)
    get_tick_count64 = kernel32.GetTickCount64
    get_tick_count64.restype = ctypes.c_ulonglong
    get_tick_count64.argtypes = []
    return int(get_tick_count64())


def message_time_context(capture_tick_ms: int, *, now_utc: datetime | None = None,
                         now_tick_ms: int | None = None) -> dict[str, str | int | None]:
    tz_name, tz_info = configured_timezone()
    event_time = estimate_message_datetime(capture_tick_ms, now_utc=now_utc,
                                           now_tick_ms=now_tick_ms)
    local = event_time.astimezone(tz_info)
    return {
        "timezone": tz_name,
        "local_minute": local.strftime("%Y-%m-%d %H:%M"),
        "time_period": _time_period(local.hour),
    }


def current_time_context(*, now: datetime | None = None) -> str:
    """Render the real current local time separately from message event times."""
    _, tz_info = configured_timezone()
    current = now or datetime.now(tz_info)
    if current.tzinfo is None:
        current = current.replace(tzinfo=tz_info)
    local = current.astimezone(tz_info)
    return fill_runtime(
        runtime_templates()["current_time"],
        timestamp=local.strftime("%Y-%m-%d %H:%M"),
        period=_time_period(local.hour),
    )


def _time_period(hour: int) -> str:
    if hour >= 23 or hour < 3:
        return "深夜"
    if hour < 7:
        return "凌晨"
    if hour < 9:
        return "早晨"
    if hour < 12:
        return "上午"
    if hour < 14:
        return "中午"
    if hour < 18:
        return "下午"
    if hour < 23:
        return "夜晚"
    return "深夜"


def holiday_day_info(day: date) -> dict[str, str | int | None]:
    for period in _HOLIDAY_PERIODS:
        if period.start <= day <= period.end:
            index = (day - period.start).days + 1
            return {"day_type": "法定节假日", "holiday": period.name,
                    "holiday_day": index, "holiday_length": (period.end - period.start).days + 1}
    if day in _ADJUSTED_WORKDAYS:
        return {"day_type": f"{_ADJUSTED_WORKDAYS[day]}（工作日）", "holiday": None,
                "holiday_day": None, "holiday_length": None}
    for marker in _FESTIVAL_MARKERS:
        if day == marker.day:
            return {"day_type": "节日当天（假期调休安排待公布）", "holiday": marker.name,
                    "holiday_day": None, "holiday_length": None}
    if day.weekday() >= 5:
        return {"day_type": "周末休息日", "holiday": None,
                "holiday_day": None, "holiday_length": None}
    return {"day_type": "工作日", "holiday": None,
            "holiday_day": None, "holiday_length": None}


def _next_holiday(day: date) -> tuple[date, str, bool] | None:
    options: list[tuple[date, str, bool]] = []
    options.extend((period.start, period.name, period.official_schedule)
                   for period in _HOLIDAY_PERIODS if period.start > day)
    options.extend((marker.day, marker.name, marker.official_schedule)
                   for marker in _FESTIVAL_MARKERS if marker.day > day)
    return min(options, default=None, key=lambda item: item[0])


def holiday_day_summary(day: date) -> str:
    status = holiday_day_info(day)
    summaries: list[str] = []
    if (status["holiday"] and isinstance(status["holiday_day"], int)
            and isinstance(status["holiday_length"], int)):
        left = status["holiday_length"] - status["holiday_day"]
        summaries.append(
            f"今天是{status['holiday']}假期第 {status['holiday_day']}/"
            f"{status['holiday_length']} 天，假期还剩 {left} 天。"
        )
    next_holiday = _next_holiday(day)
    if next_holiday is None:
        summaries.append("已配置节假日日历没有更晚的节日数据；不要推测节假日安排。")
    else:
        holiday_date, name, official = next_holiday
        days = (holiday_date - day).days
        certainty = "国务院已公布放假安排" if official else "节日日期已知，放假调休安排待国务院公布"
        summaries.append(f"距离{name}还有 {days} 天（{holiday_date.isoformat()}；{certainty}）。")

    return "".join(summaries)


_WMO_CN = {
    0: "晴", 1: "大部晴朗", 2: "局部多云", 3: "阴",
    45: "雾", 48: "冻雾", 51: "毛毛雨", 53: "毛毛雨", 55: "毛毛雨",
    56: "冻毛毛雨", 57: "冻毛毛雨", 61: "小雨", 63: "中雨", 65: "大雨",
    66: "冻雨", 67: "冻雨", 71: "小雪", 73: "中雪", 75: "大雪", 77: "雪粒",
    80: "小阵雨", 81: "中阵雨", 82: "强阵雨", 85: "小阵雪", 86: "强阵雪",
    95: "雷暴", 96: "雷暴伴小冰雹", 99: "雷暴伴大冰雹",
}


class LocalWeatherProvider:
    """Daily in-memory cache over the public Open-Meteo API for a configured city."""

    URL = "https://api.open-meteo.com/v1/forecast"

    def __init__(
        self,
        *,
        city: str = "",
        latitude: float = 0.0,
        longitude: float = 0.0,
        timezone_name: str = "Asia/Shanghai",
        enabled: bool | None = None,
        transport: httpx.BaseTransport | None = None,
    ):
        self.city = city.strip()
        self.latitude = float(latitude)
        self.longitude = float(longitude)
        self.timezone_name = timezone_name or "Asia/Shanghai"
        self.enabled = bool(self.city) if enabled is None else bool(enabled and self.city)
        try:
            self._tz: Any = ZoneInfo(self.timezone_name)
        except (ZoneInfoNotFoundError, ValueError):
            self._tz = SHENZHEN_TZ
        self._transport = transport
        self._lock = threading.Lock()
        self._cached_day: date | None = None
        self._cached_text: str | None = None
        self._retry_after: datetime | None = None

    @classmethod
    def from_local_config(
        cls,
        *,
        transport: httpx.BaseTransport | None = None,
    ) -> LocalWeatherProvider:
        loc = local_config.load_local_runtime_config().location
        if not loc.weather_enabled or not loc.weather_city:
            return cls(enabled=False, transport=transport)
        tz_name = loc.timezone or "Asia/Shanghai"
        return cls(
            city=loc.weather_city,
            latitude=loc.weather_latitude,
            longitude=loc.weather_longitude,
            timezone_name=tz_name,
            enabled=True,
            transport=transport,
        )

    def summary_for_today(self, *, now: datetime | None = None) -> str:
        if not self.enabled:
            return ""
        local_now = (now or datetime.now(self._tz)).astimezone(self._tz)
        today = local_now.date()
        with self._lock:
            if self._cached_day == today and self._cached_text is not None:
                return self._cached_text
            if (self._cached_day == today and self._retry_after is not None
                    and local_now < self._retry_after):
                return f"{self.city}天气暂不可用；不要猜测或编造天气。"
            try:
                summary = self._fetch(local_now)
            except (httpx.HTTPError, ValueError, TypeError, KeyError):
                self._cached_day = today
                self._cached_text = None
                self._retry_after = local_now + timedelta(minutes=30)
                return f"{self.city}天气暂不可用；不要猜测或编造天气。"
            self._cached_day = today
            self._cached_text = summary
            self._retry_after = None
            return summary

    def _fetch(self, now: datetime) -> str:
        params: dict[str, str | int | float | bool | None] = {
            "latitude": self.latitude,
            "longitude": self.longitude,
            "current": "temperature_2m,relative_humidity_2m,apparent_temperature,"
                       "precipitation,weather_code,wind_speed_10m",
            "daily": "weather_code,temperature_2m_max,temperature_2m_min,"
                     "precipitation_probability_max",
            "forecast_days": 1,
            "timezone": self.timezone_name,
        }
        with httpx.Client(timeout=5, trust_env=False, transport=self._transport) as client:
            response = client.get(self.URL, params=params)
            response.raise_for_status()
            data = response.json()
        current = data["current"]
        daily = data["daily"]
        description = _WMO_CN.get(int(current["weather_code"]), "天气状况未知")
        fields = [
            f"{self.city}当前天气（Open-Meteo，查询时间 {str(current['time'])[:16]}）：{description}",
            f"气温 {_number(current.get('temperature_2m'))}°C",
            f"体感 {_number(current.get('apparent_temperature'))}°C",
            f"湿度 {_number(current.get('relative_humidity_2m'))}%",
            f"降水 { _number(current.get('precipitation'))} mm",
            f"风速 {_number(current.get('wind_speed_10m'))} km/h",
        ]
        if daily.get("time") and daily["time"][0] == now.date().isoformat():
            forecast_code = int(daily["weather_code"][0])
            forecast = _WMO_CN.get(forecast_code, "天气状况未知")
            fields.append(
                f"今日预报 {forecast}，最低 {_number(daily['temperature_2m_min'][0])}°C，"
                f"最高 {_number(daily['temperature_2m_max'][0])}°C，"
                f"最高降水概率 {_number(daily['precipitation_probability_max'][0])}%"
            )
        return "；".join(fields) + "。"


class ShenzhenWeatherProvider(LocalWeatherProvider):
    """Backward-compatible preset preserved for legacy unit tests."""

    LATITUDE = 22.5431
    LONGITUDE = 114.0579

    def __init__(self, *, transport: httpx.BaseTransport | None = None):
        super().__init__(
            city="深圳",
            latitude=self.LATITUDE,
            longitude=self.LONGITUDE,
            timezone_name="Asia/Shanghai",
            enabled=True,
            transport=transport,
        )


def _number(value: Any) -> str:
    if value is None:
        return "未知"
    number = float(value)
    return str(int(number)) if number.is_integer() else f"{number:.1f}"


class DailyEnvironmentProvider:
    """Build one compact daily system-prompt block for the configured local timezone."""

    def __init__(self, weather: LocalWeatherProvider | None = None):
        self.weather = weather or LocalWeatherProvider.from_local_config()

    def system_prompt_context(self, *, now: datetime | None = None) -> str:
        _, tz_info = configured_timezone()
        local_now = (now or datetime.now(tz_info)).astimezone(tz_info)
        date_text = local_now.strftime("%Y-%m-%d")
        daily_date = f"今天是{date_text}。"
        parts = [daily_date, holiday_day_summary(local_now.date())]
        weather_text = self.weather.summary_for_today(now=local_now)
        if weather_text:
            parts.append(weather_text)
        return "\n".join(parts)
