from datetime import UTC, date, datetime

import httpx

from echoloom.temporal_context import (
    DailyEnvironmentProvider,
    ShenzhenWeatherProvider,
    current_time_context,
    holiday_day_info,
    holiday_day_summary,
    message_time_context,
)


def test_current_time_context_is_distinct_from_event_time():
    current = current_time_context(
        now=datetime(2026, 10, 30, 1, 23, tzinfo=UTC),
    )
    assert "2026-10-30 09:23" in current
    assert "时段：上午" in current
    assert "消息时间仍表示消息实际发生时间" in current


def test_hook_uptime_tick_maps_to_shenzhen_local_minute_and_labels():
    now_utc = datetime(2026, 10, 29, 2, 4, 45, tzinfo=UTC)
    context = message_time_context(
        9_940_000, now_utc=now_utc, now_tick_ms=10_000_000,
    )
    assert context == {
        "timezone": "Asia/Shanghai",
        "local_minute": "2026-10-29 10:03",
        "time_period": "上午",
    }


def test_message_time_has_holiday_day_index_and_makeup_workday():
    national_day = message_time_context(
        10_000, now_utc=datetime(2026, 10, 2, 16, 30, tzinfo=UTC),
        now_tick_ms=10_000,
    )
    assert national_day["local_minute"] == "2026-10-03 00:30"
    assert national_day["time_period"] == "深夜"
    assert set(national_day) == {"timezone", "local_minute", "time_period"}

    makeup = message_time_context(
        10_000, now_utc=datetime(2026, 2, 13, 16, 0, tzinfo=UTC),
        now_tick_ms=10_000,
    )
    assert makeup["local_minute"] == "2026-02-14 00:00"
    assert set(makeup) == {"timezone", "local_minute", "time_period"}


def test_holiday_summary_handles_current_and_upcoming_holiday():
    active = holiday_day_info(date(2026, 2, 17))
    assert active["holiday"] == "春节"
    assert active["holiday_day"] == 3
    assert "春节假期第 3/9 天" in holiday_day_summary(date(2026, 2, 17))
    summary = holiday_day_summary(date(2026, 10, 29))
    assert "元旦还有 64 天" in summary
    assert "距上次国庆节假期结束" not in summary
    assert "工作" not in summary
    future = holiday_day_info(date(2027, 2, 6))
    assert future["day_type"] == "节日当天（假期调休安排待公布）"


def test_shenzhen_weather_is_parsed_and_cached_once_per_local_day():
    requests = []

    def forecast(request: httpx.Request) -> httpx.Response:
        requests.append(request)
        params = dict(request.url.params)
        assert params["latitude"] == "22.5431"
        assert params["longitude"] == "114.0579"
        assert params["timezone"] == "Asia/Shanghai"
        assert "current" in params and "daily" in params
        assert "apikey" not in params
        return httpx.Response(200, json={
            "current": {
                "time": "2026-10-29T10:00", "temperature_2m": 24.3,
                "relative_humidity_2m": 73, "apparent_temperature": 26.1,
                "precipitation": 0, "weather_code": 2, "wind_speed_10m": 8.2,
            },
            "daily": {
                "time": ["2026-10-29"], "weather_code": [3],
                "temperature_2m_min": [21], "temperature_2m_max": [28.5],
                "precipitation_probability_max": [15],
            },
        })

    provider = ShenzhenWeatherProvider(transport=httpx.MockTransport(forecast))
    now = datetime(2026, 10, 29, 10, 5, tzinfo=UTC)
    first = provider.summary_for_today(now=now)
    second = provider.summary_for_today(now=now)
    assert first == second
    assert "深圳当前天气" in first
    assert "局部多云" in first
    assert "今日预报 阴" in first
    assert "24.3°C" in first and "26.1°C" in first
    assert len(requests) == 1


def test_daily_prompt_combines_date_holiday_and_weather():
    provider = DailyEnvironmentProvider(
        weather=ShenzhenWeatherProvider(transport=httpx.MockTransport(
            lambda _: httpx.Response(503),
        )),
    )
    prompt = provider.system_prompt_context(
        now=datetime(2026, 10, 29, 1, 0, tzinfo=UTC),
    )
    assert "今天是2026-10-29" in prompt
    assert "星期四" not in prompt and "工作日" not in prompt
    assert "元旦还有 64 天" in prompt
    assert "假期结束后已工作" not in prompt
    assert "深圳天气暂不可用" in prompt
