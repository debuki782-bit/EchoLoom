"""Durable, conservative cost reservations before cloud model calls."""

from dataclasses import dataclass
from datetime import UTC, date, datetime, time, timedelta, timezone

from .ports import Clock
from .storage import Repository

DAILY_BUDGET_TIMEZONE = timezone(timedelta(hours=8), "Asia/Shanghai")
DAILY_ACCOUNT_BUDGET_MICROUSD = 4_000_000
TEXT_MODEL_DAILY_BUDGET_MICROUSD = 2_000_000
IMAGE_READ_DAILY_BUDGET_MICROUSD = 1_000_000
IMAGE_GENERATION_DAILY_BUDGET_MICROUSD = 1_000_000

_BUDGET_GROUP_FEATURES = {
    "text_model": ("adk_chat", "chat", "voice_transcription", "episode_embedding"),
    "image_read": ("adk_chat_image_context", "image_description", "file_description"),
    "image_generation": ("image_generate",),
}


def _day_bounds(day: date) -> tuple[str, str]:
    start_local = datetime.combine(day, time.min, tzinfo=DAILY_BUDGET_TIMEZONE)
    end_local = datetime.combine(day + timedelta(days=1), time.min,
                                 tzinfo=DAILY_BUDGET_TIMEZONE)
    return (
        start_local.astimezone(UTC).isoformat(),
        end_local.astimezone(UTC).isoformat(),
    )


@dataclass(frozen=True)
class ReserveResult:
    status: str
    month_utc: str | None = None


class UsageLedger:
    """Durable account-wide daily reservations, grouped by billable feature."""

    def __init__(self, repository: Repository, clock: Clock) -> None:
        self.repository = repository
        self.clock = clock
        with self.repository.transaction() as conn:
            conn.execute(
                "CREATE TABLE IF NOT EXISTS usage_budget_resets ("
                "account_id TEXT NOT NULL,budget_group TEXT NOT NULL,"
                "reset_at TEXT NOT NULL,reason TEXT NOT NULL,"
                "PRIMARY KEY(account_id,budget_group,reset_at))"
            )

    def reset_daily_group(self, account_id: str, budget_group: str, reason: str) -> str:
        """Start a fresh local daily feature-budget window without changing spend history."""
        if not account_id or budget_group not in _BUDGET_GROUP_FEATURES:
            raise ValueError("unknown account or budget group")
        if not isinstance(reason, str) or not reason.strip() or len(reason) > 200:
            raise ValueError("reason must be 1..200 characters")
        now = datetime.fromisoformat(self.clock.now())
        if now.tzinfo is None:
            raise ValueError("usage clock must be timezone-aware")
        reset_at = now.astimezone(UTC).isoformat()
        with self.repository.transaction() as conn:
            conn.execute(
                "INSERT INTO usage_budget_resets(account_id,budget_group,reset_at,reason) "
                "VALUES(?,?,?,?)", (account_id, budget_group, reset_at, reason.strip()),
            )
        return reset_at

    @staticmethod
    def _group_window_start(conn, account_id: str, budget_group: str,
                            day_start: str, day_end: str) -> str:
        row = conn.execute(
            "SELECT max(reset_at) FROM usage_budget_resets "
            "WHERE account_id=? AND budget_group=? AND reset_at>=? AND reset_at<?",
            (account_id, budget_group, day_start, day_end),
        ).fetchone()
        return row[0] or day_start

    def reserve(
        self, *, request_id: str, account_id: str, conversation_id: str,
        feature: str, model_id: str, maximum_microusd: int, budget_microusd: int,
        feature_budget_microusd: int | None = None,
        budget_group: str | None = None,
        budget_scope_id: str = "default",
        scope_budget_microusd: int | None = None,
        account_feature_budget_microusd: int | None = None,
    ) -> ReserveResult:
        if not all((request_id, account_id, conversation_id, feature, model_id,
                    budget_scope_id)):
            raise ValueError("usage identity fields must be nonempty")
        if (maximum_microusd <= 0 or budget_microusd < 0
                or (feature_budget_microusd is not None and feature_budget_microusd < 0)
                or (scope_budget_microusd is not None and scope_budget_microusd < 0)):
            raise ValueError("usage amounts are invalid")
        if account_feature_budget_microusd is not None and account_feature_budget_microusd < 0:
            raise ValueError("account feature budget is invalid")
        at = self.clock.now()
        moment = datetime.fromisoformat(at)
        if moment.tzinfo is None:
            raise ValueError("usage clock must be timezone-aware")
        month = moment.astimezone(UTC).strftime("%Y-%m")
        local_day = moment.astimezone(DAILY_BUDGET_TIMEZONE).date()
        day_start, day_end = _day_bounds(local_day)
        created_at = moment.astimezone(UTC).isoformat()
        group_features = None
        if budget_group is not None:
            group_features = _BUDGET_GROUP_FEATURES.get(budget_group)
            if group_features is None or feature not in group_features:
                raise ValueError("feature does not belong to budget group")
        with self.repository.transaction():
            conn = self.repository.conn
            existing = conn.execute(
                "SELECT account_id,conversation_id,feature,model_id,month_utc,"
                "reserved_microusd,state,budget_scope_id FROM usage_reservations "
                "WHERE request_id=?",
                (request_id,),
            ).fetchone()
            if existing is not None:
                same = (
                    existing["account_id"] == account_id
                    and existing["conversation_id"] == conversation_id
                    and existing["feature"] == feature
                    and existing["model_id"] == model_id
                    and existing["month_utc"] == month
                    and existing["reserved_microusd"] == maximum_microusd
                    and existing["budget_scope_id"] == budget_scope_id
                )
                return ReserveResult(
                    f"already_{existing['state']}" if same else "request_id_conflict", month
                )
            committed = conn.execute(
                "SELECT coalesce(sum(CASE state WHEN 'reserved' THEN reserved_microusd "
                "WHEN 'settled' THEN actual_microusd ELSE 0 END),0) "
                "FROM usage_reservations WHERE account_id=? AND created_at>=? AND created_at<?",
                (account_id, day_start, day_end),
            ).fetchone()[0]
            if committed + maximum_microusd > budget_microusd:
                return ReserveResult("budget_exhausted", month)
            if scope_budget_microusd is not None:
                scoped = conn.execute(
                    "SELECT coalesce(sum(CASE state WHEN 'reserved' THEN reserved_microusd "
                    "WHEN 'settled' THEN actual_microusd ELSE 0 END),0) "
                    "FROM usage_reservations WHERE account_id=? AND budget_scope_id=? "
                    "AND created_at>=? AND created_at<?",
                    (account_id, budget_scope_id, day_start, day_end),
                ).fetchone()[0]
                if scoped + maximum_microusd > scope_budget_microusd:
                    return ReserveResult("scope_budget_exhausted", month)
            if feature_budget_microusd is not None:
                features = group_features or (feature,)
                group_start = (self._group_window_start(
                    conn, account_id, budget_group, day_start, day_end,
                ) if budget_group is not None else day_start)
                placeholders = ",".join("?" for _ in features)
                feature_committed = conn.execute(
                    "SELECT coalesce(sum(CASE state WHEN 'reserved' THEN reserved_microusd "
                    "WHEN 'settled' THEN actual_microusd ELSE 0 END),0) "
                    "FROM usage_reservations WHERE account_id=? AND budget_scope_id=? "
                    "AND created_at>=? "
                    f"AND created_at<? AND feature IN ({placeholders})",
                    (account_id, budget_scope_id, group_start, day_end, *features),
                ).fetchone()[0]
                if feature_committed + maximum_microusd > feature_budget_microusd:
                    return ReserveResult("feature_budget_exhausted", month)
            if account_feature_budget_microusd is not None:
                features = group_features or (feature,)
                group_start = (self._group_window_start(
                    conn, account_id, budget_group, day_start, day_end,
                ) if budget_group is not None else day_start)
                placeholders = ",".join("?" for _ in features)
                account_feature_spend = conn.execute(
                    "SELECT coalesce(sum(CASE state WHEN 'reserved' THEN reserved_microusd "
                    "WHEN 'settled' THEN actual_microusd ELSE 0 END),0) "
                    "FROM usage_reservations WHERE account_id=? AND created_at>=? "
                    f"AND created_at<? AND feature IN ({placeholders})",
                    (account_id, group_start, day_end, *features),
                ).fetchone()[0]
                if account_feature_spend + maximum_microusd > account_feature_budget_microusd:
                    return ReserveResult("account_feature_budget_exhausted", month)
            conn.execute(
                "INSERT INTO usage_reservations "
                "(request_id,account_id,conversation_id,feature,model_id,month_utc,"
                "reserved_microusd,actual_microusd,state,created_at,updated_at,budget_scope_id) "
                "VALUES (?,?,?,?,?,?,?,NULL,'reserved',?,?,?)",
                (request_id, account_id, conversation_id, feature, model_id,
                 month, maximum_microusd, created_at, created_at, budget_scope_id),
            )
            return ReserveResult("reserved", month)

    def settle(self, request_id: str, actual_microusd: int) -> str:
        if actual_microusd < 0:
            raise ValueError("actual usage cannot be negative")
        with self.repository.transaction():
            conn = self.repository.conn
            row = conn.execute(
                "SELECT state,reserved_microusd,actual_microusd "
                "FROM usage_reservations WHERE request_id=?", (request_id,),
            ).fetchone()
            if row is None:
                return "not_found"
            if row["state"] == "settled":
                return "duplicate" if row["actual_microusd"] == actual_microusd else "conflict"
            if row["state"] != "reserved":
                return "not_reserved"
            conn.execute(
                "UPDATE usage_reservations SET state='settled',actual_microusd=?,"
                "updated_at=? WHERE request_id=?",
                (actual_microusd, self.clock.now(), request_id),
            )
            return "over_reservation" if actual_microusd > row["reserved_microusd"] else "settled"

    def release(self, request_id: str) -> str:
        """Only call after the provider confirms no billable request was made."""
        with self.repository.transaction():
            conn = self.repository.conn
            row = conn.execute(
                "SELECT state FROM usage_reservations WHERE request_id=?", (request_id,),
            ).fetchone()
            if row is None:
                return "not_found"
            if row["state"] == "released":
                return "duplicate"
            if row["state"] != "reserved":
                return "not_reserved"
            conn.execute(
                "UPDATE usage_reservations SET state='released',updated_at=? "
                "WHERE request_id=?", (self.clock.now(), request_id),
            )
            return "released"

    def committed_microusd(self, account_id: str, month_utc: str) -> int:
        with self.repository.read_lock() as conn:
            value = conn.execute(
                "SELECT coalesce(sum(CASE state WHEN 'reserved' THEN reserved_microusd "
                "WHEN 'settled' THEN actual_microusd ELSE 0 END),0) "
                "FROM usage_reservations WHERE account_id=? AND month_utc=?",
                (account_id, month_utc),
            ).fetchone()[0]
        return int(value)

    def committed_daily_microusd(
        self, account_id: str, day_local: str, *, budget_group: str | None = None,
    ) -> int:
        """Return settled plus in-flight usage for a Shanghai-local calendar day."""
        day = date.fromisoformat(day_local)
        day_start, day_end = _day_bounds(day)
        query = (
            "SELECT coalesce(sum(CASE state WHEN 'reserved' THEN reserved_microusd "
            "WHEN 'settled' THEN actual_microusd ELSE 0 END),0) "
            "FROM usage_reservations WHERE account_id=? AND created_at>=? AND created_at<?"
        )
        params: tuple[object, ...] = (account_id, day_start, day_end)
        if budget_group is not None:
            features = _BUDGET_GROUP_FEATURES.get(budget_group)
            if features is None:
                raise ValueError("unknown budget group")
            with self.repository.read_lock() as conn:
                group_start = self._group_window_start(
                    conn, account_id, budget_group, day_start, day_end,
                )
            placeholders = ",".join("?" for _ in features)
            params = (account_id, group_start, day_end)
            query += f" AND feature IN ({placeholders})"
            params += features
        with self.repository.read_lock() as conn:
            value = conn.execute(query, params).fetchone()[0]
        return int(value)

    def _stale_reservation_rows(
        self,
        conn,
        account_id: str,
        day_local: str,
        *,
        min_age_seconds: float,
        budget_group: str | None = None,
    ) -> list[dict[str, object]]:
        if min_age_seconds < 0:
            raise ValueError("min_age_seconds must be non-negative")
        now_dt = datetime.fromisoformat(self.clock.now())
        if now_dt.tzinfo is None:
            raise ValueError("usage clock must be timezone-aware")
        day = date.fromisoformat(day_local)
        day_start, day_end = _day_bounds(day)
        query = (
            "SELECT request_id,conversation_id,feature,model_id,reserved_microusd,"
            "created_at,budget_scope_id FROM usage_reservations "
            "WHERE account_id=? AND state='reserved' AND created_at>=? AND created_at<?"
        )
        params: tuple[object, ...] = (account_id, day_start, day_end)
        if budget_group is not None:
            features = _BUDGET_GROUP_FEATURES.get(budget_group)
            if features is None:
                raise ValueError("unknown budget group")
            group_start = self._group_window_start(
                conn, account_id, budget_group, day_start, day_end,
            )
            placeholders = ",".join("?" for _ in features)
            query = (
                "SELECT request_id,conversation_id,feature,model_id,reserved_microusd,"
                "created_at,budget_scope_id FROM usage_reservations "
                "WHERE account_id=? AND state='reserved' AND created_at>=? AND created_at<? "
                f"AND feature IN ({placeholders})"
            )
            params = (account_id, group_start, day_end, *features)
        stale: list[dict[str, object]] = []
        for row in conn.execute(query + " ORDER BY created_at ASC", params).fetchall():
            created_dt = datetime.fromisoformat(str(row["created_at"]))
            if created_dt.tzinfo is None:
                continue
            age = (now_dt.astimezone(UTC) - created_dt.astimezone(UTC)).total_seconds()
            if age >= min_age_seconds:
                stale.append({
                    "request_id": str(row["request_id"]),
                    "conversation_id": str(row["conversation_id"]),
                    "feature": str(row["feature"]),
                    "model_id": str(row["model_id"]),
                    "reserved_microusd": int(row["reserved_microusd"]),
                    "created_at": str(row["created_at"]),
                    "budget_scope_id": str(row["budget_scope_id"]),
                    "age_seconds": round(age, 1),
                })
        return stale

    def stale_reservations(
        self,
        account_id: str,
        day_local: str,
        *,
        min_age_seconds: float = 300.0,
        budget_group: str | None = None,
    ) -> list[dict[str, object]]:
        """List un-settled `reserved` rows older than `min_age_seconds` on `day_local`."""
        with self.repository.read_lock() as conn:
            return self._stale_reservation_rows(
                conn,
                account_id,
                day_local,
                min_age_seconds=min_age_seconds,
                budget_group=budget_group,
            )

    def release_stale_reservations(
        self,
        account_id: str,
        day_local: str,
        *,
        min_age_seconds: float = 300.0,
        budget_group: str | None = None,
        reason: str = "",
    ) -> int:
        """Explicitly release aged `reserved` rows without disturbing `settled` spend."""
        _ = reason
        now_iso = self.clock.now()
        with self.repository.transaction() as conn:
            stale = self._stale_reservation_rows(
                conn,
                account_id,
                day_local,
                min_age_seconds=min_age_seconds,
                budget_group=budget_group,
            )
            if not stale:
                return 0
            released = 0
            for item in stale:
                cursor = conn.execute(
                    "UPDATE usage_reservations SET state='released',updated_at=? "
                    "WHERE request_id=? AND state='reserved'",
                    (now_iso, item["request_id"]),
                )
                released += int(cursor.rowcount)
            return released
