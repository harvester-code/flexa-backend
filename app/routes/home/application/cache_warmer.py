"""
Cache Warmer

Background service that pre-computes S3 caches for all active scenarios.
Handles two scenarios:
  1. Code deployment — new _CODE_HASH invalidates every cache
  2. Simulation completion — new parquet makes the cache stale

Runs once at server startup then repeats on a configurable interval.
"""

import asyncio
import os
from typing import List, Optional, Tuple

import pandas as pd
from loguru import logger
from sqlalchemy import select

from app.routes.home.application.core.home_analyzer import HomeAnalyzer
from app.routes.home.application.core.timeline_builder import build_passenger_timelines
from app.routes.home.application.service import _CODE_HASH
from app.routes.home.infra.repository import HomeRepository
from app.routes.simulation.infra.models import ScenarioInformation
from packages.aws.s3.s3_manager import S3Manager
from packages.supabase.database import AsyncSessionLocal

TIMELINE_CACHE = f"passenger-timelines-{_CODE_HASH}.json"
STATIC_CACHE = f"home-static-response-{_CODE_HASH}.json"

_COUNTRY_AIRPORTS_PATH = os.getenv(
    "COUNTRY_TO_AIRPORTS_PATH",
    os.path.join(os.path.dirname(__file__), "country_to_airports.json"),
)


async def _get_active_scenario_ids() -> List[str]:
    async with AsyncSessionLocal() as session:
        result = await session.execute(
            select(ScenarioInformation.scenario_id).where(
                ScenarioInformation.has_simulation_data.is_(True),
                ScenarioInformation.is_active.is_(True),
            )
        )
        return [row[0] for row in result.fetchall()]


async def _check_staleness(
    scenario_id: str, repo: HomeRepository
) -> Tuple[bool, bool]:
    """Return (timeline_stale, static_stale) with minimal S3 calls."""
    t_valid = await repo.is_cache_valid(scenario_id, TIMELINE_CACHE)
    s_valid = await repo.is_cache_valid(scenario_id, STATIC_CACHE)
    return (not t_valid, not s_valid)


async def _warm_scenario(
    scenario_id: str,
    repo: HomeRepository,
    need_timeline: bool,
    need_static: bool,
) -> Tuple[bool, bool]:
    """Warm one scenario, loading parquet/metadata at most once."""
    pax_df: Optional[pd.DataFrame] = None
    metadata: Optional[dict] = None
    t_ok = False
    s_ok = False

    if need_timeline:
        pax_df = await repo.load_simulation_parquet(scenario_id)
        if pax_df is not None:
            metadata = await repo.load_metadata(scenario_id, "metadata-for-frontend.json")
            result = build_passenger_timelines(pax_df, metadata)
            t_ok = await repo.save_cached_response(scenario_id, TIMELINE_CACHE, result)
            if t_ok:
                await repo.delete_old_caches(scenario_id, "passenger-timelines-", TIMELINE_CACHE)

    if need_static:
        if pax_df is None:
            pax_df = await repo.load_simulation_parquet(scenario_id)
        if metadata is None and pax_df is not None:
            metadata = await repo.load_metadata(scenario_id, "metadata-for-frontend.json")
        if pax_df is not None:
            process_flow = None
            if metadata:
                pf = metadata.get("process_flow")
                process_flow = pf if isinstance(pf, list) else None
            calculator = HomeAnalyzer(
                pax_df,
                process_flow=process_flow,
                country_to_airports_path=_COUNTRY_AIRPORTS_PATH,
            )
            result = {
                "flow_chart": calculator.get_flow_chart_data(),
                "histogram": calculator.get_histogram_data(),
                "sankey_diagram": calculator.get_sankey_diagram_data(),
            }
            s_ok = await repo.save_cached_response(scenario_id, STATIC_CACHE, result)
            if s_ok:
                await repo.delete_old_caches(scenario_id, "home-static-response-", STATIC_CACHE)

    return (t_ok, s_ok)


async def warm_all_caches() -> None:
    """Check every active scenario and rebuild stale/missing caches."""
    try:
        scenario_ids = await _get_active_scenario_ids()
    except Exception as e:
        logger.error(f"[WARMER] Failed to query scenarios: {e}")
        return

    if not scenario_ids:
        logger.debug("[WARMER] No active scenarios with simulation data")
        return

    logger.info(f"[WARMER] Scanning {len(scenario_ids)} scenarios (hash={_CODE_HASH})")
    repo = HomeRepository(s3_manager=S3Manager())
    warmed = 0
    skipped = 0

    for sid in scenario_ids:
        try:
            need_t, need_s = await _check_staleness(sid, repo)
            if not need_t and not need_s:
                skipped += 1
                continue

            t, s = await _warm_scenario(sid, repo, need_t, need_s)
            if t or s:
                warmed += 1
                logger.info(f"[WARMER] Built {sid} (timeline={t}, static={s})")
        except Exception as e:
            logger.error(f"[WARMER] Failed {sid}: {e}")

    if warmed > 0:
        logger.info(f"[WARMER] Done — {warmed} rebuilt, {skipped} up-to-date")
    else:
        logger.info(f"[WARMER] Done — all {skipped} scenarios up-to-date")


async def run_periodic_warmer(interval_seconds: int = 300) -> None:
    """Background loop: warm caches on startup then every *interval_seconds*."""
    await asyncio.sleep(5)
    logger.info(f"[WARMER] Cache warmer started (interval={interval_seconds}s)")
    while True:
        try:
            await warm_all_caches()
        except Exception as e:
            logger.error(f"[WARMER] Run failed: {e}")
        await asyncio.sleep(interval_seconds)
