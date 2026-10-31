#!/usr/bin/env python3
"""
Universal Multi-Platform ATS Live Job Extractor.

Extracts live job postings and precise posting timestamps across all major ATS platforms:
- Greenhouse
- Lever
- Ashby
- Workable
- SmartRecruiters
- Recruitee
- Breezy HR
- Rippling
- BambooHR
- Pinpoint

Features:
- High-concurrency async fetching with per-host rate limiting (prevents 429s).
- Direct extraction of exact job publication & update timestamps.
- Normalized ISO 8601 UTC timestamps + raw timestamp preservation.
- Resilient SQLite storage + automated CSV export.
- Can ingest single slug, platform-specific CSVs, or master multi-ATS CSVs.
"""

import sys
import os
import csv
import json
import asyncio
import argparse
import sqlite3
import html
import re
import time
import random
import email.utils
import glob
from datetime import datetime, timezone, timedelta
from urllib.parse import urlparse
from typing import List, Dict, Any, Optional, Tuple

try:
    import aiohttp
    HAS_AIOHTTP = True
except ImportError:
    HAS_AIOHTTP = False

# Ensure local packages (scrapers, matching) are importable
_CURR_DIR = os.path.dirname(os.path.abspath(__file__))
if _CURR_DIR not in sys.path:
    sys.path.insert(0, _CURR_DIR)

try:
    from scrapers.job_extraction.engine import extract_job
    from scrapers.job_extraction.jd_enricher import parse_salary, detect_work_mode
    from matching.jd_facts import extract_jd_facts
    HAS_ENRICHMENT = True
except Exception:
    HAS_ENRICHMENT = False


# US State identification regex & sets
US_STATES = {
    "AL": "Alabama", "AK": "Alaska", "AZ": "Arizona", "AR": "Arkansas", "CA": "California",
    "CO": "Colorado", "CT": "Connecticut", "DE": "Delaware", "FL": "Florida", "GA": "Georgia",
    "HI": "Hawaii", "ID": "Idaho", "IL": "Illinois", "IN": "Indiana", "IA": "Iowa",
    "KS": "Kansas", "KY": "Kentucky", "LA": "Louisiana", "ME": "Maine", "MD": "Maryland",
    "MA": "Massachusetts", "MI": "Michigan", "MN": "Minnesota", "MS": "Mississippi", "MO": "Missouri",
    "MT": "Montana", "NE": "Nebraska", "NV": "Nevada", "NH": "New Hampshire", "NJ": "New Jersey",
    "NM": "New Mexico", "NY": "New York", "NC": "North Carolina", "ND": "North Dakota", "OH": "Ohio",
    "OK": "Oklahoma", "OR": "Oregon", "PA": "Pennsylvania", "RI": "Rhode Island", "SC": "South Carolina",
    "SD": "South Dakota", "TN": "Tennessee", "TX": "Texas", "UT": "Utah", "VT": "Vermont",
    "VA": "Virginia", "WA": "Washington", "WV": "West Virginia", "WI": "Wisconsin", "WY": "Wyoming",
    "DC": "District of Columbia", "PR": "Puerto Rico",
}
US_STATE_CODES = set(US_STATES.keys())
US_STATE_NAMES = {name.lower(): code for code, name in US_STATES.items()}

US_CITIES_SAMPLE = {
    "new york", "san francisco", "los angeles", "chicago", "austin", "seattle", "boston",
    "denver", "atlanta", "dallas", "houston", "san diego", "san jose", "miami", "philadelphia",
    "phoenix", "portland", "salt lake city", "raleigh", "charlotte", "minneapolis", "nashville"
}

NON_US_COUNTRIES = {
    "united kingdom", "london", "canada", "germany", "berlin", "france", "paris",
    "spain", "madrid", "barcelona", "italy", "milan", "rome", "india", "bangalore",
    "hyderabad", "pune", "mumbai", "delhi", "australia", "sydney", "melbourne",
    "netherlands", "amsterdam", "brazil", "poland", "warsaw", "ukraine", "kyiv",
    "denmark", "copenhagen", "sweden", "stockholm", "finland", "helsinki",
    "ireland", "dublin", "japan", "tokyo", "singapore", "mexico", "austria", "vienna",
    "belgium", "antwerp", "brussels", "greece", "athens", "portugal", "lisbon",
    "switzerland", "zurich", "norway", "oslo", "czech", "prague", "philippines",
    "manila", "argentina", "buenos aires", "colombia", "bogota", "chile", "santiago",
    "israel", "tel aviv", "new zealand", "auckland", "south africa", "emea", "apac", "latam"
}


def is_us_location(loc_input: Any, extra_text: str = "") -> bool:
    """
    Robust heuristic to determine if a job is US-based or US-Remote.
    Checks location string, job title, and office names.
    """
    if isinstance(loc_input, dict):
        loc_str = " ".join(str(v) for v in loc_input.values() if v)
    else:
        loc_str = str(loc_input or "")

    clean = loc_str.lower().strip()
    combined = f"{loc_str} {extra_text}".lower().strip()

    # If explicitly foreign and doesn't mention US
    has_foreign = any(re.search(r"\b" + re.escape(fc) + r"\b", combined) for fc in NON_US_COUNTRIES)
    has_explicit_us = any(u in combined for u in ["united states", ", us", "usa", "(us)", "- us", "us -"])

    if has_foreign and not has_explicit_us:
        return False

    # Direct US indicators
    if clean in ("us", "usa", "united states", "united states of america"):
        return True
    if has_explicit_us:
        return True
    if any(tag in combined for tag in ["us remote", "remote - us", "remote, us", "remote (us)", "remote us"]):
        return True

    # Generic remote for US companies (without non-US tags)
    if clean in ("remote", "work from home", "anywhere"):
        return True

    # State code pattern e.g. "San Francisco, CA" or "Austin, TX"
    match = re.search(r",\s*([a-zA-Z]{2})(?:\s+|$|\b)", combined)
    if match:
        code = match.group(1).upper()
        if code in US_STATE_CODES:
            return True

    # State name match
    for st_name in US_STATE_NAMES:
        if re.search(r"\b" + re.escape(st_name) + r"\b", combined):
            return True

    # Major US tech cities
    for city in US_CITIES_SAMPLE:
        if re.search(r"\b" + re.escape(city) + r"\b", combined):
            return True

    return False


def parse_to_iso(val: Any) -> str:
    """Standardizes various ATS timestamp formats into an ISO 8601 UTC string."""
    if not val:
        return ""
    # Epoch milliseconds (Lever style)
    if isinstance(val, (int, float)):
        try:
            return datetime.fromtimestamp(val / 1000.0, tz=timezone.utc).isoformat()
        except Exception:
            return ""

    s = str(val).strip()
    if not s:
        return ""

    # Recruitee format: "2026-09-23 19:02:33 UTC"
    if s.endswith(" UTC"):
        s = s[:-4].replace(" ", "T") + "Z"

    # Try ISO parser
    try:
        dt = datetime.fromisoformat(s.replace("Z", "+00:00"))
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return dt.astimezone(timezone.utc).isoformat()
    except Exception:
        # Match YYYY-MM-DD
        m = re.match(r"^(\d{4}-\d{2}-\d{2})", s)
        if m:
            try:
                dt = datetime.fromisoformat(m.group(1)).replace(tzinfo=timezone.utc)
                return dt.isoformat()
            except Exception:
                pass
        return s


def clean_html(raw_html: str) -> str:
    """Unescapes HTML entities and strips basic tags for plaintext storage."""
    if not raw_html:
        return ""
    unescaped = html.unescape(raw_html)
    # Strip scripts & styles
    clean = re.sub(r"<(script|style)[^>]*>.*?</\1>", "", unescaped, flags=re.DOTALL | re.IGNORECASE)
    # Convert <br>, <p>, <li> to newlines
    clean = re.sub(r"<(?:br|p|li|tr)[^>]*>", "\n", clean, flags=re.IGNORECASE)
    # Strip remaining HTML tags
    clean = re.sub(r"<[^>]+>", "", clean)
    # Collapse multiple whitespaces
    clean = re.sub(r"[ \t]+", " ", clean)
    clean = re.sub(r"\n\s*\n+", "\n\n", clean)
    return clean.strip()


class AsyncHTTPClient:
    """Asynchronous HTTP client with per-host token pacing, circuit-breaker backoff on 429, and exponential jitter."""
    def __init__(
        self,
        concurrency: int = 40,
        per_host_concurrency: int = 8,
        min_host_interval: float = 0.12,  # Enforce <= 8 req/sec max per host
        timeout: float = 18.0,
    ):
        self.semaphore = asyncio.Semaphore(concurrency)
        self.per_host_concurrency = per_host_concurrency
        self.min_host_interval = min_host_interval
        self.timeout = timeout
        self._host_semaphores: Dict[str, asyncio.Semaphore] = {}
        self._host_locks: Dict[str, asyncio.Lock] = {}
        self._host_last_request_time: Dict[str, float] = {}
        self._host_cooldown_until: Dict[str, float] = {}
        self._session = None

    def _get_host(self, url: str) -> str:
        try:
            return (urlparse(url).hostname or "default").lower()
        except Exception:
            return "default"

    def _get_host_sem(self, host: str) -> asyncio.Semaphore:
        if host not in self._host_semaphores:
            self._host_semaphores[host] = asyncio.Semaphore(self.per_host_concurrency)
        return self._host_semaphores[host]

    def _get_host_lock(self, host: str) -> asyncio.Lock:
        if host not in self._host_locks:
            self._host_locks[host] = asyncio.Lock()
        return self._host_locks[host]

    async def _wait_for_host(self, host: str):
        """Enforces host-wide cooldowns on 429 and paces inter-request intervals."""
        lock = self._get_host_lock(host)
        async with lock:
            now = time.monotonic()
            # 1. Cooldown check: if host encountered a 429/503, wait until cooldown expires
            cooldown = self._host_cooldown_until.get(host, 0.0)
            if now < cooldown:
                wait_s = cooldown - now + random.uniform(0.1, 0.5)
                await asyncio.sleep(wait_s)
                now = time.monotonic()

            # 2. Minimum inter-request pacing
            last_time = self._host_last_request_time.get(host, 0.0)
            elapsed = now - last_time
            if elapsed < self.min_host_interval:
                await asyncio.sleep(self.min_host_interval - elapsed)

            self._host_last_request_time[host] = time.monotonic()

    def _parse_retry_after(self, retry_after_val: Optional[str]) -> Optional[float]:
        if not retry_after_val:
            return None
        val_str = str(retry_after_val).strip()
        # Check integer seconds
        try:
            return max(1.0, float(val_str))
        except ValueError:
            pass
        # Check HTTP-date format
        try:
            dt = email.utils.parsedate_to_datetime(val_str)
            now_dt = datetime.now(timezone.utc)
            diff = (dt - now_dt).total_seconds()
            return max(1.0, diff)
        except Exception:
            return None

    async def __aenter__(self):
        if HAS_AIOHTTP:
            timeout_cfg = aiohttp.ClientTimeout(total=self.timeout)
            connector = aiohttp.TCPConnector(
                limit=self.semaphore._value,
                limit_per_host=self.per_host_concurrency,
                ssl=False,
            )
            self._session = aiohttp.ClientSession(
                timeout=timeout_cfg,
                connector=connector,
                headers={"User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36"},
            )
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        if self._session:
            await self._session.close()

    async def get_json(self, url: str, max_retries: int = 4) -> Tuple[int, Any]:
        """Performs async GET and parses JSON with host-wide circuit breaker and exponential jitter on 429/503."""
        host = self._get_host(url)
        host_sem = self._get_host_sem(host)

        for attempt in range(max_retries + 1):
            async with self.semaphore:
                async with host_sem:
                    await self._wait_for_host(host)

                    if not HAS_AIOHTTP or not self._session:
                        # Fallback for systems without aiohttp
                        import urllib.request
                        try:
                            req = urllib.request.Request(
                                url,
                                headers={"User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36"}
                            )
                            with urllib.request.urlopen(req, timeout=self.timeout) as resp:
                                status = resp.status
                                data = json.loads(resp.read().decode("utf-8", errors="ignore"))
                                return status, data
                        except urllib.error.HTTPError as e:
                            if e.code in (429, 503) and attempt < max_retries:
                                retry_after = self._parse_retry_after(e.headers.get("Retry-After"))
                                backoff = retry_after or (2.0 * (2 ** attempt) + random.uniform(0.5, 2.0))
                                backoff = min(60.0, max(2.0, backoff))
                                self._host_cooldown_until[host] = time.monotonic() + backoff
                                await asyncio.sleep(backoff)
                                continue
                            return e.code, None
                        except Exception:
                            return 0, None

                    try:
                        async with self._session.get(url) as resp:
                            if resp.status == 200:
                                try:
                                    data = await resp.json(content_type=None)
                                    return 200, data
                                except Exception:
                                    text = await resp.text()
                                    return 200, json.loads(text)
                            elif resp.status in (429, 503) and attempt < max_retries:
                                retry_after = self._parse_retry_after(resp.headers.get("Retry-After"))
                                backoff = retry_after or (2.5 * (2 ** attempt) + random.uniform(0.5, 2.0))
                                backoff = min(60.0, max(2.0, backoff))
                                # Set host-wide cooldown so all tasks hitting this host will pause
                                self._host_cooldown_until[host] = time.monotonic() + backoff
                                await asyncio.sleep(backoff)
                                continue
                            else:
                                return resp.status, None
                    except (asyncio.TimeoutError, aiohttp.ClientError):
                        if attempt < max_retries:
                            backoff = 1.5 * (attempt + 1) + random.uniform(0.2, 1.0)
                            await asyncio.sleep(backoff)
                            continue
                        return 0, None
                    except Exception:
                        return 0, None
        return 0, None


# Platform Extractors
async def extract_greenhouse(http: AsyncHTTPClient, slug: str, full_content: bool) -> Tuple[int, List[Dict[str, Any]]]:
    url = f"https://boards-api.greenhouse.io/v1/boards/{slug}/jobs"
    if full_content:
        url += "?content=true"
    status, data = await http.get_json(url)
    if status != 200 or not isinstance(data, dict):
        return status, []

    company_name = data.get("name") or slug.title()
    jobs = data.get("jobs") or []
    extracted = []
    now_iso = datetime.now(timezone.utc).isoformat()

    for j in jobs:
        loc_obj = j.get("location") or {}
        loc_str = loc_obj.get("name", "") if isinstance(loc_obj, dict) else str(loc_obj)
        depts = [d.get("name") for d in (j.get("departments") or []) if d.get("name")]
        offices = [o.get("name") for o in (j.get("offices") or []) if o.get("name")]

        first_pub = j.get("first_published") or ""
        updated = j.get("updated_at") or ""
        raw_ts = first_pub or updated

        content_raw = j.get("content", "") if full_content else ""
        extracted.append({
            "job_id": f"greenhouse_{slug}_{j.get('id')}",
            "platform": "greenhouse",
            "company_name": company_name,
            "company_slug": slug,
            "title": j.get("title", "").strip(),
            "location": loc_str.strip(),
            "is_us": 1 if is_us_location(loc_str, extra_text=f"{j.get('title', '')} {' '.join(offices)}") else 0,
            "apply_url": j.get("absolute_url", ""),
            "department": ", ".join(filter(None, depts)),
            "office": ", ".join(filter(None, offices)),
            "employment_type": "",
            "posted_at": parse_to_iso(first_pub) or parse_to_iso(updated),
            "updated_at": parse_to_iso(updated),
            "raw_timestamp": str(raw_ts),
            "content": clean_html(content_raw) if full_content else "",
            "extracted_at": now_iso,
        })
    return status, extracted


async def extract_lever(http: AsyncHTTPClient, slug: str, full_content: bool) -> Tuple[int, List[Dict[str, Any]]]:
    url = f"https://api.lever.co/v0/postings/{slug}?mode=json"
    status, data = await http.get_json(url)
    if status != 200 or not isinstance(data, list):
        return status, []

    company_name = slug.replace("-", " ").title()
    extracted = []
    now_iso = datetime.now(timezone.utc).isoformat()

    for j in data:
        if not isinstance(j, dict):
            continue
        cats = j.get("categories") or {}
        loc_str = cats.get("location", "")
        country = j.get("country", "")
        if country.upper() == "US" or not loc_str:
            loc_str = f"{loc_str}, US".strip(", ") if loc_str else "United States"

        dept = cats.get("team") or cats.get("department", "")
        emp_type = cats.get("commitment", "")
        workplace = j.get("workplaceType", "")

        created_ms = j.get("createdAt")
        posted_iso = parse_to_iso(created_ms)

        content_raw = (j.get("descriptionPlain") or j.get("description", "")) if full_content else ""
        extracted.append({
            "job_id": f"lever_{slug}_{j.get('id')}",
            "platform": "lever",
            "company_name": company_name,
            "company_slug": slug,
            "title": j.get("text", "").strip(),
            "location": loc_str.strip(),
            "is_us": 1 if (country.upper() == "US" or is_us_location(loc_str, extra_text=j.get("text", "")) or workplace == "remote") else 0,
            "apply_url": j.get("applyUrl") or j.get("hostedUrl", ""),
            "department": str(dept),
            "office": str(workplace),
            "employment_type": str(emp_type),
            "posted_at": posted_iso,
            "updated_at": posted_iso,
            "raw_timestamp": str(created_ms or ""),
            "content": clean_html(content_raw) if full_content else "",
            "extracted_at": now_iso,
        })
    return status, extracted


async def extract_ashby(http: AsyncHTTPClient, slug: str, full_content: bool) -> Tuple[int, List[Dict[str, Any]]]:
    url = f"https://api.ashbyhq.com/posting-api/job-board/{slug}"
    status, data = await http.get_json(url)
    if status != 200 or not isinstance(data, dict):
        return status, []

    company_name = data.get("organizationName") or slug.replace("-", " ").title()
    jobs = data.get("jobs") or data.get("jobPostings") or []
    extracted = []
    now_iso = datetime.now(timezone.utc).isoformat()

    for j in jobs:
        loc_str = j.get("location") or j.get("locationName") or ""
        addr = j.get("address") or ""
        is_remote = bool(j.get("isRemote"))
        workplace = "Remote" if is_remote else (j.get("workplaceType") or "")

        pub_at = j.get("publishedAt") or ""
        content_raw = (j.get("descriptionPlain") or j.get("descriptionHtml", "")) if full_content else ""

        extracted.append({
            "job_id": f"ashby_{slug}_{j.get('id')}",
            "platform": "ashby",
            "company_name": company_name,
            "company_slug": slug,
            "title": j.get("title", "").strip(),
            "location": loc_str.strip(),
            "is_us": 1 if (is_us_location(loc_str, extra_text=j.get("title", "")) or is_us_location(addr, extra_text=j.get("title", "")) or is_remote) else 0,
            "apply_url": j.get("applyUrl") or j.get("jobUrl", ""),
            "department": j.get("department") or j.get("team") or "",
            "office": workplace,
            "employment_type": j.get("employmentType", ""),
            "posted_at": parse_to_iso(pub_at),
            "updated_at": parse_to_iso(pub_at),
            "raw_timestamp": str(pub_at),
            "content": clean_html(content_raw) if full_content else "",
            "extracted_at": now_iso,
        })
    return status, extracted


async def extract_smartrecruiters(http: AsyncHTTPClient, slug: str, full_content: bool) -> Tuple[int, List[Dict[str, Any]]]:
    url = f"https://api.smartrecruiters.com/v1/companies/{slug}/postings"
    status, data = await http.get_json(url)
    if status != 200 or not isinstance(data, dict):
        return status, []

    jobs = data.get("content") or []
    extracted = []
    now_iso = datetime.now(timezone.utc).isoformat()

    for j in jobs:
        loc_obj = j.get("location") or {}
        c_code = (loc_obj.get("country") or "").upper()
        loc_str = ", ".join(filter(None, [loc_obj.get("city"), loc_obj.get("region"), c_code]))

        comp_name = (j.get("company") or {}).get("name") or slug.replace("-", " ").title()
        rel_date = j.get("releasedDate") or ""
        job_id = j.get("id") or j.get("uuid")

        extracted.append({
            "job_id": f"smartrecruiters_{slug}_{job_id}",
            "platform": "smartrecruiters",
            "company_name": comp_name,
            "company_slug": slug,
            "title": j.get("name", "").strip(),
            "location": loc_str.strip(),
            "is_us": 1 if (c_code == "US" or is_us_location(loc_str, extra_text=j.get("name", ""))) else 0,
            "apply_url": f"https://jobs.smartrecruiters.com/{slug}/{job_id}",
            "department": (j.get("department") or {}).get("label", ""),
            "office": "",
            "employment_type": (j.get("typeOfEmployment") or {}).get("label", ""),
            "posted_at": parse_to_iso(rel_date),
            "updated_at": parse_to_iso(rel_date),
            "raw_timestamp": str(rel_date),
            "content": "",
            "extracted_at": now_iso,
        })
    return status, extracted


async def extract_workable(http: AsyncHTTPClient, slug: str, full_content: bool) -> Tuple[int, List[Dict[str, Any]]]:
    url = f"https://apply.workable.com/api/v1/widget/accounts/{slug}"
    status, data = await http.get_json(url)
    if status != 200 or not isinstance(data, dict):
        return status, []

    company_name = data.get("name") or slug.replace("-", " ").title()
    jobs = data.get("jobs") or []
    extracted = []
    now_iso = datetime.now(timezone.utc).isoformat()

    for j in jobs:
        loc_str = ", ".join(filter(None, [j.get("city"), j.get("state"), j.get("country")]))
        is_tele = bool(j.get("telecommuting"))
        shortcode = j.get("shortcode") or j.get("code") or j.get("id")

        pub_on = j.get("published_on") or j.get("created_at") or ""
        extracted.append({
            "job_id": f"workable_{slug}_{shortcode}",
            "platform": "workable",
            "company_name": company_name,
            "company_slug": slug,
            "title": j.get("title", "").strip(),
            "location": loc_str.strip(),
            "is_us": 1 if (j.get("country") in ("United States", "US") or is_us_location(loc_str, extra_text=j.get("title", "")) or is_tele) else 0,
            "apply_url": j.get("application_url") or j.get("url") or f"https://apply.workable.com/{slug}/j/{shortcode}/",
            "department": j.get("department", ""),
            "office": "Remote" if is_tele else "",
            "employment_type": j.get("employment_type", ""),
            "posted_at": parse_to_iso(pub_on),
            "updated_at": parse_to_iso(pub_on),
            "raw_timestamp": str(pub_on),
            "content": "",
            "extracted_at": now_iso,
        })
    return status, extracted


async def extract_recruitee(http: AsyncHTTPClient, slug: str, full_content: bool) -> Tuple[int, List[Dict[str, Any]]]:
    url = f"https://{slug}.recruitee.com/api/offers"
    status, data = await http.get_json(url)
    if status != 200 or not isinstance(data, dict):
        return status, []

    offers = data.get("offers") or []
    extracted = []
    now_iso = datetime.now(timezone.utc).isoformat()

    for o in offers:
        loc_str = ", ".join(filter(None, [o.get("city"), o.get("country_code")])) or o.get("location", "")
        comp_name = o.get("company_name") or slug.replace("-", " ").title()
        is_remote = bool(o.get("remote"))
        workplace = "Remote" if is_remote else ("Hybrid" if o.get("hybrid") else "")

        pub_at = o.get("published_at") or o.get("created_at") or ""
        updated_at = o.get("updated_at") or ""
        content_raw = o.get("description", "") if full_content else ""

        extracted.append({
            "job_id": f"recruitee_{slug}_{o.get('id')}",
            "platform": "recruitee",
            "company_name": comp_name,
            "company_slug": slug,
            "title": o.get("title", "").strip(),
            "location": loc_str.strip(),
            "is_us": 1 if (is_us_location(loc_str, extra_text=o.get("title", "")) or o.get("country_code") in ("US", "USA") or is_remote) else 0,
            "apply_url": o.get("careers_apply_url") or o.get("careers_url", ""),
            "department": o.get("department", ""),
            "office": workplace,
            "employment_type": o.get("employment_type_code", ""),
            "posted_at": parse_to_iso(pub_at),
            "updated_at": parse_to_iso(updated_at),
            "raw_timestamp": str(pub_at),
            "content": clean_html(content_raw) if full_content else "",
            "extracted_at": now_iso,
        })
    return status, extracted


async def extract_breezy(http: AsyncHTTPClient, slug: str, full_content: bool) -> Tuple[int, List[Dict[str, Any]]]:
    url = f"https://{slug}.breezy.hr/json"
    status, data = await http.get_json(url)
    if status != 200 or not isinstance(data, list):
        return status, []

    company_name = slug.replace("-", " ").title()
    extracted = []
    now_iso = datetime.now(timezone.utc).isoformat()

    for j in data:
        if not isinstance(j, dict):
            continue
        loc_obj = j.get("location") or {}
        loc_str = loc_obj.get("name", "") if isinstance(loc_obj, dict) else str(loc_obj)
        type_obj = j.get("type") or {}
        emp_type = type_obj.get("name", "") if isinstance(type_obj, dict) else str(type_obj)

        pub_date = j.get("published_date") or ""
        job_id = j.get("id") or j.get("friendly_id")

        extracted.append({
            "job_id": f"breezy_{slug}_{job_id}",
            "platform": "breezy",
            "company_name": j.get("company") or company_name,
            "company_slug": slug,
            "title": j.get("name", "").strip(),
            "location": loc_str.strip(),
            "is_us": 1 if is_us_location(loc_str, extra_text=j.get("name", "")) else 0,
            "apply_url": j.get("url", ""),
            "department": j.get("department", ""),
            "office": "",
            "employment_type": emp_type,
            "posted_at": parse_to_iso(pub_date),
            "updated_at": parse_to_iso(pub_date),
            "raw_timestamp": str(pub_date),
            "content": "",
            "extracted_at": now_iso,
        })
    return status, extracted


async def extract_rippling(http: AsyncHTTPClient, slug: str, full_content: bool) -> Tuple[int, List[Dict[str, Any]]]:
    url = f"https://ats.rippling.com/api/v1/board/{slug}/jobs"
    status, data = await http.get_json(url)
    if status != 200 or not isinstance(data, list):
        return status, []

    company_name = slug.replace("-", " ").title()
    extracted = []
    now_iso = datetime.now(timezone.utc).isoformat()

    for j in data:
        if not isinstance(j, dict):
            continue
        work_loc = (j.get("workLocation") or {}).get("label", "")
        extracted.append({
            "job_id": f"rippling_{slug}_{j.get('uuid')}",
            "platform": "rippling",
            "company_name": company_name,
            "company_slug": slug,
            "title": j.get("name", "").strip(),
            "location": work_loc.strip(),
            "is_us": 1 if is_us_location(work_loc, extra_text=j.get("name", "")) else 0,
            "apply_url": j.get("url", ""),
            "department": (j.get("department") or {}).get("name", ""),
            "office": "",
            "employment_type": "",
            "posted_at": "",
            "updated_at": "",
            "raw_timestamp": "",
            "content": "",
            "extracted_at": now_iso,
        })
    return status, extracted


async def extract_bamboohr(http: AsyncHTTPClient, slug: str, full_content: bool) -> Tuple[int, List[Dict[str, Any]]]:
    url = f"https://{slug}.bamboohr.com/careers/list"
    status, data = await http.get_json(url)
    if status != 200 or not isinstance(data, dict):
        return status, []

    jobs = data.get("result") or []
    company_name = slug.replace("-", " ").title()
    extracted = []
    now_iso = datetime.now(timezone.utc).isoformat()

    for j in jobs:
        loc_obj = j.get("location") or {}
        loc_str = ", ".join(filter(None, [loc_obj.get("city"), loc_obj.get("state")]))
        is_remote = bool(j.get("isRemote"))
        job_id = j.get("id")

        extracted.append({
            "job_id": f"bamboohr_{slug}_{job_id}",
            "platform": "bamboohr",
            "company_name": company_name,
            "company_slug": slug,
            "title": j.get("jobOpeningName", "").strip(),
            "location": loc_str.strip(),
            "is_us": 1 if (is_us_location(loc_str, extra_text=j.get("jobOpeningName", "")) or is_remote) else 0,
            "apply_url": f"https://{slug}.bamboohr.com/careers/{job_id}",
            "department": j.get("departmentLabel", ""),
            "office": "Remote" if is_remote else "",
            "employment_type": j.get("employmentType", ""),
            "posted_at": "",
            "updated_at": "",
            "raw_timestamp": "",
            "content": "",
            "extracted_at": now_iso,
        })
    return status, extracted


async def extract_pinpoint(http: AsyncHTTPClient, slug: str, full_content: bool) -> Tuple[int, List[Dict[str, Any]]]:
    url = f"https://{slug}.pinpointhq.com/postings.json"
    status, data = await http.get_json(url)
    if status != 200 or not isinstance(data, dict):
        return status, []

    jobs = data.get("data") or []
    company_name = slug.replace("-", " ").title()
    extracted = []
    now_iso = datetime.now(timezone.utc).isoformat()

    for j in jobs:
        loc_obj = j.get("location") or {}
        loc_str = loc_obj.get("name", "") if isinstance(loc_obj, dict) else str(loc_obj)
        dept = ((j.get("job") or {}).get("department") or {}).get("name", "")
        deadline = j.get("deadline_at") or ""

        extracted.append({
            "job_id": f"pinpoint_{slug}_{j.get('id')}",
            "platform": "pinpoint",
            "company_name": company_name,
            "company_slug": slug,
            "title": j.get("title", "").strip(),
            "location": loc_str.strip(),
            "is_us": 1 if is_us_location(loc_str, extra_text=j.get("title", "")) else 0,
            "apply_url": j.get("url", ""),
            "department": dept,
            "office": j.get("workplace_type_text", ""),
            "employment_type": j.get("employment_type_text", ""),
            "posted_at": "",
            "updated_at": "",
            "raw_timestamp": str(deadline),
            "content": clean_html(j.get("description", "")) if full_content else "",
            "extracted_at": now_iso,
        })
    return status, extracted


PLATFORM_HANDLERS = {
    "greenhouse": extract_greenhouse,
    "lever": extract_lever,
    "ashby": extract_ashby,
    "smartrecruiters": extract_smartrecruiters,
    "workable": extract_workable,
    "recruitee": extract_recruitee,
    "breezy": extract_breezy,
    "rippling": extract_rippling,
    "bamboohr": extract_bamboohr,
    "pinpoint": extract_pinpoint,
}


def enrich_job_record(raw: Dict[str, Any], enable_enrichment: bool = True) -> Dict[str, Any]:
    """Enriches job dictionary with salary, skills, H-1B, and experience facts."""
    out = dict(raw)
    out.setdefault("company_name", "")
    out.setdefault("location", "")
    out.setdefault("apply_url", "")
    out.setdefault("department", "")
    out.setdefault("office", "")
    out.setdefault("employment_type", "")
    out.setdefault("posted_at", "")
    out.setdefault("updated_at", "")
    out.setdefault("raw_timestamp", "")
    out.setdefault("content", "")
    out.setdefault("salary", "")
    out.setdefault("salary_min", None)
    out.setdefault("salary_max", None)
    out.setdefault("salary_currency", "")
    out.setdefault("salary_period", "")
    out.setdefault("skills", "")
    out.setdefault("skills_required", "")
    out.setdefault("skills_preferred", "")
    out.setdefault("work_mode", "")
    out.setdefault("experience_min", None)
    out.setdefault("experience_max", None)
    out.setdefault("education", "")
    out.setdefault("h1b_status", "unknown")
    out.setdefault("visa_sponsorship_status", "unknown")
    out.setdefault("sponsorship_evidence", "")
    out.setdefault("reports_to", "")

    if not enable_enrichment or not HAS_ENRICHMENT:
        return out

    desc = raw.get("content") or raw.get("description") or ""
    title = raw.get("title") or ""
    location = raw.get("location") or ""
    company = raw.get("company_name") or ""
    platform = raw.get("platform") or ""

    if desc:
        # 1. Main JD extraction engine
        try:
            eng = extract_job({
                "title": title,
                "location": location,
                "description": desc,
                "company": company,
                "source_board": platform,
            })
            if eng:
                out["salary"] = eng.get("salary") or ""
                out["salary_min"] = eng.get("salary_min")
                out["salary_max"] = eng.get("salary_max")
                out["salary_currency"] = eng.get("salary_currency") or ""
                out["salary_period"] = eng.get("salary_period") or ""
                out["skills"] = eng.get("skills") or ""
                out["skills_required"] = ", ".join(eng.get("skills_required") or [])
                out["skills_preferred"] = ", ".join(eng.get("skills_preferred") or [])
                out["work_mode"] = eng.get("work_mode") or ""
                out["experience_min"] = eng.get("experience_min")
                out["experience_max"] = eng.get("experience_max")
                edu = eng.get("education_requirements") or []
                out["education"] = ", ".join(edu) if isinstance(edu, list) else str(edu)
        except Exception:
            pass

        # 2. Fallback salary parser
        if not out.get("salary_min") and not out.get("salary"):
            try:
                sal = parse_salary(desc)
                if sal.get("salary_min") or sal.get("salary_max"):
                    out["salary_min"] = sal.get("salary_min")
                    out["salary_max"] = sal.get("salary_max")
                    out["salary_currency"] = sal.get("salary_currency") or ""
                    out["salary_period"] = sal.get("salary_period") or ""
            except Exception:
                pass

        # 3. Work mode fallback
        if not out.get("work_mode"):
            try:
                wm = detect_work_mode(raw)
                if wm:
                    out["work_mode"] = wm.title()
            except Exception:
                pass

        # 4. JD Facts: H-1B, Visa Sponsorship, and Reporting Line
        try:
            facts = extract_jd_facts(desc)
            if facts:
                out["h1b_status"] = facts.h1b_status or "unknown"
                out["visa_sponsorship_status"] = facts.visa_sponsorship_status or "unknown"
                out["sponsorship_evidence"] = " | ".join(facts.sponsorship_evidence)
                out["reports_to"] = facts.reports_to or ""
        except Exception:
            pass
    else:
        # Without full description, still attempt basic work mode detection from location
        try:
            wm = detect_work_mode(raw)
            if wm:
                out["work_mode"] = wm.title()
        except Exception:
            pass

    return out


def init_db(db_path: str):
    conn = sqlite3.connect(db_path)
    cur = conn.cursor()
    cur.execute("PRAGMA journal_mode=WAL;")
    cur.execute("""
        CREATE TABLE IF NOT EXISTS live_jobs (
            job_id TEXT PRIMARY KEY,
            platform TEXT NOT NULL,
            company_name TEXT,
            company_slug TEXT NOT NULL,
            title TEXT NOT NULL,
            location TEXT,
            is_us INTEGER DEFAULT 0,
            apply_url TEXT,
            department TEXT,
            office TEXT,
            employment_type TEXT,
            posted_at TEXT,
            updated_at TEXT,
            raw_timestamp TEXT,
            content TEXT,
            salary TEXT,
            salary_min REAL,
            salary_max REAL,
            salary_currency TEXT,
            salary_period TEXT,
            skills TEXT,
            skills_required TEXT,
            skills_preferred TEXT,
            work_mode TEXT,
            experience_min REAL,
            experience_max REAL,
            education TEXT,
            h1b_status TEXT,
            visa_sponsorship_status TEXT,
            sponsorship_evidence TEXT,
            reports_to TEXT,
            extracted_at TEXT NOT NULL
        );
    """)

    # Auto-migrate table if created under an older schema version
    cur.execute("PRAGMA table_info(live_jobs);")
    existing_cols = {r[1] for r in cur.fetchall()}
    columns_to_add = [
        ("salary", "TEXT"),
        ("salary_min", "REAL"),
        ("salary_max", "REAL"),
        ("salary_currency", "TEXT"),
        ("salary_period", "TEXT"),
        ("skills", "TEXT"),
        ("skills_required", "TEXT"),
        ("skills_preferred", "TEXT"),
        ("work_mode", "TEXT"),
        ("experience_min", "REAL"),
        ("experience_max", "REAL"),
        ("education", "TEXT"),
        ("h1b_status", "TEXT"),
        ("visa_sponsorship_status", "TEXT"),
        ("sponsorship_evidence", "TEXT"),
        ("reports_to", "TEXT"),
    ]
    for col_name, col_type in columns_to_add:
        if col_name not in existing_cols:
            cur.execute(f"ALTER TABLE live_jobs ADD COLUMN {col_name} {col_type};")

    cur.execute("""
        CREATE TABLE IF NOT EXISTS scanned_boards (
            platform TEXT NOT NULL,
            company_slug TEXT NOT NULL,
            http_status INTEGER NOT NULL,
            total_jobs INTEGER DEFAULT 0,
            us_jobs INTEGER DEFAULT 0,
            scanned_at TEXT NOT NULL,
            PRIMARY KEY (platform, company_slug)
        );
    """)
    cur.execute("CREATE INDEX IF NOT EXISTS idx_jobs_platform ON live_jobs(platform);")
    cur.execute("CREATE INDEX IF NOT EXISTS idx_jobs_company ON live_jobs(company_slug);")
    cur.execute("CREATE INDEX IF NOT EXISTS idx_jobs_posted ON live_jobs(posted_at);")
    cur.execute("CREATE INDEX IF NOT EXISTS idx_jobs_us ON live_jobs(is_us);")
    cur.execute("CREATE INDEX IF NOT EXISTS idx_jobs_sal ON live_jobs(salary_min);")
    cur.execute("CREATE INDEX IF NOT EXISTS idx_jobs_h1b ON live_jobs(h1b_status);")
    cur.execute("CREATE INDEX IF NOT EXISTS idx_boards_status ON scanned_boards(http_status);")
    cur.execute("CREATE INDEX IF NOT EXISTS idx_boards_platform ON scanned_boards(platform);")
    conn.commit()
    conn.close()


def save_boards_batch(db_path: str, board_records: List[Tuple[str, str, int, int, int, str]]):
    if not board_records:
        return
    conn = sqlite3.connect(db_path)
    cur = conn.cursor()
    cur.executemany("""
        INSERT OR REPLACE INTO scanned_boards (
            platform, company_slug, http_status, total_jobs, us_jobs, scanned_at
        ) VALUES (?, ?, ?, ?, ?, ?)
    """, board_records)
    conn.commit()
    conn.close()


def save_jobs_batch(db_path: str, jobs: List[Dict[str, Any]], enable_enrichment: bool = False):
    if not jobs:
        return
    prepared = [enrich_job_record(j, enable_enrichment=enable_enrichment) for j in jobs]
    conn = sqlite3.connect(db_path)
    cur = conn.cursor()
    cur.executemany("""
        INSERT OR REPLACE INTO live_jobs (
            job_id, platform, company_name, company_slug, title, location, is_us,
            apply_url, department, office, employment_type, posted_at, updated_at,
            raw_timestamp, content, salary, salary_min, salary_max, salary_currency,
            salary_period, skills, skills_required, skills_preferred, work_mode,
            experience_min, experience_max, education, h1b_status,
            visa_sponsorship_status, sponsorship_evidence, reports_to, extracted_at
        ) VALUES (
            :job_id, :platform, :company_name, :company_slug, :title, :location, :is_us,
            :apply_url, :department, :office, :employment_type, :posted_at, :updated_at,
            :raw_timestamp, :content, :salary, :salary_min, :salary_max, :salary_currency,
            :salary_period, :skills, :skills_required, :skills_preferred, :work_mode,
            :experience_min, :experience_max, :education, :h1b_status,
            :visa_sponsorship_status, :sponsorship_evidence, :reports_to, :extracted_at
        )
    """, prepared)
    conn.commit()
    conn.close()


def export_sqlite_to_csv(db_path: str, csv_path: str, only_us: bool = False):
    conn = sqlite3.connect(db_path)
    cur = conn.cursor()
    query = "SELECT * FROM live_jobs"
    if only_us:
        query += " WHERE is_us = 1"
    query += " ORDER BY posted_at DESC"
    cur.execute(query)
    columns = [desc[0] for desc in cur.description]
    rows = cur.fetchall()
    conn.close()

    with open(csv_path, "w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow(columns)
        writer.writerows(rows)
    print(f"Exported {len(rows):,} jobs to '{csv_path}'.")


def merge_databases(input_patterns: List[str], output_db: str, export_csv: Optional[str] = None):
    """Merges multiple shard SQLite databases into a unified database and optional CSV."""
    resolved_paths = []
    for pat in input_patterns:
        for p in pat.split(","):
            p = p.strip()
            if not p:
                continue
            matched = glob.glob(p)
            if matched:
                resolved_paths.extend(matched)
            elif os.path.exists(p):
                resolved_paths.append(p)

    resolved_paths = sorted(list(set(resolved_paths)))
    # Exclude output_db if it was matched in the glob
    out_abs = os.path.abspath(output_db)
    resolved_paths = [p for p in resolved_paths if os.path.abspath(p) != out_abs]

    if not resolved_paths:
        print(f"No valid shard database files found matching: {input_patterns}")
        return

    print(f"\nMerging {len(resolved_paths)} shard databases into '{output_db}':")
    for p in resolved_paths:
        print(f"  • {p}")

    init_db(output_db)
    conn_out = sqlite3.connect(output_db)
    cur_out = conn_out.cursor()

    total_merged_boards = 0
    total_merged_jobs = 0

    for path in resolved_paths:
        try:
            conn_in = sqlite3.connect(path)
            cur_in = conn_in.cursor()

            # Merge scanned_boards
            try:
                cur_in.execute("SELECT platform, company_slug, http_status, total_jobs, us_jobs, scanned_at FROM scanned_boards;")
                boards = cur_in.fetchall()
                if boards:
                    cur_out.executemany("""
                        INSERT OR REPLACE INTO scanned_boards (
                            platform, company_slug, http_status, total_jobs, us_jobs, scanned_at
                        ) VALUES (?, ?, ?, ?, ?, ?)
                    """, boards)
                    total_merged_boards += len(boards)
            except Exception as e:
                print(f"Notice on merging boards from {path}: {e}")

            # Merge live_jobs
            try:
                cur_in.execute("SELECT * FROM live_jobs;")
                cols = [d[0] for d in cur_in.description]
                jobs = cur_in.fetchall()
                if jobs:
                    placeholders = ", ".join(["?"] * len(cols))
                    col_names = ", ".join(cols)
                    cur_out.executemany(f"""
                        INSERT OR REPLACE INTO live_jobs ({col_names})
                        VALUES ({placeholders})
                    """, jobs)
                    total_merged_jobs += len(jobs)
            except Exception as e:
                print(f"Notice on merging jobs from {path}: {e}")

            conn_in.close()
        except Exception as e:
            print(f"Error reading shard database {path}: {e}")

    conn_out.commit()
    conn_out.close()
    print(f"\nMerge Complete! Unified database now contains:")
    print(f"  • Total Scanned Boards: {total_merged_boards:,}")
    print(f"  • Total Live Jobs: {total_merged_jobs:,}")

    if export_csv:
        export_sqlite_to_csv(output_db, export_csv)


def load_companies(input_path: str, default_platform: str) -> List[Tuple[str, str]]:
    """Loads list of (platform, slug) pairs from CSV or TXT file."""
    if not os.path.exists(input_path):
        raise FileNotFoundError(f"Input file not found: {input_path}")

    pairs = []
    if input_path.endswith(".csv"):
        with open(input_path, "r", encoding="utf-8", errors="ignore") as f:
            reader = csv.DictReader(f)
            for row in reader:
                slug = (row.get("slug") or row.get("company_slug") or "").strip().lower()
                ats = (row.get("ats_id") or row.get("platform") or default_platform).strip().lower()
                if slug:
                    pairs.append((ats, slug))
    else:
        with open(input_path, "r", encoding="utf-8", errors="ignore") as f:
            for line in f:
                s = line.strip().lower()
                if s and not s.startswith("#"):
                    # Check for "platform,slug" format
                    if "," in s:
                        p, sl = s.split(",", 1)
                        pairs.append((p.strip(), sl.strip()))
                    else:
                        pairs.append((default_platform, s))
    return pairs


async def run_extractor(
    targets: List[Tuple[str, str]],
    output_db: str,
    export_csv: Optional[str] = None,
    concurrency: int = 50,
    full_content: bool = False,
    only_us: bool = False,
    days: int = 0,
    keyword: Optional[str] = None,
    batch_size: int = 150,
    enable_enrichment: bool = False,
):
    init_db(output_db)
    total_companies = len(targets)
    total_jobs_saved = 0
    jobs_with_post_time = 0
    boards_with_200 = 0
    boards_with_jobs = 0

    cutoff_dt = None
    if days > 0:
        cutoff_dt = datetime.now(timezone.utc) - timedelta(days=days)

    kw_clean = keyword.lower().strip() if keyword else ""

    print(f"Starting job extraction across {total_companies:,} companies...")
    print(f"  • Concurrency: {concurrency} (Rate-limited <= 10 req/host)")
    print(f"  • Database: {output_db}")
    print(f"  • Full Content (HTML): {full_content}")
    print(f"  • JD Enrichment (Salary, Skills, H-1B): {enable_enrichment and HAS_ENRICHMENT}")
    print(f"  • Only US Filter: {only_us}")
    if days > 0:
        print(f"  • Recency Filter: Last {days} days (>= {cutoff_dt.isoformat()})")
    if kw_clean:
        print(f"  • Keyword Filter: '{kw_clean}'")
    print()

    async with AsyncHTTPClient(concurrency=concurrency, per_host_concurrency=10) as http:
        for idx in range(0, total_companies, batch_size):
            batch = targets[idx : idx + batch_size]
            tasks = []
            for platform, slug in batch:
                handler = PLATFORM_HANDLERS.get(platform)
                if handler:
                    tasks.append(handler(http, slug, full_content))
                else:
                    tasks.append(asyncio.sleep(0, result=(0, [])))

            results = await asyncio.gather(*tasks, return_exceptions=True)
            batch_jobs = []
            board_records = []
            now_iso = datetime.now(timezone.utc).isoformat()

            for (platform, slug), res in zip(batch, results):
                if isinstance(res, tuple):
                    status_code, comp_jobs = res
                elif isinstance(res, list):
                    status_code, comp_jobs = 200, res
                else:
                    status_code, comp_jobs = 0, []

                if status_code == 200:
                    boards_with_200 += 1

                valid_comp_jobs = []
                for j in comp_jobs:
                    if only_us and not j["is_us"]:
                        continue

                    # Check recency filter
                    if cutoff_dt and j.get("posted_at"):
                        try:
                            j_dt = datetime.fromisoformat(j["posted_at"].replace("Z", "+00:00"))
                            if j_dt < cutoff_dt:
                                continue
                        except Exception:
                            pass

                    # Check keyword filter
                    if kw_clean:
                        search_corpus = f"{j.get('title', '')} {j.get('department', '')} {j.get('location', '')}".lower()
                        if kw_clean not in search_corpus:
                            continue

                    valid_comp_jobs.append(j)
                    if j.get("posted_at"):
                        jobs_with_post_time += 1

                if len(comp_jobs) > 0:
                    boards_with_jobs += 1

                board_records.append((
                    platform,
                    slug,
                    status_code,
                    len(comp_jobs),
                    len(valid_comp_jobs),
                    now_iso
                ))
                batch_jobs.extend(valid_comp_jobs)

            if board_records:
                save_boards_batch(output_db, board_records)
            if batch_jobs:
                save_jobs_batch(output_db, batch_jobs, enable_enrichment=enable_enrichment)
                total_jobs_saved += len(batch_jobs)

            progress = min(idx + batch_size, total_companies)
            pct = (progress / total_companies) * 100
            rate_200 = (boards_with_200 / max(1, progress)) * 100
            print(f"[{progress:,}/{total_companies:,} ({pct:.1f}%)] Scanned | Boards 200 OK: {boards_with_200:,}/{progress:,} ({rate_200:.1f}%) | Live Jobs: {total_jobs_saved:,} | Timestamp Rate: {jobs_with_post_time}/{max(1, total_jobs_saved)} ({((jobs_with_post_time / max(1, total_jobs_saved)) * 100):.1f}%)", flush=True)

    print(f"\nExtraction completed successfully!", flush=True)
    print(f"Total Boards Returning HTTP 200: {boards_with_200:,} / {total_companies:,} ({((boards_with_200 / max(1, total_companies)) * 100):.1f}%)", flush=True)
    print(f"Total live jobs in '{output_db}': {total_jobs_saved:,}", flush=True)
    if export_csv:
        export_sqlite_to_csv(output_db, export_csv, only_us=only_us)


def main():
    parser = argparse.ArgumentParser(description="Universal Multi-Platform ATS Job Extractor")
    parser.add_argument("--platform", default="greenhouse", choices=list(PLATFORM_HANDLERS.keys()) + ["all"],
                        help="ATS platform to target (or 'all' for auto-routing)")
    parser.add_argument("--slug", help="Single company slug to extract immediately")
    parser.add_argument("--input", "-i", help="Input file (CSV or TXT) containing companies/slugs")
    parser.add_argument("--output-db", default="all_ats_jobs.db", help="SQLite database output file")
    parser.add_argument("--export-csv", help="Optional CSV file to export extracted jobs to")
    parser.add_argument("--limit", type=int, default=0, help="Limit number of companies to scan (0 = all)")
    parser.add_argument("--offset", type=int, default=0, help="Offset index into input list")
    parser.add_argument("--shard-index", type=int, default=0, help="Zero-based index of this shard runner")
    parser.add_argument("--total-shards", type=int, default=1, help="Total number of parallel runner shards")
    parser.add_argument("--merge-dbs", nargs="+", help="Merge one or more shard databases into --output-db")
    parser.add_argument("--concurrency", type=int, default=40, help="Max async requests concurrency")
    parser.add_argument("--full-content", action="store_true", help="Download complete job descriptions")
    parser.add_argument("--enrich", action="store_true", help="Run deep JD enrichment (salary, skills, H-1B, experience)")
    parser.add_argument("--only-us", action="store_true", help="Only save US / US-Remote jobs")
    parser.add_argument("--days", type=int, default=0, help="Filter jobs posted within last N days (0 = all time)")
    parser.add_argument("--keyword", type=str, default="", help="Filter job title/department/location by keyword")

    args = parser.parse_args()

    # Mode 0: Database merge mode
    if args.merge_dbs:
        merge_databases(args.merge_dbs, args.output_db, export_csv=args.export_csv)
        return

    enable_enrichment = args.enrich or args.full_content

    # 1. Single slug mode
    if args.slug:
        platform = args.platform if args.platform != "all" else "greenhouse"
        handler = PLATFORM_HANDLERS.get(platform)
        if not handler:
            print(f"Unsupported platform: {platform}")
            sys.exit(1)

        async def run_single():
            async with AsyncHTTPClient(concurrency=10) as http:
                status, jobs = await handler(http, args.slug, args.full_content)
                filtered = []
                cutoff_dt = datetime.now(timezone.utc) - timedelta(days=args.days) if args.days > 0 else None
                kw = args.keyword.lower().strip()

                for j in jobs:
                    if args.only_us and not j["is_us"]:
                        continue
                    if cutoff_dt and j.get("posted_at"):
                        try:
                            j_dt = datetime.fromisoformat(j["posted_at"].replace("Z", "+00:00"))
                            if j_dt < cutoff_dt:
                                continue
                        except Exception:
                            pass
                    if kw:
                        sc = f"{j.get('title', '')} {j.get('department', '')} {j.get('location', '')}".lower()
                        if kw not in sc:
                            continue
                    filtered.append(j)

                print(f"\nBoard HTTP Status: {status} (200 = Board Exists & Active)")
                print(f"Extracted {len(filtered)} matching live jobs for {platform.upper()}:{args.slug}")
                for j in filtered[:10]:
                    print(f"  • {j['title']} | Loc: {j['location']} (US={j['is_us']})")
                    print(f"    Posted: {j['posted_at'] or 'N/A'} | Apply: {j['apply_url']}")

                if args.output_db:
                    init_db(args.output_db)
                    save_boards_batch(args.output_db, [(platform, args.slug, status, len(jobs), len(filtered), datetime.now(timezone.utc).isoformat())])
                    save_jobs_batch(args.output_db, filtered, enable_enrichment=enable_enrichment)
                    print(f"Saved to {args.output_db}")

                if args.export_csv:
                    export_sqlite_to_csv(args.output_db, args.export_csv, only_us=args.only_us)

        asyncio.run(run_single())
        return

    # 2. Batch input mode
    input_file = args.input
    if not input_file:
        # Default auto-selection
        if args.platform == "greenhouse" and os.path.exists("greenhouse_companies_us_active.txt"):
            input_file = "greenhouse_companies_us_active.txt"
        elif os.path.exists(f"exports/us_{args.platform}_companies.csv"):
            input_file = f"exports/us_{args.platform}_companies.csv"
        elif os.path.exists("exports/us_companies_by_ats_master.csv"):
            input_file = "exports/us_companies_by_ats_master.csv"
        else:
            print("Please specify an input file via --input <file.csv/txt>")
            sys.exit(1)

    print(f"Loading companies from: {input_file} (default platform: {args.platform})")
    targets = load_companies(input_file, default_platform=args.platform)

    # Filter by platform if specific platform requested
    if args.platform != "all":
        targets = [(p, s) for (p, s) in targets if p == args.platform]

    # Deduplicate while preserving order
    seen = set()
    unique_targets = []
    for p, s in targets:
        key = (p, s)
        if key not in seen:
            seen.add(key)
            unique_targets.append(key)

    if args.offset > 0:
        unique_targets = unique_targets[args.offset:]
    if args.limit > 0:
        unique_targets = unique_targets[:args.limit]

    # Multi-runner sharding: strided modulo partition
    if args.total_shards > 1:
        if args.shard_index < 0 or args.shard_index >= args.total_shards:
            print(f"Error: --shard-index ({args.shard_index}) must be between 0 and {args.total_shards - 1}")
            sys.exit(1)
        unique_targets = [t for i, t in enumerate(unique_targets) if i % args.total_shards == args.shard_index]
        print(f"Shard [{args.shard_index + 1}/{args.total_shards}]: Partitioned {len(unique_targets):,} target boards.")
        stagger = (args.shard_index * 1.5) + random.uniform(0.1, 0.5)
        print(f"Applying startup stagger: {stagger:.2f}s to avoid thundering herd...")
        time.sleep(stagger)

    print(f"Total unique target boards to scan: {len(unique_targets):,}")

    asyncio.run(
        run_extractor(
            targets=unique_targets,
            output_db=args.output_db,
            export_csv=args.export_csv,
            concurrency=args.concurrency,
            full_content=args.full_content,
            only_us=args.only_us,
            days=args.days,
            keyword=args.keyword,
            enable_enrichment=enable_enrichment,
        )
    )


if __name__ == "__main__":
    main()
