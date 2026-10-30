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
from datetime import datetime, timezone, timedelta
from urllib.parse import urlparse
from typing import List, Dict, Any, Optional, Tuple

try:
    import aiohttp
    HAS_AIOHTTP = True
except ImportError:
    HAS_AIOHTTP = False


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
    """Asynchronous HTTP client with per-host rate-limiting and exponential backoff."""
    def __init__(self, concurrency: int = 50, per_host_concurrency: int = 10, timeout: float = 15.0):
        self.semaphore = asyncio.Semaphore(concurrency)
        self.per_host_concurrency = per_host_concurrency
        self._host_semaphores: Dict[str, asyncio.Semaphore] = {}
        self.timeout = timeout
        self._session = None

    def _get_host_sem(self, url: str) -> asyncio.Semaphore:
        try:
            host = (urlparse(url).hostname or "default").lower()
        except Exception:
            host = "default"
        if host not in self._host_semaphores:
            self._host_semaphores[host] = asyncio.Semaphore(self.per_host_concurrency)
        return self._host_semaphores[host]

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

    async def get_json(self, url: str, max_retries: int = 2) -> Tuple[int, Any]:
        """Performs async GET and parses JSON with retries on rate limits."""
        host_sem = self._get_host_sem(url)
        for attempt in range(max_retries + 1):
            async with self.semaphore:
                async with host_sem:
                    if not HAS_AIOHTTP or not self._session:
                        # Fallback for systems without aiohttp
                        import urllib.request
                        try:
                            req = urllib.request.Request(
                                url,
                                headers={"User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7)"}
                            )
                            with urllib.request.urlopen(req, timeout=self.timeout) as resp:
                                status = resp.status
                                data = json.loads(resp.read().decode("utf-8", errors="ignore"))
                                return status, data
                        except urllib.error.HTTPError as e:
                            if e.code in (429, 503) and attempt < max_retries:
                                await asyncio.sleep(1.5 * (attempt + 1))
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
                                await asyncio.sleep(1.5 * (attempt + 1))
                                continue
                            else:
                                return resp.status, None
                    except (asyncio.TimeoutError, aiohttp.ClientError):
                        if attempt < max_retries:
                            await asyncio.sleep(1.0)
                            continue
                        return 0, None
                    except Exception:
                        return 0, None
        return 0, None


# Platform Extractors
async def extract_greenhouse(http: AsyncHTTPClient, slug: str, full_content: bool) -> List[Dict[str, Any]]:
    url = f"https://boards-api.greenhouse.io/v1/boards/{slug}/jobs"
    if full_content:
        url += "?content=true"
    status, data = await http.get_json(url)
    if status != 200 or not isinstance(data, dict):
        return []

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
    return extracted


async def extract_lever(http: AsyncHTTPClient, slug: str, full_content: bool) -> List[Dict[str, Any]]:
    url = f"https://api.lever.co/v0/postings/{slug}?mode=json"
    status, data = await http.get_json(url)
    if status != 200 or not isinstance(data, list):
        return []

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
    return extracted


async def extract_ashby(http: AsyncHTTPClient, slug: str, full_content: bool) -> List[Dict[str, Any]]:
    url = f"https://api.ashbyhq.com/posting-api/job-board/{slug}"
    status, data = await http.get_json(url)
    if status != 200 or not isinstance(data, dict):
        return []

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
    return extracted


async def extract_smartrecruiters(http: AsyncHTTPClient, slug: str, full_content: bool) -> List[Dict[str, Any]]:
    url = f"https://api.smartrecruiters.com/v1/companies/{slug}/postings"
    status, data = await http.get_json(url)
    if status != 200 or not isinstance(data, dict):
        return []

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
    return extracted


async def extract_workable(http: AsyncHTTPClient, slug: str, full_content: bool) -> List[Dict[str, Any]]:
    url = f"https://apply.workable.com/api/v1/widget/accounts/{slug}"
    status, data = await http.get_json(url)
    if status != 200 or not isinstance(data, dict):
        return []

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
    return extracted


async def extract_recruitee(http: AsyncHTTPClient, slug: str, full_content: bool) -> List[Dict[str, Any]]:
    url = f"https://{slug}.recruitee.com/api/offers"
    status, data = await http.get_json(url)
    if status != 200 or not isinstance(data, dict):
        return []

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
            "is_us": 1 if (is_us_location(loc_str) or o.get("country_code") in ("US", "USA") or is_remote) else 0,
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
    return extracted


async def extract_breezy(http: AsyncHTTPClient, slug: str, full_content: bool) -> List[Dict[str, Any]]:
    url = f"https://{slug}.breezy.hr/json"
    status, data = await http.get_json(url)
    if status != 200 or not isinstance(data, list):
        return []

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
            "is_us": 1 if is_us_location(loc_str, extra_text=f"{j.get('title', '')} {' '.join(offices)}") else 0,
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
    return extracted


async def extract_rippling(http: AsyncHTTPClient, slug: str, full_content: bool) -> List[Dict[str, Any]]:
    url = f"https://ats.rippling.com/api/v1/board/{slug}/jobs"
    status, data = await http.get_json(url)
    if status != 200 or not isinstance(data, list):
        return []

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
            "is_us": 1 if is_us_location(work_loc) else 0,
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
    return extracted


async def extract_bamboohr(http: AsyncHTTPClient, slug: str, full_content: bool) -> List[Dict[str, Any]]:
    url = f"https://{slug}.bamboohr.com/careers/list"
    status, data = await http.get_json(url)
    if status != 200 or not isinstance(data, dict):
        return []

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
            "is_us": 1 if (is_us_location(loc_str) or is_remote) else 0,
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
    return extracted


async def extract_pinpoint(http: AsyncHTTPClient, slug: str, full_content: bool) -> List[Dict[str, Any]]:
    url = f"https://{slug}.pinpointhq.com/postings.json"
    status, data = await http.get_json(url)
    if status != 200 or not isinstance(data, dict):
        return []

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
            "is_us": 1 if is_us_location(loc_str, extra_text=f"{j.get('title', '')} {' '.join(offices)}") else 0,
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
    return extracted


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
            extracted_at TEXT NOT NULL
        );
    """)
    cur.execute("CREATE INDEX IF NOT EXISTS idx_jobs_platform ON live_jobs(platform);")
    cur.execute("CREATE INDEX IF NOT EXISTS idx_jobs_company ON live_jobs(company_slug);")
    cur.execute("CREATE INDEX IF NOT EXISTS idx_jobs_posted ON live_jobs(posted_at);")
    cur.execute("CREATE INDEX IF NOT EXISTS idx_jobs_us ON live_jobs(is_us);")
    conn.commit()
    conn.close()


def save_jobs_batch(db_path: str, jobs: List[Dict[str, Any]]):
    if not jobs:
        return
    conn = sqlite3.connect(db_path)
    cur = conn.cursor()
    cur.executemany("""
        INSERT OR REPLACE INTO live_jobs (
            job_id, platform, company_name, company_slug, title, location, is_us,
            apply_url, department, office, employment_type, posted_at, updated_at,
            raw_timestamp, content, extracted_at
        ) VALUES (
            :job_id, :platform, :company_name, :company_slug, :title, :location, :is_us,
            :apply_url, :department, :office, :employment_type, :posted_at, :updated_at,
            :raw_timestamp, :content, :extracted_at
        )
    """, jobs)
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
):
    init_db(output_db)
    total_companies = len(targets)
    total_jobs_saved = 0
    jobs_with_post_time = 0

    cutoff_dt = None
    if days > 0:
        cutoff_dt = datetime.now(timezone.utc) - timedelta(days=days)

    kw_clean = keyword.lower().strip() if keyword else ""

    print(f"Starting job extraction across {total_companies:,} companies...")
    print(f"  • Concurrency: {concurrency} (Rate-limited <= 10 req/host)")
    print(f"  • Database: {output_db}")
    print(f"  • Full Content (HTML): {full_content}")
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
                    tasks.append(asyncio.sleep(0, result=[]))

            results = await asyncio.gather(*tasks, return_exceptions=True)
            batch_jobs = []

            for res in results:
                if isinstance(res, list):
                    for j in res:
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

                        batch_jobs.append(j)
                        if j["posted_at"]:
                            jobs_with_post_time += 1

            if batch_jobs:
                save_jobs_batch(output_db, batch_jobs)
                total_jobs_saved += len(batch_jobs)

            progress = min(idx + batch_size, total_companies)
            pct = (progress / total_companies) * 100
            print(f"[{progress:,}/{total_companies:,} ({pct:.1f}%)] Scanned | Live Jobs Saved: {total_jobs_saved:,} | Timestamp Capture Rate: {jobs_with_post_time}/{max(1, total_jobs_saved)} ({((jobs_with_post_time / max(1, total_jobs_saved)) * 100):.1f}%)", flush=True)

    print(f"\nExtraction completed successfully!", flush=True)
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
    parser.add_argument("--concurrency", type=int, default=40, help="Max async requests concurrency")
    parser.add_argument("--full-content", action="store_true", help="Download complete job descriptions")
    parser.add_argument("--only-us", action="store_true", help="Only save US / US-Remote jobs")
    parser.add_argument("--days", type=int, default=0, help="Filter jobs posted within last N days (0 = all time)")
    parser.add_argument("--keyword", type=str, default="", help="Filter job title/department/location by keyword")

    args = parser.parse_args()

    # 1. Single slug mode
    if args.slug:
        platform = args.platform if args.platform != "all" else "greenhouse"
        handler = PLATFORM_HANDLERS.get(platform)
        if not handler:
            print(f"Unsupported platform: {platform}")
            sys.exit(1)

        async def run_single():
            async with AsyncHTTPClient(concurrency=10) as http:
                jobs = await handler(http, args.slug, args.full_content)
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

                print(f"\nExtracted {len(filtered)} matching live jobs for {platform.upper()}:{args.slug}")
                for j in filtered[:10]:
                    print(f"  • {j['title']} | Loc: {j['location']} (US={j['is_us']})")
                    print(f"    Posted: {j['posted_at'] or 'N/A'} | Apply: {j['apply_url']}")

                if args.output_db:
                    init_db(args.output_db)
                    save_jobs_batch(args.output_db, filtered)
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
        )
    )


if __name__ == "__main__":
    main()
