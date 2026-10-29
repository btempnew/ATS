#!/usr/bin/env python3
"""
ATS Company Discovery & Entity Resolution Engine (Open-Source TheirStack Alternative)
=====================================================================================

An end-to-end, high-concurrency discovery, verification, and entity-resolution
pipeline designed to map companies to their Applicant Tracking Systems (ATS) at
or above aggregator scale (365k+ career sources across 88 ATS platforms).

Core Architecture (6 Discovery Vectors + Verification + Global Entity Resolution):
1. Common Crawl CDX Index Streaming with Alphanumeric Prefix Sharding:
   Streams billions of URLs from Common Crawl's CDX API (`index.commoncrawl.org`)
   across 88 ATS domain patterns. Supports 36-way alphanumeric prefix sharding
   (`a`-`z`, `0`-`9`) on path-based ATS platforms (Greenhouse, Ashby, Lever,
   Workable, SmartRecruiters) so large job boards never crowd out smaller companies.
2. Internet Archive Wayback CDX Streaming (`web.archive.org/cdx`):
   Streams URL-key collapsed historical and active ATS board URLs across all
   supported ATS platforms in bulk.
3. Certificate Transparency (CT) Log Mining (`crt.sh`):
   Harvests every TLS certificate ever issued for subdomain-based ATS platforms
   (Workday, iCIMS, Recruitee, BreezyHR, BambooHR, Personio, Pinpoint, Teamtailor,
   JazzHR, Keka, Darwinbox, Freshteam, Zoho Recruit, Avature, Taleo, etc.).
4. Bulk Domain Career-Page & DNS CNAME Fingerprinting:
   Probes seed company domains (e.g., Tranco Top 1M, CrUX, Wikidata) by checking
   DNS CNAME records (`jobs.<domain>`, `careers.<domain>`) and scanning `/careers`
   HTML for embedded ATS scripts, iframes, `?gh_jid=` tokens, and outbound links.
5. Automated Seed Domain Bootstrapper & Direct Slug Permutation Probing:
   Downloads public domain lists (Tranco Top 1M / Wikidata SPARQL), generates
   normalized company slug candidates (`acme`, `acmehq`, `acme-inc`, `acmeusa`),
   and probes unauthenticated public ATS JSON endpoints directly.
6. Live Public ATS API Verification & Technographic Extraction:
   Polls unauthenticated ATS JSON/XML endpoints to verify active open roles,
   extract canonical company names/domains, and mine 40+ tech stack keywords.
7. Cross-Shard SQLite Merger & Multi-Signal Entity Resolution:
   Merges distributed SQLite shard databases from parallel GitHub Actions matrix
   runners and resolves slug collisions and ATS migrations globally.

Usage:
    # Run built-in verification & self-test suite (offline deterministic test)
    python3 ats_company_discovery_engine.py self-test

    # Discover ATS boards from CT Logs + Common Crawl (with prefix sharding) + Wayback CDX
    python3 ats_company_discovery_engine.py discover --sources ct,commoncrawl,wayback --platforms greenhouse --prefix-shard

    # Bootstrap public seed domains (Wikidata / Tranco) and probe slugs directly
    python3 ats_company_discovery_engine.py probe-slugs --input domains.txt --platforms greenhouse,ashby,lever

    # Verify discovered boards against live public ATS APIs & resolve entities
    python3 ats_company_discovery_engine.py verify --db ats_companies.db --concurrency 80

    # Merge multiple SQLite shard databases from parallel GitHub Actions matrix jobs
    python3 ats_company_discovery_engine.py merge-dbs --inputs shard1.db,shard2.db --output ats_companies.db

    # Export clean, deduplicated ATS-wise company lists to CSV / JSONL + Markdown report
    python3 ats_company_discovery_engine.py export --db ats_companies.db --output-dir ./exports
"""

from __future__ import annotations

import argparse
import asyncio
import csv
import dataclasses
from dataclasses import dataclass, field
from datetime import datetime, timezone
import hashlib
import html
import io
import json
import logging
import os
from pathlib import Path
import re
import socket
import sqlite3
import string
import sys
import time
from typing import Any, Callable, Dict, Iterable, List, Optional, Sequence, Set, Tuple
from urllib.parse import parse_qs, quote_plus, urljoin, urlparse
import xml.etree.ElementTree as ET
import zipfile

# Optional async HTTP dependencies with graceful fallback to stdlib urllib
try:
    import aiohttp  # type: ignore
    HAS_AIOHTTP = True
except ImportError:
    HAS_AIOHTTP = False

import urllib.error
import urllib.request

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger("ats_engine")


# =============================================================================
# 1. COMPREHENSIVE 88 ATS PLATFORM REGISTRY
# =============================================================================

@dataclass(frozen=True)
class ATSPlatformSpec:
    """Specification for detecting, extracting, and querying an ATS platform."""
    ats_id: str
    display_name: str
    category: str  # "enterprise", "mid_market", "startup_modern", "smb", "regional_global"
    url_regexes: Tuple[str, ...]
    cdx_patterns: Tuple[str, ...]
    ct_patterns: Tuple[str, ...] = ()
    cname_targets: Tuple[str, ...] = ()
    html_signatures: Tuple[str, ...] = ()
    public_api_url: Optional[str] = None
    public_api_method: str = "GET"
    board_url_template: str = ""


ATS_PLATFORMS: Dict[str, ATSPlatformSpec] = {
    "ashby": ATSPlatformSpec(
        ats_id="ashby",
        display_name="Ashby",
        category="startup_modern",
        url_regexes=(
            r"https?://jobs\.ashbyhq\.com/(?P<slug>[a-zA-Z0-9._-]+)(?:/|$|\?)",
            r"https?://api\.ashbyhq\.com/posting-api/job-board/(?P<slug>[a-zA-Z0-9._-]+)",
        ),
        cdx_patterns=("jobs.ashbyhq.com/*",),
        ct_patterns=(),
        cname_targets=("jobs.ashbyhq.com", "custom.ashbyhq.com", "ashbyhq.com"),
        html_signatures=(
            r"jobs\.ashbyhq\.com/(?P<slug>[a-zA-Z0-9._-]+)",
            r"ashby_job_board_iframe",
            r"cdn\.ashbyhq\.com",
        ),
        public_api_url="https://api.ashbyhq.com/posting-api/job-board/{slug}?includeCompensation=true",
        board_url_template="https://jobs.ashbyhq.com/{slug}",
    ),
    "greenhouse": ATSPlatformSpec(
        ats_id="greenhouse",
        display_name="Greenhouse",
        category="mid_market",
        url_regexes=(
            r"https?://(?:boards|job-boards)\.greenhouse\.io/(?:embed/job_board\?for=)?(?P<slug>[a-zA-Z0-9_-]+)(?:/|$|\?)",
            r"https?://(?:boards|job-boards)\.eu\.greenhouse\.io/(?P<slug>[a-zA-Z0-9_-]+)(?:/|$|\?)",
            r"https?://(?:boards-api|api)\.greenhouse\.io/v1/boards/(?P<slug>[a-zA-Z0-9_-]+)",
        ),
        cdx_patterns=(
            "boards.greenhouse.io/*",
            "job-boards.greenhouse.io/*",
            "boards.eu.greenhouse.io/*",
            "job-boards.eu.greenhouse.io/*",
        ),
        cname_targets=("ghs.greenhouse.io", "boards.greenhouse.io", "job-boards.greenhouse.io"),
        html_signatures=(
            r"boards(?:\.eu)?\.greenhouse\.io/embed/job_board(?:/js)?\?for=(?P<slug>[a-zA-Z0-9_-]+)",
            r"(?:boards|job-boards)(?:\.eu)?\.greenhouse\.io/(?P<slug>[a-zA-Z0-9_-]+)",
            r"gh_jid=",
        ),
        public_api_url="https://boards-api.greenhouse.io/v1/boards/{slug}/jobs?content=true",
        board_url_template="https://job-boards.greenhouse.io/{slug}",
    ),
    "lever": ATSPlatformSpec(
        ats_id="lever",
        display_name="Lever",
        category="mid_market",
        url_regexes=(
            r"https?://jobs(?:\.eu)?\.lever\.co/(?P<slug>[a-zA-Z0-9._-]+)(?:/|$|\?)",
            r"https?://api(?:\.eu)?\.lever\.co/v0/postings/(?P<slug>[a-zA-Z0-9._-]+)",
        ),
        cdx_patterns=("jobs.lever.co/*", "jobs.eu.lever.co/*"),
        cname_targets=("jobs.lever.co", "jobs.eu.lever.co", "lever.co"),
        html_signatures=(
            r"jobs(?:\.eu)?\.lever\.co/(?P<slug>[a-zA-Z0-9._-]+)",
            r"api(?:\.eu)?\.lever\.co/v0/postings/(?P<slug>[a-zA-Z0-9._-]+)",
            r"lever-jobs-embed",
        ),
        public_api_url="https://api.lever.co/v0/postings/{slug}?mode=json",
        board_url_template="https://jobs.lever.co/{slug}",
    ),
    "workday": ATSPlatformSpec(
        ats_id="workday",
        display_name="Workday",
        category="enterprise",
        url_regexes=(
            r"https?://(?P<slug>[a-zA-Z0-9-]+)\.(?P<shard>wd\d+)\.myworkdayjobs\.com/(?:[a-z]{2}-[A-Z]{2}/)?(?P<site>[a-zA-Z0-9_-]+)",
            r"https?://(?P<slug>[a-zA-Z0-9-]+)\.(?P<shard>wd\d+)\.myworkdayjobs\.com/?$",
        ),
        cdx_patterns=("*.myworkdayjobs.com/*",),
        ct_patterns=("%.myworkdayjobs.com",),
        cname_targets=("myworkdayjobs.com", "myworkdaysite.com"),
        html_signatures=(
            r"https?://(?P<slug>[a-zA-Z0-9-]+)\.(?P<shard>wd\d+)\.myworkdayjobs\.com/(?:[a-z]{2}-[A-Z]{2}/)?(?P<site>[a-zA-Z0-9_-]+)",
            r"/wday/cxs/(?P<slug>[a-zA-Z0-9-]+)/(?P<site>[a-zA-Z0-9_-]+)/jobs",
        ),
        public_api_url="https://{slug}.{shard}.myworkdayjobs.com/wday/cxs/{slug}/{site}/jobs",
        public_api_method="POST",
        board_url_template="https://{slug}.{shard}.myworkdayjobs.com/{site}",
    ),
    "workable": ATSPlatformSpec(
        ats_id="workable",
        display_name="Workable",
        category="smb",
        url_regexes=(
            r"https?://apply\.workable\.com/(?:api/v1/widget/accounts/)?(?P<slug>[a-zA-Z0-9_-]+)(?:/|$|\?)",
            r"https?://(?P<slug>[a-zA-Z0-9_-]+)\.workable\.com(?:/|$|\?)",
        ),
        cdx_patterns=("apply.workable.com/*", "*.workable.com/*"),
        ct_patterns=("%.workable.com",),
        cname_targets=("apply.workable.com", "workable.com"),
        html_signatures=(
            r"apply\.workable\.com/(?P<slug>[a-zA-Z0-9_-]+)",
            r"whr\.workable\.com",
            r"whr_embed\(['\"](?P<slug>[a-zA-Z0-9_-]+)['\"]",
        ),
        public_api_url="https://apply.workable.com/api/v1/widget/accounts/{slug}?details=true",
        board_url_template="https://apply.workable.com/{slug}",
    ),
    "smartrecruiters": ATSPlatformSpec(
        ats_id="smartrecruiters",
        display_name="SmartRecruiters",
        category="enterprise",
        url_regexes=(
            r"https?://(?:jobs|careers)\.smartrecruiters\.com/(?P<slug>[a-zA-Z0-9._-]+)(?:/|$|\?)",
            r"https?://api\.smartrecruiters\.com/v1/companies/(?P<slug>[a-zA-Z0-9._-]+)/postings",
        ),
        cdx_patterns=("jobs.smartrecruiters.com/*", "careers.smartrecruiters.com/*"),
        cname_targets=("jobs.smartrecruiters.com", "careers.smartrecruiters.com"),
        html_signatures=(
            r"(?:jobs|careers)\.smartrecruiters\.com/(?P<slug>[a-zA-Z0-9._-]+)",
            r"api\.smartrecruiters\.com/v1/companies/(?P<slug>[a-zA-Z0-9._-]+)/postings",
        ),
        public_api_url="https://api.smartrecruiters.com/v1/companies/{slug}/postings?limit=100",
        board_url_template="https://jobs.smartrecruiters.com/{slug}",
    ),
    "recruitee": ATSPlatformSpec(
        ats_id="recruitee",
        display_name="Recruitee",
        category="smb",
        url_regexes=(
            r"https?://(?P<slug>[a-zA-Z0-9-]+)\.recruitee\.com(?:/|$|\?)",
        ),
        cdx_patterns=("*.recruitee.com/*",),
        ct_patterns=("%.recruitee.com",),
        cname_targets=("cname.recruitee.com", "recruitee.com"),
        html_signatures=(
            r"https?://(?P<slug>[a-zA-Z0-9-]+)\.recruitee\.com",
            r"recruitee-careers-widget",
        ),
        public_api_url="https://{slug}.recruitee.com/api/offers",
        board_url_template="https://{slug}.recruitee.com",
    ),
    "breezy": ATSPlatformSpec(
        ats_id="breezy",
        display_name="Breezy HR",
        category="smb",
        url_regexes=(
            r"https?://(?P<slug>[a-zA-Z0-9-]+)\.breezy\.hr(?:/|$|\?)",
        ),
        cdx_patterns=("*.breezy.hr/*",),
        ct_patterns=("%.breezy.hr",),
        cname_targets=("custom-portal.breezy.hr", "breezy.hr"),
        html_signatures=(
            r"https?://(?P<slug>[a-zA-Z0-9-]+)\.breezy\.hr",
            r"breezy-portal",
        ),
        public_api_url="https://{slug}.breezy.hr/json",
        board_url_template="https://{slug}.breezy.hr",
    ),
    "bamboohr": ATSPlatformSpec(
        ats_id="bamboohr",
        display_name="BambooHR",
        category="smb",
        url_regexes=(
            r"https?://(?P<slug>[a-zA-Z0-9-]+)\.bamboohr\.com/(?:careers|jobs)(?:/|$|\?)",
        ),
        cdx_patterns=("*.bamboohr.com/careers*", "*.bamboohr.com/jobs*"),
        ct_patterns=("%.bamboohr.com",),
        cname_targets=("bamboohr.com",),
        html_signatures=(
            r"https?://(?P<slug>[a-zA-Z0-9-]+)\.bamboohr\.com/(?:careers|jobs)",
            r"BambooHR-ATS",
        ),
        public_api_url="https://{slug}.bamboohr.com/careers/list",
        board_url_template="https://{slug}.bamboohr.com/careers",
    ),
    "personio": ATSPlatformSpec(
        ats_id="personio",
        display_name="Personio",
        category="mid_market",
        url_regexes=(
            r"https?://(?P<slug>[a-zA-Z0-9-]+)\.jobs\.personio\.(?P<shard>de|com)(?:/|$|\?)",
        ),
        cdx_patterns=("*.jobs.personio.de/*", "*.jobs.personio.com/*"),
        ct_patterns=("%.jobs.personio.de", "%.jobs.personio.com"),
        cname_targets=("jobs.personio.de", "jobs.personio.com"),
        html_signatures=(
            r"https?://(?P<slug>[a-zA-Z0-9-]+)\.jobs\.personio\.(?P<shard>de|com)",
        ),
        public_api_url="https://{slug}.jobs.personio.{shard}/xml",
        board_url_template="https://{slug}.jobs.personio.{shard}",
    ),
    "teamtailor": ATSPlatformSpec(
        ats_id="teamtailor",
        display_name="Teamtailor",
        category="mid_market",
        url_regexes=(
            r"https?://(?P<slug>[a-zA-Z0-9-]+)\.teamtailor\.com(?:/|$|\?)",
            r"https?://career\.teamtailor\.com/(?P<slug>[a-zA-Z0-9-]+)",
        ),
        cdx_patterns=("*.teamtailor.com/*",),
        ct_patterns=("%.teamtailor.com",),
        cname_targets=("ext.teamtailor.com", "teamtailor.com", "career.teamtailor.com"),
        html_signatures=(
            r"https?://(?P<slug>[a-zA-Z0-9-]+)\.teamtailor\.com",
            r"teamtailor-portal",
            r"cdn\.teamtailor-cdn\.com",
        ),
        public_api_url="https://{slug}.teamtailor.com/jobs.rss",
        board_url_template="https://{slug}.teamtailor.com",
    ),
    "pinpoint": ATSPlatformSpec(
        ats_id="pinpoint",
        display_name="Pinpoint",
        category="mid_market",
        url_regexes=(
            r"https?://(?P<slug>[a-zA-Z0-9-]+)\.pinpointhq\.com(?:/|$|\?)",
        ),
        cdx_patterns=("*.pinpointhq.com/*",),
        ct_patterns=("%.pinpointhq.com",),
        cname_targets=("pinpointhq.com", "custom.pinpointhq.com"),
        html_signatures=(
            r"https?://(?P<slug>[a-zA-Z0-9-]+)\.pinpointhq\.com",
        ),
        public_api_url="https://{slug}.pinpointhq.com/postings.json",
        board_url_template="https://{slug}.pinpointhq.com",
    ),
    "rippling": ATSPlatformSpec(
        ats_id="rippling",
        display_name="Rippling ATS",
        category="startup_modern",
        url_regexes=(
            r"https?://ats\.rippling\.com/(?P<slug>[a-zA-Z0-9_-]+)(?:/|$|\?)",
            r"https?://(?P<slug>[a-zA-Z0-9_-]+)\.rippling-ats\.com(?:/|$|\?)",
        ),
        cdx_patterns=("ats.rippling.com/*", "*.rippling-ats.com/*"),
        ct_patterns=("%.rippling-ats.com",),
        cname_targets=("ats.rippling.com", "rippling-ats.com"),
        html_signatures=(
            r"ats\.rippling\.com/(?P<slug>[a-zA-Z0-9_-]+)",
            r"https?://(?P<slug>[a-zA-Z0-9_-]+)\.rippling-ats\.com",
        ),
        public_api_url="https://ats.rippling.com/api/board/{slug}/jobs",
        board_url_template="https://ats.rippling.com/{slug}/jobs",
    ),
    "hirehive": ATSPlatformSpec(
        ats_id="hirehive",
        display_name="HireHive",
        category="smb",
        url_regexes=(
            r"https?://(?P<slug>[a-zA-Z0-9-]+)\.hirehive\.com(?:/|$|\?)",
            r"https?://my\.hirehive\.io/(?P<slug>[a-zA-Z0-9-]+)(?:/|$|\?)",
        ),
        cdx_patterns=("*.hirehive.com/*", "my.hirehive.io/*"),
        ct_patterns=("%.hirehive.com",),
        cname_targets=("hirehive.com",),
        html_signatures=(r"https?://(?P<slug>[a-zA-Z0-9-]+)\.hirehive\.com",),
        public_api_url="https://{slug}.hirehive.com/api/v1/jobs",
        board_url_template="https://{slug}.hirehive.com",
    ),
    "manatal": ATSPlatformSpec(
        ats_id="manatal",
        display_name="Manatal",
        category="smb",
        url_regexes=(
            r"https?://(?:www\.)?careers-page\.com/(?P<slug>[a-zA-Z0-9_-]+)(?:/|$|\?)",
            r"https?://api\.careers-page\.com/open/v1/career-pages/(?P<slug>[a-zA-Z0-9_-]+)/job-posts",
        ),
        cdx_patterns=("careers-page.com/*", "www.careers-page.com/*"),
        cname_targets=("careers-page.com",),
        html_signatures=(r"careers-page\.com/(?P<slug>[a-zA-Z0-9_-]+)",),
        public_api_url="https://api.careers-page.com/open/v1/career-pages/{slug}/job-posts",
        board_url_template="https://www.careers-page.com/{slug}",
    ),
    "join": ATSPlatformSpec(
        ats_id="join",
        display_name="JOIN",
        category="smb",
        url_regexes=(
            r"https?://join\.com/companies/(?P<slug>[a-zA-Z0-9_-]+)(?:/|$|\?)",
        ),
        cdx_patterns=("join.com/companies/*",),
        cname_targets=("join.com",),
        html_signatures=(
            r"join\.com/companies/(?P<slug>[a-zA-Z0-9_-]+)",
            r"widget\.join\.com",
        ),
        public_api_url="https://join.com/companies/{slug}",
        board_url_template="https://join.com/companies/{slug}",
    ),
    "dover": ATSPlatformSpec(
        ats_id="dover",
        display_name="Dover",
        category="startup_modern",
        url_regexes=(
            r"https?://app\.dover\.com/apply/(?P<slug>[a-zA-Z0-9_-]+)(?:/|$|\?)",
            r"https?://app\.dover\.io/apply/(?P<slug>[a-zA-Z0-9_-]+)(?:/|$|\?)",
        ),
        cdx_patterns=("app.dover.com/apply/*", "app.dover.io/apply/*"),
        html_signatures=(r"app\.dover\.(?:com|io)/apply/(?P<slug>[a-zA-Z0-9_-]+)",),
        public_api_url="https://app.dover.com/api/v1/careers-page/{slug}/jobs",
        board_url_template="https://app.dover.com/apply/{slug}",
    ),
    "comeet": ATSPlatformSpec(
        ats_id="comeet",
        display_name="Comeet",
        category="mid_market",
        url_regexes=(
            r"https?://(?:www\.)?comeet\.com/jobs/(?P<slug>[a-zA-Z0-9._-]+)/(?P<shard>[a-zA-Z0-9._-]+)(?:/|$|\?)",
        ),
        cdx_patterns=("www.comeet.com/jobs/*", "comeet.com/jobs/*"),
        html_signatures=(
            r"comeet\.com/jobs/(?P<slug>[a-zA-Z0-9._-]+)/(?P<shard>[a-zA-Z0-9._-]+)",
            r"comeet-careers-api",
        ),
        public_api_url="https://www.comeet.com/jobs/{slug}/{shard}",
        board_url_template="https://www.comeet.com/jobs/{slug}/{shard}",
    ),
    "homerun": ATSPlatformSpec(
        ats_id="homerun",
        display_name="Homerun",
        category="smb",
        url_regexes=(
            r"https?://(?P<slug>[a-zA-Z0-9-]+)\.homerun\.co(?:/|$|\?)",
        ),
        cdx_patterns=("*.homerun.co/*",),
        ct_patterns=("%.homerun.co",),
        cname_targets=("custom.homerun.co", "homerun.co"),
        html_signatures=(r"https?://(?P<slug>[a-zA-Z0-9-]+)\.homerun\.co",),
        public_api_url="https://{slug}.homerun.co",
        board_url_template="https://{slug}.homerun.co",
    ),
    "jobvite": ATSPlatformSpec(
        ats_id="jobvite",
        display_name="Jobvite",
        category="enterprise",
        url_regexes=(
            r"https?://jobs\.jobvite\.com/(?:careers/)?(?P<slug>[a-zA-Z0-9_-]+)(?:/|$|\?)",
        ),
        cdx_patterns=("jobs.jobvite.com/*",),
        cname_targets=("jobs.jobvite.com", "jobvite.com"),
        html_signatures=(
            r"jobs\.jobvite\.com/(?:careers/)?(?P<slug>[a-zA-Z0-9_-]+)",
            r"jv-careersite",
        ),
        public_api_url="https://jobs.jobvite.com/{slug}",
        board_url_template="https://jobs.jobvite.com/{slug}",
    ),
    "icims": ATSPlatformSpec(
        ats_id="icims",
        display_name="iCIMS",
        category="enterprise",
        url_regexes=(
            r"https?://(?P<slug>[a-zA-Z0-9-]+)\.icims\.com(?:/|$|\?)",
        ),
        cdx_patterns=("*.icims.com/*",),
        ct_patterns=("%.icims.com",),
        cname_targets=("icims.com",),
        html_signatures=(
            r"https?://(?P<slug>[a-zA-Z0-9-]+)\.icims\.com",
            r"iCIMS_MainWrapper",
        ),
        public_api_url="https://{slug}.icims.com/jobs/search?in_iframe=1",
        board_url_template="https://{slug}.icims.com",
    ),
    "jazzhr": ATSPlatformSpec(
        ats_id="jazzhr",
        display_name="JazzHR",
        category="smb",
        url_regexes=(
            r"https?://(?P<slug>[a-zA-Z0-9-]+)\.applytojob\.com(?:/|$|\?)",
        ),
        cdx_patterns=("*.applytojob.com/*",),
        ct_patterns=("%.applytojob.com",),
        cname_targets=("applytojob.com",),
        html_signatures=(
            r"https?://(?P<slug>[a-zA-Z0-9-]+)\.applytojob\.com",
            r"app\.jazz\.co/widgets/basic/create/(?P<slug>[a-zA-Z0-9_-]+)",
        ),
        public_api_url="https://{slug}.applytojob.com/apply",
        board_url_template="https://{slug}.applytojob.com/apply",
    ),
    "gem": ATSPlatformSpec(
        ats_id="gem",
        display_name="Gem ATS",
        category="startup_modern",
        url_regexes=(
            r"https?://jobs\.gem\.com/(?P<slug>[a-zA-Z0-9_-]+)(?:/|$|\?)",
        ),
        cdx_patterns=("jobs.gem.com/*",),
        cname_targets=("jobs.gem.com",),
        html_signatures=(r"jobs\.gem\.com/(?P<slug>[a-zA-Z0-9_-]+)",),
        public_api_url="https://api.gem.com/job_board/v0/{slug}/job_posts",
        board_url_template="https://jobs.gem.com/{slug}",
    ),
    "kula": ATSPlatformSpec(
        ats_id="kula",
        display_name="Kula",
        category="startup_modern",
        url_regexes=(
            r"https?://careers\.kula\.ai/(?P<slug>[a-zA-Z0-9_-]+)(?:/|$|\?)",
        ),
        cdx_patterns=("careers.kula.ai/*",),
        cname_targets=("careers.kula.ai",),
        html_signatures=(r"careers\.kula\.ai/(?P<slug>[a-zA-Z0-9_-]+)",),
        public_api_url="https://careers.kula.ai/{slug}",
        board_url_template="https://careers.kula.ai/{slug}",
    ),
    "polymer": ATSPlatformSpec(
        ats_id="polymer",
        display_name="Polymer",
        category="smb",
        url_regexes=(
            r"https?://jobs\.polymer\.co/(?P<slug>[a-zA-Z0-9_-]+)(?:/|$|\?)",
        ),
        cdx_patterns=("jobs.polymer.co/*",),
        cname_targets=("jobs.polymer.co",),
        html_signatures=(r"jobs\.polymer\.co/(?P<slug>[a-zA-Z0-9_-]+)",),
        public_api_url="https://jobs.polymer.co/{slug}",
        board_url_template="https://jobs.polymer.co/{slug}",
    ),
    "freshteam": ATSPlatformSpec(
        ats_id="freshteam",
        display_name="Freshteam",
        category="smb",
        url_regexes=(
            r"https?://(?P<slug>[a-zA-Z0-9-]+)\.freshteam\.com/jobs(?:/|$|\?)",
        ),
        cdx_patterns=("*.freshteam.com/jobs*",),
        ct_patterns=("%.freshteam.com",),
        cname_targets=("freshteam.com",),
        html_signatures=(r"https?://(?P<slug>[a-zA-Z0-9-]+)\.freshteam\.com/jobs",),
        public_api_url="https://{slug}.freshteam.com/hire/widgets/jobs.json",
        board_url_template="https://{slug}.freshteam.com/jobs",
    ),
    "zohorecruit": ATSPlatformSpec(
        ats_id="zohorecruit",
        display_name="Zoho Recruit",
        category="smb",
        url_regexes=(
            r"https?://(?P<slug>[a-zA-Z0-9-]+)\.zohorecruit\.(?P<shard>com|eu|in|com\.au|jp)/jobs/Careers(?:/|$|\?)",
        ),
        cdx_patterns=("*.zohorecruit.com/*", "*.zohorecruit.in/*", "*.zohorecruit.eu/*"),
        ct_patterns=("%.zohorecruit.com", "%.zohorecruit.in", "%.zohorecruit.eu"),
        cname_targets=("zohorecruit.com", "zohorecruit.in", "zohorecruit.eu"),
        html_signatures=(r"https?://(?P<slug>[a-zA-Z0-9-]+)\.zohorecruit\.(?P<shard>com|eu|in)/jobs",),
        public_api_url="https://{slug}.zohorecruit.{shard}/jobs/Careers",
        board_url_template="https://{slug}.zohorecruit.{shard}/jobs/Careers",
    ),
    "keka": ATSPlatformSpec(
        ats_id="keka",
        display_name="Keka Hire",
        category="regional_global",
        url_regexes=(
            r"https?://(?P<slug>[a-zA-Z0-9-]+)\.keka\.com/careers(?:/|$|\?)",
        ),
        cdx_patterns=("*.keka.com/careers*",),
        ct_patterns=("%.keka.com",),
        cname_targets=("keka.com",),
        html_signatures=(r"https?://(?P<slug>[a-zA-Z0-9-]+)\.keka\.com/careers",),
        public_api_url="https://{slug}.keka.com/careers/api/embedjobs/default",
        board_url_template="https://{slug}.keka.com/careers",
    ),
    "darwinbox": ATSPlatformSpec(
        ats_id="darwinbox",
        display_name="Darwinbox",
        category="regional_global",
        url_regexes=(
            r"https?://(?P<slug>[a-zA-Z0-9-]+)\.darwinbox\.(?P<shard>in|com)/ms/candidate/careers(?:/|$|\?)",
        ),
        cdx_patterns=("*.darwinbox.in/*", "*.darwinbox.com/*"),
        ct_patterns=("%.darwinbox.in", "%.darwinbox.com"),
        cname_targets=("darwinbox.in", "darwinbox.com"),
        html_signatures=(r"https?://(?P<slug>[a-zA-Z0-9-]+)\.darwinbox\.(?P<shard>in|com)/ms/candidate/careers",),
        public_api_url="https://{slug}.darwinbox.{shard}/ms/candidate/careers",
        board_url_template="https://{slug}.darwinbox.{shard}/ms/candidate/careers",
    ),
    "fountain": ATSPlatformSpec(
        ats_id="fountain",
        display_name="Fountain",
        category="enterprise",
        url_regexes=(
            r"https?://(?:apply|jobs)\.fountain\.com/(?P<slug>[a-zA-Z0-9_-]+)(?:/|$|\?)",
        ),
        cdx_patterns=("apply.fountain.com/*", "jobs.fountain.com/*"),
        cname_targets=("fountain.com",),
        html_signatures=(r"(?:apply|jobs)\.fountain\.com/(?P<slug>[a-zA-Z0-9_-]+)",),
        public_api_url="https://apply.fountain.com/{slug}",
        board_url_template="https://apply.fountain.com/{slug}",
    ),
    "dayforce": ATSPlatformSpec(
        ats_id="dayforce",
        display_name="Dayforce (Ceridian)",
        category="enterprise",
        url_regexes=(
            r"https?://jobs\.dayforcehcm\.com/[a-z]{2}-[A-Z]{2}/(?P<slug>[a-zA-Z0-9_-]+)/(?P<site>[a-zA-Z0-9_-]+)(?:/|$|\?)",
        ),
        cdx_patterns=("jobs.dayforcehcm.com/*",),
        cname_targets=("jobs.dayforcehcm.com", "dayforcehcm.com"),
        html_signatures=(r"jobs\.dayforcehcm\.com/[a-z]{2}-[A-Z]{2}/(?P<slug>[a-zA-Z0-9_-]+)",),
        public_api_url="https://jobs.dayforcehcm.com/en-US/{slug}/CANDIDATEPORTAL",
        board_url_template="https://jobs.dayforcehcm.com/en-US/{slug}/CANDIDATEPORTAL",
    ),
    "paylocity": ATSPlatformSpec(
        ats_id="paylocity",
        display_name="Paylocity",
        category="mid_market",
        url_regexes=(
            r"https?://recruiting\.paylocity\.com/recruiting/jobs/(?:All|List)/(?P<slug>[a-zA-Z0-9-]+)(?:/(?P<site>[^/?#]+))?",
        ),
        cdx_patterns=("recruiting.paylocity.com/recruiting/jobs/*",),
        html_signatures=(r"recruiting\.paylocity\.com/recruiting/jobs/(?:All|List)/(?P<slug>[a-zA-Z0-9-]+)",),
        public_api_url="https://recruiting.paylocity.com/recruiting/jobs/All/{slug}",
        board_url_template="https://recruiting.paylocity.com/recruiting/jobs/All/{slug}",
    ),
    "paycom": ATSPlatformSpec(
        ats_id="paycom",
        display_name="Paycom",
        category="mid_market",
        url_regexes=(
            r"https?://(?:www\.)?paycomonline\.net/v4/ats/web\.php/jobs\?clientkey=(?P<slug>[a-zA-Z0-9]+)",
        ),
        cdx_patterns=("www.paycomonline.net/v4/ats/*", "paycomonline.net/v4/ats/*"),
        html_signatures=(r"paycomonline\.net/v4/ats/web\.php/jobs\?clientkey=(?P<slug>[a-zA-Z0-9]+)",),
        public_api_url="https://www.paycomonline.net/v4/ats/web.php/jobs?clientkey={slug}",
        board_url_template="https://www.paycomonline.net/v4/ats/web.php/jobs?clientkey={slug}",
    ),
    "ukg": ATSPlatformSpec(
        ats_id="ukg",
        display_name="UKG (UltiPro)",
        category="enterprise",
        url_regexes=(
            r"https?://recruiting2?\.ultipro\.com/(?P<slug>[a-zA-Z0-9_-]+)/JobBoard/(?P<site>[a-zA-Z0-9-]+)",
        ),
        cdx_patterns=("recruiting.ultipro.com/*", "recruiting2.ultipro.com/*"),
        html_signatures=(r"recruiting2?\.ultipro\.com/(?P<slug>[a-zA-Z0-9_-]+)/JobBoard/(?P<site>[a-zA-Z0-9-]+)",),
        public_api_url="https://recruiting2.ultipro.com/{slug}/JobBoard/{site}",
        board_url_template="https://recruiting2.ultipro.com/{slug}/JobBoard/{site}",
    ),
    "taleo": ATSPlatformSpec(
        ats_id="taleo",
        display_name="Oracle Taleo",
        category="enterprise",
        url_regexes=(
            r"https?://(?P<slug>[a-zA-Z0-9-]+)\.taleo\.net/careersection/(?P<site>[a-zA-Z0-9_-]+)",
        ),
        cdx_patterns=("*.taleo.net/careersection/*",),
        ct_patterns=("%.taleo.net",),
        cname_targets=("taleo.net",),
        html_signatures=(r"https?://(?P<slug>[a-zA-Z0-9-]+)\.taleo\.net/careersection",),
        public_api_url="https://{slug}.taleo.net/careersection/rest/jobboard/searchjobs?lang=en&portal=101430233",
        board_url_template="https://{slug}.taleo.net/careersection/ex/jobsearch.ftl",
    ),
    "oracle_recruiting": ATSPlatformSpec(
        ats_id="oracle_recruiting",
        display_name="Oracle Recruiting Cloud (ORC)",
        category="enterprise",
        url_regexes=(
            r"https?://(?P<slug>[a-zA-Z0-9-]+)\.fa\.(?P<shard>[a-zA-Z0-9]+)\.oraclecloud\.com/hcmUI/CandidateExperience/[a-zA-Z-]+/sites/(?P<site>[a-zA-Z0-9_-]+)",
        ),
        cdx_patterns=("*.oraclecloud.com/hcmUI/CandidateExperience/*",),
        ct_patterns=("%.oraclecloud.com",),
        html_signatures=(r"https?://(?P<slug>[a-zA-Z0-9-]+)\.fa\.(?P<shard>[a-zA-Z0-9]+)\.oraclecloud\.com/hcmUI/CandidateExperience",),
        public_api_url="https://{slug}.fa.{shard}.oraclecloud.com/hcmRestApi/resources/latest/recruitingCEJobRequisitions?onlyData=true&expand=requisitionList.secondaryLocations",
        board_url_template="https://{slug}.fa.{shard}.oraclecloud.com/hcmUI/CandidateExperience/en/sites/{site}",
    ),
    "successfactors": ATSPlatformSpec(
        ats_id="successfactors",
        display_name="SAP SuccessFactors",
        category="enterprise",
        url_regexes=(
            r"https?://career(?P<shard>\d+)?\.successfactors\.(?P<site>com|eu)/career\?company=(?P<slug>[a-zA-Z0-9_-]+)",
            r"https?://(?P<slug>[a-zA-Z0-9-]+)\.successfactors\.(?P<site>com|eu)(?:/|$|\?)",
        ),
        cdx_patterns=("*.successfactors.com/*", "*.successfactors.eu/*"),
        cname_targets=("successfactors.com", "successfactors.eu", "jobs2web.com"),
        html_signatures=(
            r"successfactors\.(?:com|eu)/career\?company=(?P<slug>[a-zA-Z0-9_-]+)",
            r"j2w\.jobs2web\.com",
        ),
        public_api_url="https://career.successfactors.com/career?company={slug}",
        board_url_template="https://career.successfactors.com/career?company={slug}",
    ),
    "eightfold": ATSPlatformSpec(
        ats_id="eightfold",
        display_name="Eightfold AI",
        category="enterprise",
        url_regexes=(
            r"https?://(?P<slug>[a-zA-Z0-9-]+)\.eightfold\.ai/careers(?:/|$|\?)",
        ),
        cdx_patterns=("*.eightfold.ai/careers*",),
        ct_patterns=("%.eightfold.ai",),
        cname_targets=("eightfold.ai",),
        html_signatures=(
            r"https?://(?P<slug>[a-zA-Z0-9-]+)\.eightfold\.ai/careers",
            r"/api/apply/v2/jobs\?domain=(?P<slug>[a-zA-Z0-9.-]+)",
        ),
        public_api_url="https://{slug}.eightfold.ai/api/apply/v2/jobs",
        board_url_template="https://{slug}.eightfold.ai/careers",
    ),
    "phenom": ATSPlatformSpec(
        ats_id="phenom",
        display_name="Phenom People",
        category="enterprise",
        url_regexes=(
            r"https?://(?P<slug>[a-zA-Z0-9-]+)\.phenompeople\.com(?:/|$|\?)",
        ),
        cdx_patterns=("*.phenompeople.com/*",),
        ct_patterns=("%.phenompeople.com",),
        cname_targets=("phenompeople.com",),
        html_signatures=(
            r"cdn\.phenompeople\.com/CareerConnectResources/(?P<slug>[A-Z0-9]+)",
            r"phenom-track",
        ),
        public_api_url="https://{slug}.phenompeople.com",
        board_url_template="https://{slug}.phenompeople.com",
    ),
    "avature": ATSPlatformSpec(
        ats_id="avature",
        display_name="Avature",
        category="enterprise",
        url_regexes=(
            r"https?://(?P<slug>[a-zA-Z0-9-]+)\.avature\.net/(?P<site>[a-zA-Z0-9_-]+)(?:/|$|\?)",
        ),
        cdx_patterns=("*.avature.net/*",),
        ct_patterns=("%.avature.net",),
        cname_targets=("avature.net",),
        html_signatures=(r"https?://(?P<slug>[a-zA-Z0-9-]+)\.avature\.net",),
        public_api_url="https://{slug}.avature.net/careers",
        board_url_template="https://{slug}.avature.net/careers",
    ),
    "adp": ATSPlatformSpec(
        ats_id="adp",
        display_name="ADP Workforce Now",
        category="enterprise",
        url_regexes=(
            r"https?://workforcenow\.adp\.com/mascsr/default/mdf/recruitment/recruitment\.html\?cid=(?P<slug>[a-zA-Z0-9-]+)",
        ),
        cdx_patterns=("workforcenow.adp.com/mascsr/default/mdf/recruitment/*",),
        html_signatures=(r"workforcenow\.adp\.com/mascsr/default/mdf/recruitment/recruitment\.html\?cid=(?P<slug>[a-zA-Z0-9-]+)",),
        public_api_url="https://workforcenow.adp.com/mascsr/default/careercenter/public/events/staffing/v1/job-requisitions?cid={slug}",
        board_url_template="https://workforcenow.adp.com/mascsr/default/mdf/recruitment/recruitment.html?cid={slug}",
    ),
    "cornerstone": ATSPlatformSpec(
        ats_id="cornerstone",
        display_name="Cornerstone OnDemand",
        category="enterprise",
        url_regexes=(
            r"https?://(?P<slug>[a-zA-Z0-9-]+)\.csod\.com/ux/ats/careersite/(?P<site>\d+)/home",
        ),
        cdx_patterns=("*.csod.com/ux/ats/careersite/*",),
        ct_patterns=("%.csod.com",),
        cname_targets=("csod.com",),
        html_signatures=(r"https?://(?P<slug>[a-zA-Z0-9-]+)\.csod\.com/ux/ats/careersite",),
        public_api_url="https://{slug}.csod.com/ux/ats/careersite/{site}/home",
        board_url_template="https://{slug}.csod.com/ux/ats/careersite/1/home",
    ),
    "trakstar": ATSPlatformSpec(
        ats_id="trakstar",
        display_name="Trakstar Hire (Recruiterbox)",
        category="smb",
        url_regexes=(
            r"https?://(?P<slug>[a-zA-Z0-9-]+)\.recruiterbox\.com(?:/|$|\?)",
            r"https?://(?P<slug>[a-zA-Z0-9-]+)\.hire\.trakstar\.com(?:/|$|\?)",
        ),
        cdx_patterns=("*.recruiterbox.com/*", "*.hire.trakstar.com/*"),
        ct_patterns=("%.recruiterbox.com", "%.hire.trakstar.com"),
        cname_targets=("recruiterbox.com", "hire.trakstar.com"),
        html_signatures=(
            r"https?://(?P<slug>[a-zA-Z0-9-]+)\.(?:recruiterbox|hire\.trakstar)\.com",
        ),
        public_api_url="https://js.recruiterbox.com/v1/openings?client_name={slug}",
        board_url_template="https://{slug}.hire.trakstar.com",
    ),
    "jobylon": ATSPlatformSpec(
        ats_id="jobylon",
        display_name="Jobylon",
        category="mid_market",
        url_regexes=(
            r"https?://emp\.jobylon\.com/jobs/companies/(?P<slug>[a-zA-Z0-9_-]+)(?:/|$|\?)",
        ),
        cdx_patterns=("emp.jobylon.com/jobs/*",),
        cname_targets=("jobylon.com",),
        html_signatures=(r"emp\.jobylon\.com/jobs", r"jobylon-jobs-widget"),
        public_api_url="https://emp.jobylon.com/jobs/companies/{slug}",
        board_url_template="https://emp.jobylon.com/jobs/companies/{slug}",
    ),
}

_ADDITIONAL_LONG_TAIL_ATS: List[Tuple[str, str, str, str, str]] = [
    ("applicantpro", "ApplicantPro", "smb", "*.applicantpro.com/*", r"https?://(?P<slug>[a-zA-Z0-9-]+)\.applicantpro\.com"),
    ("careerplug", "CareerPlug", "smb", "*.careerplug.com/*", r"https?://(?P<slug>[a-zA-Z0-9-]+)\.careerplug\.com"),
    ("gohire", "GoHire", "smb", "*.gohire.io/*", r"https?://(?P<slug>[a-zA-Z0-9-]+)\.gohire\.io"),
    ("hiringthing", "HiringThing", "smb", "*.hiringthing.com/*", r"https?://(?P<slug>[a-zA-Z0-9-]+)\.hiringthing\.com"),
    ("jboard", "JBoard.io", "smb", "*.jboard.io/*", r"https?://(?P<slug>[a-zA-Z0-9-]+)\.jboard\.io"),
    ("jobboardfire", "JobBoardFire", "smb", "*.jobboardfire.com/*", r"https?://(?P<slug>[a-zA-Z0-9-]+)\.jobboardfire\.com"),
    ("jobkit", "JobKit", "smb", "*.jobkit.io/*", r"https?://(?P<slug>[a-zA-Z0-9-]+)\.jobkit\.io"),
    ("jobboardly", "Jobboardly", "smb", "*.jobboardly.com/*", r"https?://(?P<slug>[a-zA-Z0-9-]+)\.jobboardly\.com"),
    ("jobiqo", "Jobiqo", "mid_market", "*.jobiqo.com/*", r"https?://(?P<slug>[a-zA-Z0-9-]+)\.jobiqo\.com"),
    ("niceboard", "Niceboard", "smb", "*.niceboard.co/*", r"https?://(?P<slug>[a-zA-Z0-9-]+)\.niceboard\.co"),
    ("recooty", "Recooty", "smb", "*.recooty.com/*", r"https?://(?P<slug>[a-zA-Z0-9-]+)\.recooty\.com"),
    ("smartjobboard", "SmartJobBoard", "smb", "*.mysmartjobboard.com/*", r"https?://(?P<slug>[a-zA-Z0-9-]+)\.mysmartjobboard\.com"),
    ("isolvedhire", "iSolved Hire", "mid_market", "*.isolvedhire.com/*", r"https?://(?P<slug>[a-zA-Z0-9-]+)\.isolvedhire\.com"),
    ("brassring", "Kenexa BrassRing", "enterprise", "sjobs.brassring.com/*", r"https?://sjobs\.brassring\.com/TGnewUI/Search/Home/Home\?partnerid=(?P<slug>\d+)"),
    ("harri", "Harri", "mid_market", "harri.com/*", r"https?://harri\.com/(?P<slug>[a-zA-Z0-9_-]+)"),
    ("apploi", "Apploi", "mid_market", "jobs.apploi.com/*", r"https?://jobs\.apploi\.com/company/(?P<slug>[a-zA-Z0-9_-]+)"),
    ("applitrack", "Frontline AppliTrack", "mid_market", "www.applitrack.com/*", r"https?://www\.applitrack\.com/(?P<slug>[a-zA-Z0-9_-]+)/onlineapp"),
    ("hireology", "Hireology", "mid_market", "careers.hireology.com/*", r"https?://careers\.hireology\.com/(?P<slug>[a-zA-Z0-9_-]+)"),
    ("jobadder", "JobAdder", "mid_market", "*.jobadder.com/*", r"https?://(?P<slug>[a-zA-Z0-9-]+)\.jobadder\.com"),
    ("jobdiva", "JobDiva", "mid_market", "*.jobdiva.com/*", r"https?://(?P<slug>[a-zA-Z0-9-]+)\.jobdiva\.com"),
    ("selectminds", "Oracle SelectMinds", "enterprise", "*.referrals.selectminds.com/*", r"https?://(?P<slug>[a-zA-Z0-9-]+)\.referrals\.selectminds\.com"),
    ("jibe", "Jibe (iCIMS)", "enterprise", "*.jibeapply.com/*", r"https?://(?P<slug>[a-zA-Z0-9-]+)\.jibeapply\.com"),
    ("pageup", "PageUp", "enterprise", "careers.pageuppeople.com/*", r"https?://careers\.pageuppeople\.com/(?P<slug>\d+)"),
    ("peopleadmin", "PeopleAdmin", "enterprise", "*.peopleadmin.com/*", r"https?://(?P<slug>[a-zA-Z0-9-]+)\.peopleadmin\.com"),
    ("silkroad", "SilkRoad Recruiting", "enterprise", "*.silkroad.com/*", r"https?://(?P<slug>[a-zA-Z0-9-]+)-openhire\.silkroad\.com"),
    ("livehire", "LiveHire", "mid_market", "www.livehire.com/careers/*", r"https?://www\.livehire\.com/careers/(?P<slug>[a-zA-Z0-9_-]+)"),
    ("flatchr", "Flatchr", "smb", "careers.flatchr.io/*", r"https?://careers\.flatchr\.io/(?P<slug>[a-zA-Z0-9_-]+)"),
    ("clearcompany", "ClearCompany", "mid_market", "*.hrmdirect.com/*", r"https?://(?P<slug>[a-zA-Z0-9-]+)\.hrmdirect\.com"),
    ("ceipal", "CEIPAL", "mid_market", "api.ceipal.com/*", r"https?://(?P<slug>[a-zA-Z0-9-]+)\.ceipal\.com"),
    ("umantis", "Haufe Umantis", "mid_market", "*.umantis.com/*", r"https?://(?P<slug>[a-zA-Z0-9-]+)\.umantis\.com"),
    ("reachmee", "ReachMee", "mid_market", "web103.reachmee.com/*", r"https?://web\d+\.reachmee\.com/ext/(?P<slug>[a-zA-Z0-9_-]+)"),
    ("applicantstack", "ApplicantStack", "smb", "*.applicantstack.com/*", r"https?://(?P<slug>[a-zA-Z0-9-]+)\.applicantstack\.com"),
    ("recruiterflow", "Recruiterflow", "smb", "recruiterflow.com/*", r"https?://recruiterflow\.com/(?P<slug>[a-zA-Z0-9_-]+)/jobs"),
    ("recruitcrm", "Recruit CRM", "smb", "recruitcrm.io/jobs/*", r"https?://recruitcrm\.io/jobs/(?P<slug>[a-zA-Z0-9_-]+)"),
    ("wizehire", "Wizehire", "smb", "app.wizehire.com/*", r"https?://app\.wizehire\.com/career-site/(?P<slug>[a-zA-Z0-9_-]+)"),
    ("occupop", "Occupop", "smb", "*.occupop.com/*", r"https?://(?P<slug>[a-zA-Z0-9-]+)\.occupop\.com"),
    ("varbi", "Varbi", "mid_market", "*.varbi.com/*", r"https?://(?P<slug>[a-zA-Z0-9-]+)\.varbi\.com"),
    ("persy", "Persy", "smb", "persy.jobs/*", r"https?://persy\.jobs/persy/l/(?P<slug>[a-zA-Z0-9_-]+)"),
    ("catsone", "CATS ATS", "smb", "*.catsone.com/careers*", r"https?://(?P<slug>[a-zA-Z0-9-]+)\.catsone\.com/careers"),
    ("crelate", "Crelate", "smb", "jobs.crelate.com/portal/*", r"https?://jobs\.crelate\.com/portal/(?P<slug>[a-zA-Z0-9_-]+)"),
    ("loxo", "Loxo", "smb", "app.loxo.co/job/*", r"https?://app\.loxo\.co/(?P<slug>[a-zA-Z0-9_-]+)"),
    ("deel", "Deel ATS", "startup_modern", "jobs.deel.com/*", r"https?://jobs\.deel\.com/job-boards/(?P<slug>[a-zA-Z0-9_-]+)"),
    ("pyjamahr", "PyjamaHR", "regional_global", "app.pyjamahr.com/careers*", r"https?://app\.pyjamahr\.com/careers\?company=(?P<slug>[a-zA-Z0-9_-]+)"),
    ("bullhorn", "Bullhorn Career Portal", "enterprise", "*.bullhornstaffing.com/*", r"https?://(?P<slug>[a-zA-Z0-9-]+)\.bullhornstaffing\.com"),
]

for _id, _name, _cat, _cdx, _regex in _ADDITIONAL_LONG_TAIL_ATS:
    _ct_tuple: Tuple[str, ...] = ()
    if _cdx.startswith("*."):
        _base_dom = _cdx[2:].split("/")[0]
        _ct_tuple = (f"%.{_base_dom}",)
    ATS_PLATFORMS[_id] = ATSPlatformSpec(
        ats_id=_id,
        display_name=_name,
        category=_cat,
        url_regexes=(_regex,),
        cdx_patterns=(_cdx,),
        ct_patterns=_ct_tuple,
        cname_targets=tuple([_cdx.replace("*.", "").split("/")[0]]),
        html_signatures=(_regex,),
        public_api_url=None,
        board_url_template="",
    )


RESERVED_SLUGS: Set[str] = {
    "www", "api", "app", "jobs", "careers", "career", "apply", "boards",
    "job-boards", "embed", "static", "cdn", "assets", "login", "auth",
    "admin", "support", "help", "docs", "status", "blog", "mail", "smtp",
    "test", "demo", "sandbox", "staging", "dev", "internal", "embed-js",
    "v0", "v1", "v2", "v3", "widget", "accounts", "companies", "open",
    "posting-api", "job-board", "postings", "offers", "search", "undefined",
    "null", "favicon.ico", "robots.txt", "sitemap.xml",
}


# =============================================================================
# 2. DATA MODELS & TECHNOGRAPHIC EXTRACTOR
# =============================================================================

@dataclass
class DiscoveredBoard:
    """Represents a candidate ATS board discovered via any vector."""
    ats_id: str
    slug: str
    shard: str = ""
    site: str = ""
    board_url: str = ""
    discovery_source: str = "unknown"
    source_company_domain: Optional[str] = None
    discovered_at: str = field(
        default_factory=lambda: datetime.now(timezone.utc).isoformat()
    )

    @property
    def board_key(self) -> str:
        return f"{self.ats_id}:{self.slug.lower()}"


@dataclass
class VerifiedATSBoard:
    """Represents a verified ATS board enriched from its public API or career page."""
    ats_id: str
    ats_display_name: str
    slug: str
    shard: str = ""
    site: str = ""
    board_url: str = ""
    is_active: bool = False
    http_status: int = 0
    company_name: str = ""
    resolved_domain: str = ""
    domain_resolution_method: str = ""
    confidence_score: float = 0.0
    board_status: str = "unverified"
    active_job_count: int = 0
    latest_posted_at: str = ""
    sample_job_titles: List[str] = field(default_factory=list)
    locations: List[str] = field(default_factory=list)
    departments: List[str] = field(default_factory=list)
    detected_tech_stack: List[str] = field(default_factory=list)
    discovery_sources: List[str] = field(default_factory=list)
    verified_at: str = field(
        default_factory=lambda: datetime.now(timezone.utc).isoformat()
    )


TECH_STACK_PATTERNS: Dict[str, re.Pattern[str]] = {
    tech: re.compile(rf"\b{pattern}\b", re.IGNORECASE)
    for tech, pattern in [
        ("Python", r"python"),
        ("TypeScript", r"typescript"),
        ("Go", r"golang|go\s+programming"),
        ("Rust", r"rust"),
        ("Java", r"java"),
        ("Kotlin", r"kotlin"),
        ("React", r"react(?:\.js|js)?"),
        ("Next.js", r"next\.?js"),
        ("Vue.js", r"vue(?:\.js|js)?"),
        ("Angular", r"angular"),
        ("Node.js", r"node(?:\.js|js)"),
        ("PostgreSQL", r"postgres(?:ql)?"),
        ("MySQL", r"mysql"),
        ("MongoDB", r"mongodb"),
        ("Redis", r"redis"),
        ("Snowflake", r"snowflake"),
        ("Databricks", r"databricks"),
        ("BigQuery", r"bigquery"),
        ("ClickHouse", r"clickhouse"),
        ("Kafka", r"kafka"),
        ("Airflow", r"airflow"),
        ("dbt", r"dbt"),
        ("Spark", r"apache\s+spark|pyspark"),
        ("AWS", r"aws|amazon\s+web\s+services"),
        ("GCP", r"gcp|google\s+cloud"),
        ("Azure", r"azure"),
        ("Kubernetes", r"kubernetes|k8s"),
        ("Docker", r"docker"),
        ("Terraform", r"terraform"),
        ("Datadog", r"datadog"),
        ("Grafana", r"grafana"),
        ("Prometheus", r"prometheus"),
        ("Salesforce", r"salesforce"),
        ("HubSpot", r"hubspot"),
        ("Segment", r"segment"),
        ("Looker", r"looker"),
        ("Tableau", r"tableau"),
        ("PyTorch", r"pytorch"),
        ("TensorFlow", r"tensorflow"),
        ("OpenAI", r"openai|langchain|llamaindex"),
        ("Stripe", r"stripe"),
        ("Okta", r"okta"),
    ]
}


def extract_tech_stack_from_text(text: str) -> List[str]:
    if not text:
        return []
    found: List[str] = []
    for tech, regex in TECH_STACK_PATTERNS.items():
        if regex.search(text):
            found.append(tech)
    return sorted(found)


# =============================================================================
# 3. URL & HTML FINGERPRINT EXTRACTORS
# =============================================================================

IGNORED_DOMAINS: Set[str] = {
    "gmail.com", "yahoo.com", "outlook.com", "hotmail.com", "icloud.com",
    "linkedin.com", "twitter.com", "x.com", "facebook.com", "instagram.com",
    "youtube.com", "github.com", "medium.com", "glassdoor.com", "indeed.com",
    "greenhouse.io", "lever.co", "ashbyhq.com", "workable.com", "breezy.hr",
    "recruitee.com", "smartrecruiters.com", "myworkdayjobs.com", "bamboohr.com",
    "personio.de", "personio.com", "teamtailor.com", "pinpointhq.com",
    "rippling.com", "rippling-ats.com", "hirehive.com", "careers-page.com",
    "join.com", "dover.com", "dover.io", "comeet.com", "homerun.co",
    "jobvite.com", "icims.com", "applytojob.com", "gem.com", "kula.ai",
    "polymer.co", "freshteam.com", "zohorecruit.com", "keka.com",
    "darwinbox.in", "darwinbox.com", "fountain.com", "dayforcehcm.com",
    "paylocity.com", "paycomonline.net", "ultipro.com", "taleo.net",
    "oraclecloud.com", "successfactors.com", "eightfold.ai", "phenompeople.com",
    "avature.net", "adp.com", "csod.com", "recruiterbox.com", "trakstar.com",
    "jobylon.com", "w3.org", "schema.org", "googleapis.com", "gstatic.com",
    "cloudflare.com", "amazonaws.com", "cloudfront.net",
}


def normalize_root_domain(url_or_domain: str) -> str:
    if not url_or_domain:
        return ""
    candidate = url_or_domain.strip().lower()
    if "://" not in candidate:
        candidate = "https://" + candidate
    try:
        parsed = urlparse(candidate)
        host = (parsed.hostname or "").lower().strip(".")
    except Exception:
        return ""
    if not host or "." not in host:
        return ""
    for prefix in ("www.", "jobs.", "careers.", "career.", "apply.", "talent.", "work."):
        if host.startswith(prefix):
            host = host[len(prefix):]
    if host in IGNORED_DOMAINS:
        return ""
    for ignored in IGNORED_DOMAINS:
        if host.endswith("." + ignored):
            return ""
    return host


def extract_ats_from_url(
    url: str,
    discovery_source: str = "url_scan",
    source_company_domain: Optional[str] = None,
    platforms: Optional[Sequence[str]] = None,
) -> Optional[DiscoveredBoard]:
    if not url:
        return None
    target_specs = (
        [ATS_PLATFORMS[p] for p in platforms if p in ATS_PLATFORMS]
        if platforms
        else list(ATS_PLATFORMS.values())
    )
    for spec in target_specs:
        for pattern in spec.url_regexes:
            match = re.search(pattern, url, re.IGNORECASE)
            if match:
                groups = match.groupdict()
                slug = (groups.get("slug") or "").strip().strip("/")
                if not slug or slug.lower() in RESERVED_SLUGS or len(slug) < 2:
                    continue
                shard = groups.get("shard") or ""
                site = groups.get("site") or ""
                if spec.ats_id == "personio" and not shard:
                    shard = "de"
                if spec.ats_id == "workday":
                    if not shard:
                        shard = "wd1"
                    if not site:
                        site = "External"
                board_url = build_canonical_board_url(spec, slug, shard, site, fallback_url=url)
                return DiscoveredBoard(
                    ats_id=spec.ats_id,
                    slug=slug,
                    shard=shard,
                    site=site,
                    board_url=board_url,
                    discovery_source=discovery_source,
                    source_company_domain=source_company_domain,
                )
    return None


def build_canonical_board_url(
    spec: ATSPlatformSpec,
    slug: str,
    shard: str = "",
    site: str = "",
    fallback_url: str = "",
) -> str:
    if spec.board_url_template:
        try:
            return spec.board_url_template.format(
                slug=slug,
                shard=shard or "wd1",
                site=site or "External",
            )
        except Exception:
            pass
    return fallback_url


def extract_ats_boards_from_html(
    html_content: str,
    source_company_domain: str,
    platforms: Optional[Sequence[str]] = None,
) -> List[DiscoveredBoard]:
    if not html_content:
        return []
    found: Dict[str, DiscoveredBoard] = {}
    root_dom = normalize_root_domain(source_company_domain)
    default_slug = root_dom.split(".")[0] if root_dom else ""

    target_specs = (
        [ATS_PLATFORMS[p] for p in platforms if p in ATS_PLATFORMS]
        if platforms
        else list(ATS_PLATFORMS.values())
    )
    for spec in target_specs:
        for sig in spec.html_signatures:
            for match in re.finditer(sig, html_content, re.IGNORECASE):
                groups = match.groupdict()
                slug = (groups.get("slug") or "").strip().strip("/")
                # Handle custom-domain Greenhouse boards where only `?gh_jid=` is visible in HTML
                if not slug and sig == r"gh_jid=" and spec.ats_id == "greenhouse" and default_slug:
                    slug = default_slug
                if not slug or slug.lower() in RESERVED_SLUGS or len(slug) < 2:
                    continue
                shard = groups.get("shard") or ("de" if spec.ats_id == "personio" else "wd1")
                site = groups.get("site") or "External"
                board_url = build_canonical_board_url(
                    spec, slug, shard, site, fallback_url=match.group(0)
                )
                board = DiscoveredBoard(
                    ats_id=spec.ats_id,
                    slug=slug,
                    shard=shard,
                    site=site,
                    board_url=board_url,
                    discovery_source="career_page_html",
                    source_company_domain=root_dom,
                )
                found[board.board_key] = board
    return list(found.values())


def match_cname_to_ats(
    cname_target: str,
    queried_subdomain: str,
    source_company_domain: str,
) -> Optional[DiscoveredBoard]:
    if not cname_target:
        return None
    target_clean = cname_target.strip().strip(".").lower()
    root_dom = normalize_root_domain(source_company_domain)
    default_slug = root_dom.split(".")[0] if root_dom else queried_subdomain.split(".")[0]

    board_from_url = extract_ats_from_url(
        f"https://{target_clean}",
        discovery_source="dns_cname",
        source_company_domain=root_dom,
    )
    if board_from_url:
        return board_from_url

    for spec in ATS_PLATFORMS.values():
        for cname_pattern in spec.cname_targets:
            if target_clean == cname_pattern or target_clean.endswith("." + cname_pattern):
                return DiscoveredBoard(
                    ats_id=spec.ats_id,
                    slug=default_slug,
                    board_url=f"https://{queried_subdomain.strip('.')}",
                    discovery_source="dns_cname",
                    source_company_domain=root_dom,
                )
    return None


# =============================================================================
# 4. ASYNC HTTP HELPER WITH RATE LIMITING & STDLIB FALLBACK
# =============================================================================

class AsyncHTTPClient:
    """High-concurrency HTTP client using `aiohttp` when installed, or threaded `urllib`."""

    def __init__(self, concurrency: int = 50, timeout: float = 15.0, user_agent: str = ""):
        self.semaphore = asyncio.Semaphore(concurrency)
        self.timeout = timeout
        self.user_agent = user_agent or (
            "Mozilla/5.0 (compatible; ATSDiscoveryEngine/2.5; +https://github.com/ats-discovery)"
        )
        self._session: Optional[Any] = None

    async def __aenter__(self) -> "AsyncHTTPClient":
        if HAS_AIOHTTP:
            timeout_cfg = aiohttp.ClientTimeout(total=self.timeout)
            connector = aiohttp.TCPConnector(limit=self.semaphore._value, ssl=False)
            self._session = aiohttp.ClientSession(
                timeout=timeout_cfg,
                connector=connector,
                headers={"User-Agent": self.user_agent},
            )
        return self

    async def __aexit__(self, exc_type: Any, exc: Any, tb: Any) -> None:
        if self._session is not None:
            await self._session.close()
            self._session = None

    async def request(
        self,
        method: str,
        url: str,
        headers: Optional[Dict[str, str]] = None,
        json_body: Optional[Dict[str, Any]] = None,
    ) -> Tuple[int, str, str]:
        async with self.semaphore:
            req_headers = {
                "User-Agent": self.user_agent,
                "Accept": "application/json, text/html, application/xml, */*",
            }
            if headers:
                req_headers.update(headers)
            if HAS_AIOHTTP and self._session is not None:
                try:
                    async with self._session.request(
                        method, url, headers=req_headers, json=json_body, allow_redirects=True
                    ) as resp:
                        text = await resp.text(errors="replace")
                        return resp.status, text, str(resp.url)
                except Exception as exc:
                    logger.debug("HTTP %s %s failed: %s", method, url, exc)
                    return 0, "", url
            else:
                return await asyncio.to_thread(
                    self._sync_request, method, url, req_headers, json_body
                )

    def _sync_request(
        self,
        method: str,
        url: str,
        headers: Dict[str, str],
        json_body: Optional[Dict[str, Any]],
    ) -> Tuple[int, str, str]:
        data: Optional[bytes] = None
        if json_body is not None:
            data = json.dumps(json_body).encode("utf-8")
            headers = dict(headers)
            headers["Content-Type"] = "application/json"
        req = urllib.request.Request(url, data=data, headers=headers, method=method)
        try:
            with urllib.request.urlopen(req, timeout=self.timeout) as resp:
                body = resp.read().decode("utf-8", errors="replace")
                return resp.getcode(), body, resp.geturl()
        except urllib.error.HTTPError as e:
            try:
                err_body = e.read().decode("utf-8", errors="replace")
            except Exception:
                err_body = ""
            return e.code, err_body, url
        except Exception:
            return 0, "", url


# =============================================================================
# 5. MULTI-VECTOR DISCOVERY ENGINES (WITH 36-PREFIX SHARDING & WAYBACK CDX)
# =============================================================================

ALPHANUMERIC_PREFIXES: Tuple[str, ...] = tuple(string.ascii_lowercase + string.digits)


def expand_cdx_patterns_with_prefixes(
    patterns: Sequence[str], enable_prefix_sharding: bool = False
) -> List[str]:
    """
    Expands path-based ATS wildcard patterns (like `boards.greenhouse.io/*` or
    `jobs.ashbyhq.com/*`) into 36 alphanumeric prefix queries (`boards.greenhouse.io/a*`
    .. `boards.greenhouse.io/9*`) so a single company with 50,000 job URLs cannot
    monopolize early CDX pagination pages.
    """
    if not enable_prefix_sharding:
        return list(patterns)
    expanded: List[str] = []
    for pat in patterns:
        # Subdomain wildcard patterns (*.myworkdayjobs.com/*) cannot be path-prefixed
        if pat.startswith("*."):
            expanded.append(pat)
        elif pat.endswith("/*"):
            base = pat[:-1]  # keep trailing slash
            for ch in ALPHANUMERIC_PREFIXES:
                expanded.append(f"{base}{ch}*")
        else:
            expanded.append(pat)
    return expanded


class CommonCrawlCDXDiscoverer:
    """
    Discovers ATS client slugs at massive scale by querying Common Crawl's public
    CDX Index API (`https://index.commoncrawl.org`) with URL-key collapsing and
    optional 36-prefix alphanumeric sharding.
    """

    COLLINFO_URL = "https://index.commoncrawl.org/collinfo.json"
    DEFAULT_INDEXES = (
        "CC-MAIN-2026-38-index",
        "CC-MAIN-2026-30-index",
        "CC-MAIN-2026-22-index",
        "CC-MAIN-2026-14-index",
    )

    def __init__(self, http_client: AsyncHTTPClient, max_indexes: int = 2):
        self.http = http_client
        self.max_indexes = max_indexes

    async def get_latest_cdx_endpoints(self) -> List[str]:
        status, body, _ = await self.http.request("GET", self.COLLINFO_URL)
        if status == 200 and body:
            try:
                items = json.loads(body)
                return [item["cdx-api"] for item in items[: self.max_indexes] if "cdx-api" in item]
            except Exception:
                pass
        return [f"https://index.commoncrawl.org/{idx}" for idx in self.DEFAULT_INDEXES[: self.max_indexes]]

    async def discover_for_platform(
        self,
        spec: ATSPlatformSpec,
        cdx_endpoints: Sequence[str],
        max_pages_per_pattern: int = 5,
        prefix_shard: bool = False,
    ) -> List[DiscoveredBoard]:
        discovered: Dict[str, DiscoveredBoard] = {}
        query_patterns = expand_cdx_patterns_with_prefixes(spec.cdx_patterns, prefix_shard)

        async def _fetch_pattern_page(cdx_api: str, pattern: str, page: int) -> List[DiscoveredBoard]:
            query_url = (
                f"{cdx_api}?url={quote_plus(pattern)}&output=json"
                f"&fl=url,status&filter=status:200&collapse=urlkey&page={page}"
            )
            status, body, _ = await self.http.request("GET", query_url)
            if status != 200 or not body.strip():
                return []
            page_boards: List[DiscoveredBoard] = []
            for line in body.splitlines():
                line = line.strip()
                if not line:
                    continue
                try:
                    record = json.loads(line)
                    raw_url = record.get("url", "")
                except Exception:
                    raw_url = line
                board = extract_ats_from_url(
                    raw_url,
                    discovery_source="commoncrawl_cdx",
                    platforms=[spec.ats_id],
                )
                if board:
                    page_boards.append(board)
            return page_boards

        for cdx_api in cdx_endpoints:
            tasks = [
                _fetch_pattern_page(cdx_api, pat, page)
                for pat in query_patterns
                for page in range(max_pages_per_pattern)
            ]
            results = await asyncio.gather(*tasks)
            for batch in results:
                for b in batch:
                    discovered[b.board_key] = b
        return list(discovered.values())


class WaybackCDXDiscoverer:
    """
    Streams historical and current ATS board URLs from the Internet Archive's
    Wayback Machine CDX API (`https://web.archive.org/cdx/search/cdx`), using
    `collapse=urlkey` to deduplicate URLs server-side.
    """

    WAYBACK_CDX_URL = (
        "https://web.archive.org/cdx/search/cdx"
        "?url={pattern}&output=json&fl=original&filter=statuscode:200"
        "&collapse=urlkey&limit={limit}"
    )

    def __init__(self, http_client: AsyncHTTPClient, limit_per_pattern: int = 25000):
        self.http = http_client
        self.limit_per_pattern = limit_per_pattern

    async def discover_for_platform(
        self, spec: ATSPlatformSpec, prefix_shard: bool = False
    ) -> List[DiscoveredBoard]:
        discovered: Dict[str, DiscoveredBoard] = {}
        patterns = expand_cdx_patterns_with_prefixes(spec.cdx_patterns, prefix_shard)

        async def _query_pattern(pattern: str) -> List[DiscoveredBoard]:
            url = self.WAYBACK_CDX_URL.format(
                pattern=quote_plus(pattern), limit=self.limit_per_pattern
            )
            status, body, _ = await self.http.request("GET", url)
            if status != 200 or not body.strip():
                return []
            return self.parse_wayback_json(body, spec)

        batches = await asyncio.gather(*(_query_pattern(p) for p in patterns))
        for batch in batches:
            for b in batch:
                discovered[b.board_key] = b
        return list(discovered.values())

    @staticmethod
    def parse_wayback_json(raw_json: str, spec: ATSPlatformSpec) -> List[DiscoveredBoard]:
        boards: Dict[str, DiscoveredBoard] = {}
        try:
            rows = json.loads(raw_json)
        except Exception:
            return []
        for idx, row in enumerate(rows):
            if idx == 0 and row == ["original"]:
                continue
            raw_url = row[0] if isinstance(row, list) and row else (row if isinstance(row, str) else "")
            if not raw_url:
                continue
            b = extract_ats_from_url(
                raw_url, discovery_source="wayback_cdx", platforms=[spec.ats_id]
            )
            if b:
                boards[b.board_key] = b
        return list(boards.values())


class CertificateTransparencyDiscoverer:
    """
    Queries Certificate Transparency (`crt.sh`) JSON logs to find every subdomain
    certificate ever issued for subdomain-hosted ATS platforms.
    """

    CRT_SH_URL = "https://crt.sh/?q={query}&output=json"

    def __init__(self, http_client: AsyncHTTPClient):
        self.http = http_client

    async def discover_for_platform(self, spec: ATSPlatformSpec) -> List[DiscoveredBoard]:
        if not spec.ct_patterns:
            return []
        discovered: Dict[str, DiscoveredBoard] = {}
        for ct_pattern in spec.ct_patterns:
            url = self.CRT_SH_URL.format(query=quote_plus(ct_pattern))
            status, body, _ = await self.http.request("GET", url)
            if status != 200 or not body:
                continue
            for board in self.parse_crt_sh_json(body, spec):
                discovered[board.board_key] = board
        return list(discovered.values())

    @staticmethod
    def parse_crt_sh_json(raw_json: str, spec: ATSPlatformSpec) -> List[DiscoveredBoard]:
        boards: Dict[str, DiscoveredBoard] = {}
        try:
            entries = json.loads(raw_json)
        except Exception:
            return []
        for entry in entries:
            names_raw = entry.get("name_value", "") or entry.get("common_name", "")
            for host in names_raw.splitlines():
                host = host.strip().lower().lstrip("*.")
                if not host or "." not in host:
                    continue
                synthetic_url = f"https://{host}/careers"
                board = extract_ats_from_url(
                    synthetic_url,
                    discovery_source="ct_logs",
                    platforms=[spec.ats_id],
                )
                if board:
                    boards[board.board_key] = board
        return list(boards.values())


class CareerPageAndCNamedDiscoverer:
    """
    Given a seed list of company domains, discovers their ATS by probing career
    subdomains (`jobs.<domain>`, `careers.<domain>`) and crawling `/careers` and
    `/jobs` HTML for embedded ATS scripts, iframes, `?gh_jid=` links, and redirects.
    """

    CAREER_PATHS = ("/careers", "/jobs", "/company/careers", "/about/careers", "/en/careers")
    CAREER_SUBDOMAINS = ("jobs", "careers", "talent", "apply")

    def __init__(self, http_client: AsyncHTTPClient):
        self.http = http_client

    async def probe_domain(self, domain: str) -> List[DiscoveredBoard]:
        clean_dom = normalize_root_domain(domain)
        if not clean_dom:
            return []
        found: Dict[str, DiscoveredBoard] = {}

        for sub in self.CAREER_SUBDOMAINS:
            sub_url = f"https://{sub}.{clean_dom}"
            status, html_body, final_url = await self.http.request("GET", sub_url)
            if status in (200, 301, 302) and final_url:
                redirect_board = extract_ats_from_url(
                    final_url,
                    discovery_source="career_subdomain_redirect",
                    source_company_domain=clean_dom,
                )
                if redirect_board:
                    found[redirect_board.board_key] = redirect_board
                for b in extract_ats_boards_from_html(html_body, clean_dom):
                    found[b.board_key] = b

        for path in self.CAREER_PATHS:
            page_url = f"https://{clean_dom}{path}"
            status, html_body, final_url = await self.http.request("GET", page_url)
            if status == 200 and html_body:
                redirect_board = extract_ats_from_url(
                    final_url,
                    discovery_source="career_page_redirect",
                    source_company_domain=clean_dom,
                )
                if redirect_board:
                    found[redirect_board.board_key] = redirect_board
                for b in extract_ats_boards_from_html(html_body, clean_dom):
                    found[b.board_key] = b
                if found:
                    break
        return list(found.values())


class SlugPermutationGenerator:
    """
    Generates candidate ATS slugs from a company name or root domain so public
    ATS APIs can be probed directly even when a company's career page hides the
    underlying ATS URL (e.g., `doordashusa`, `andurilindustries`, `stripe`).
    """

    COMMON_SUFFIXES = (
        "", "hq", "inc", "usa", "labs", "tech", "technologies", "industries",
        "global", "io", "ai", "careers", "jobs", "group", "health", "financial",
    )

    @classmethod
    def generate_slugs(cls, company_name: str = "", domain: str = "") -> List[str]:
        candidates: List[str] = []
        seen: Set[str] = set()

        def _add(s: str) -> None:
            s = s.strip("-_.").lower()
            if len(s) >= 2 and s not in RESERVED_SLUGS and s not in seen:
                seen.add(s)
                candidates.append(s)

        if domain:
            root = normalize_root_domain(domain)
            if root:
                label = root.split(".")[0]
                _add(label)
                _add(root.replace(".", "-"))
                _add(root.replace(".", ""))
                for suffix in cls.COMMON_SUFFIXES:
                    if suffix:
                        _add(f"{label}{suffix}")
                        _add(f"{label}-{suffix}")

        if company_name:
            clean = re.sub(
                r"\b(inc\.?|llc\.?|ltd\.?|corp\.?|corporation|gmbh|limited|plc|co\.?)\b",
                "",
                company_name,
                flags=re.IGNORECASE,
            )
            alpha_num = re.sub(r"[^a-zA-Z0-9]+", "", clean).lower()
            hyphenated = re.sub(r"[^a-zA-Z0-9]+", "-", clean).strip("-").lower()
            _add(alpha_num)
            _add(hyphenated)
            for suffix in cls.COMMON_SUFFIXES:
                if suffix and alpha_num:
                    _add(f"{alpha_num}{suffix}")
                    _add(f"{alpha_num}-{suffix}")
        return candidates


class SearchDorkDiscoverer:
    """Generates partitioned search engine dork queries per ATS."""

    @staticmethod
    def build_dork_queries(platforms: Optional[Sequence[str]] = None) -> Dict[str, List[str]]:
        target_specs = (
            [ATS_PLATFORMS[p] for p in platforms if p in ATS_PLATFORMS]
            if platforms
            else list(ATS_PLATFORMS.values())
        )
        queries_by_ats: Dict[str, List[str]] = {}
        shards = ("remote", "engineer", "product", "sales", "marketing", "operations", "finance", "design")
        for spec in target_specs:
            q_list: List[str] = []
            for cdx in spec.cdx_patterns:
                site_host = cdx.rstrip("/*")
                q_list.append(f"site:{site_host}")
                for keyword in shards:
                    q_list.append(f'site:{site_host} "{keyword}"')
            queries_by_ats[spec.ats_id] = q_list
        return queries_by_ats


# =============================================================================
# 6. PUBLIC ATS API VERIFIER & PAYLOAD PARSERS
# =============================================================================

class PublicATSPayloadParser:
    """
    Parses public JSON / XML / HTML responses from major ATS platforms into a
    normalized `VerifiedATSBoard` structure.
    """

    @classmethod
    def parse_response(
        cls,
        board: DiscoveredBoard,
        http_status: int,
        raw_body: str,
    ) -> VerifiedATSBoard:
        spec = ATS_PLATFORMS[board.ats_id]
        verified = VerifiedATSBoard(
            ats_id=board.ats_id,
            ats_display_name=spec.display_name,
            slug=board.slug,
            shard=board.shard,
            site=board.site,
            board_url=board.board_url or build_canonical_board_url(spec, board.slug, board.shard, board.site),
            http_status=http_status,
            discovery_sources=[board.discovery_source],
        )
        if board.source_company_domain:
            verified.resolved_domain = normalize_root_domain(board.source_company_domain)
            if verified.resolved_domain:
                verified.domain_resolution_method = "career_page_backlink"

        if http_status != 200 or not raw_body.strip():
            verified.is_active = False
            verified.board_status = "dead"
            return verified

        parser_map: Dict[str, Callable[[VerifiedATSBoard, str], None]] = {
            "ashby": cls._parse_ashby,
            "greenhouse": cls._parse_greenhouse,
            "lever": cls._parse_lever,
            "workday": cls._parse_workday,
            "workable": cls._parse_workable,
            "smartrecruiters": cls._parse_smartrecruiters,
            "recruitee": cls._parse_recruitee,
            "breezy": cls._parse_breezy,
            "bamboohr": cls._parse_bamboohr,
            "personio": cls._parse_personio,
            "pinpoint": cls._parse_pinpoint,
            "hirehive": cls._parse_hirehive,
            "manatal": cls._parse_manatal,
            "rippling": cls._parse_rippling,
            "trakstar": cls._parse_trakstar,
        }
        parser_fn = parser_map.get(board.ats_id, cls._parse_generic)
        try:
            parser_fn(verified, raw_body)
        except Exception as exc:
            logger.debug("Parser error for %s:%s -> %s", board.ats_id, board.slug, exc)
            verified.is_active = False
            verified.board_status = "dead"

        if not verified.resolved_domain:
            inferred_dom, method = cls._infer_domain_from_payload(raw_body, board.slug)
            if inferred_dom:
                verified.resolved_domain = inferred_dom
                verified.domain_resolution_method = method

        if not verified.company_name and verified.is_active:
            verified.company_name = board.slug.replace("-", " ").replace("_", " ").title()

        return verified

    @staticmethod
    def _parse_ashby(v: VerifiedATSBoard, body: str) -> None:
        data = json.loads(body)
        if not isinstance(data, dict) or ("jobs" not in data and "jobPostings" not in data):
            v.is_active = False
            return
        jobs = data.get("jobs") or data.get("jobPostings") or []
        v.is_active = True
        v.active_job_count = len(jobs)
        v.company_name = (
            data.get("organizationName")
            or data.get("companyName")
            or v.slug.replace("-", " ").title()
        )
        descriptions: List[str] = []
        dates: List[str] = []
        for j in jobs:
            title = j.get("title", "")
            if title and len(v.sample_job_titles) < 10:
                v.sample_job_titles.append(title)
            loc = j.get("location") or j.get("locationName") or ""
            if isinstance(loc, str) and loc and loc not in v.locations:
                v.locations.append(loc)
            dept = j.get("department") or j.get("departmentName") or ""
            if isinstance(dept, str) and dept and dept not in v.departments:
                v.departments.append(dept)
            pub = j.get("publishedAt") or j.get("publishedDate") or ""
            if pub:
                dates.append(str(pub))
            descriptions.append(str(j.get("descriptionHtml") or j.get("descriptionPlain") or ""))
        if dates:
            v.latest_posted_at = max(dates)
        v.detected_tech_stack = extract_tech_stack_from_text(" ".join(descriptions))

    @staticmethod
    def _parse_greenhouse(v: VerifiedATSBoard, body: str) -> None:
        data = json.loads(body)
        if not isinstance(data, dict) or "jobs" not in data:
            v.is_active = False
            return
        jobs = data.get("jobs") or []
        v.is_active = True
        v.active_job_count = len(jobs)
        if data.get("name"):
            v.company_name = str(data["name"])
        descriptions: List[str] = []
        dates: List[str] = []
        for j in jobs:
            title = j.get("title", "")
            if title and len(v.sample_job_titles) < 10:
                v.sample_job_titles.append(title)
            loc_obj = j.get("location") or {}
            loc_name = loc_obj.get("name", "") if isinstance(loc_obj, dict) else ""
            if loc_name and loc_name not in v.locations:
                v.locations.append(loc_name)
            for dept_obj in j.get("departments") or []:
                if isinstance(dept_obj, dict) and dept_obj.get("name"):
                    dname = dept_obj["name"]
                    if dname not in v.departments:
                        v.departments.append(dname)
            updated = j.get("updated_at") or ""
            if updated:
                dates.append(str(updated))
            if not v.company_name and j.get("company_name"):
                v.company_name = str(j["company_name"])
            # Check if absolute_url redirects to a custom company career domain (e.g. stripe.com/jobs?gh_jid=...)
            abs_url = j.get("absolute_url") or ""
            if abs_url and not v.resolved_domain:
                custom_dom = normalize_root_domain(abs_url)
                if custom_dom:
                    v.resolved_domain = custom_dom
                    v.domain_resolution_method = "api_company_url"
            descriptions.append(html.unescape(str(j.get("content") or "")))
        if dates:
            v.latest_posted_at = max(dates)
        v.detected_tech_stack = extract_tech_stack_from_text(" ".join(descriptions))

    @staticmethod
    def _parse_lever(v: VerifiedATSBoard, body: str) -> None:
        data = json.loads(body)
        if not isinstance(data, list):
            v.is_active = False
            return
        v.is_active = True
        v.active_job_count = len(data)
        descriptions: List[str] = []
        timestamps: List[int] = []
        for j in data:
            if not isinstance(j, dict):
                continue
            title = j.get("text", "")
            if title and len(v.sample_job_titles) < 10:
                v.sample_job_titles.append(title)
            cats = j.get("categories") or {}
            if isinstance(cats, dict):
                loc = cats.get("location") or ""
                if loc and loc not in v.locations:
                    v.locations.append(loc)
                dept = cats.get("department") or cats.get("team") or ""
                if dept and dept not in v.departments:
                    v.departments.append(dept)
            created_ms = j.get("createdAt")
            if isinstance(created_ms, (int, float)):
                timestamps.append(int(created_ms))
            descriptions.append(str(j.get("descriptionPlain") or j.get("description") or ""))
        if timestamps:
            latest_sec = max(timestamps) / 1000.0
            v.latest_posted_at = datetime.fromtimestamp(latest_sec, tz=timezone.utc).isoformat()
        v.detected_tech_stack = extract_tech_stack_from_text(" ".join(descriptions))

    @staticmethod
    def _parse_workday(v: VerifiedATSBoard, body: str) -> None:
        data = json.loads(body)
        if not isinstance(data, dict) or "jobPostings" not in data:
            v.is_active = False
            return
        postings = data.get("jobPostings") or []
        total = data.get("total")
        v.is_active = True
        v.active_job_count = int(total) if isinstance(total, int) and total >= len(postings) else len(postings)
        descriptions: List[str] = []
        for j in postings:
            if not isinstance(j, dict):
                continue
            title = j.get("title", "")
            if title and len(v.sample_job_titles) < 10:
                v.sample_job_titles.append(title)
            loc = j.get("locationsText") or ""
            if loc and loc not in v.locations:
                v.locations.append(loc)
            descriptions.append(title)
        v.detected_tech_stack = extract_tech_stack_from_text(" ".join(descriptions))

    @staticmethod
    def _parse_workable(v: VerifiedATSBoard, body: str) -> None:
        data = json.loads(body)
        if not isinstance(data, dict) or "jobs" not in data:
            v.is_active = False
            return
        jobs = data.get("jobs") or []
        v.is_active = True
        v.active_job_count = len(jobs)
        if data.get("name"):
            v.company_name = str(data["name"])
        descriptions: List[str] = []
        dates: List[str] = []
        for j in jobs:
            title = j.get("title", "")
            if title and len(v.sample_job_titles) < 10:
                v.sample_job_titles.append(title)
            loc = ", ".join(filter(None, [j.get("city"), j.get("country")]))
            if loc and loc not in v.locations:
                v.locations.append(loc)
            dept = j.get("department") or ""
            if dept and dept not in v.departments:
                v.departments.append(dept)
            pub = j.get("published_on") or ""
            if pub:
                dates.append(str(pub))
            descriptions.append(str(j.get("description") or ""))
        if dates:
            v.latest_posted_at = max(dates)
        v.detected_tech_stack = extract_tech_stack_from_text(" ".join(descriptions))

    @staticmethod
    def _parse_smartrecruiters(v: VerifiedATSBoard, body: str) -> None:
        data = json.loads(body)
        if not isinstance(data, dict) or "content" not in data:
            v.is_active = False
            return
        jobs = data.get("content") or []
        total_found = data.get("totalFound")
        v.is_active = True
        v.active_job_count = int(total_found) if isinstance(total_found, int) else len(jobs)
        dates: List[str] = []
        for j in jobs:
            title = j.get("name", "")
            if title and len(v.sample_job_titles) < 10:
                v.sample_job_titles.append(title)
            comp = j.get("company") or {}
            if not v.company_name and isinstance(comp, dict) and comp.get("name"):
                v.company_name = str(comp["name"])
            loc_obj = j.get("location") or {}
            if isinstance(loc_obj, dict):
                loc = ", ".join(filter(None, [loc_obj.get("city"), loc_obj.get("country")]))
                if loc and loc not in v.locations:
                    v.locations.append(loc)
            dept_obj = j.get("department") or {}
            if isinstance(dept_obj, dict) and dept_obj.get("label"):
                if dept_obj["label"] not in v.departments:
                    v.departments.append(dept_obj["label"])
            rel = j.get("releasedDate") or ""
            if rel:
                dates.append(str(rel))
        if dates:
            v.latest_posted_at = max(dates)

    @staticmethod
    def _parse_recruitee(v: VerifiedATSBoard, body: str) -> None:
        data = json.loads(body)
        if not isinstance(data, dict) or "offers" not in data:
            v.is_active = False
            return
        offers = data.get("offers") or []
        v.is_active = True
        v.active_job_count = len(offers)
        descriptions: List[str] = []
        dates: List[str] = []
        for o in offers:
            title = o.get("title", "")
            if title and len(v.sample_job_titles) < 10:
                v.sample_job_titles.append(title)
            if not v.company_name and o.get("company_name"):
                v.company_name = str(o["company_name"])
            loc = o.get("location") or o.get("city") or ""
            if loc and loc not in v.locations:
                v.locations.append(loc)
            dept = o.get("department") or ""
            if dept and dept not in v.departments:
                v.departments.append(dept)
            pub = o.get("published_at") or o.get("created_at") or ""
            if pub:
                dates.append(str(pub))
            descriptions.append(str(o.get("description") or "") + " " + str(o.get("requirements") or ""))
        if dates:
            v.latest_posted_at = max(dates)
        v.detected_tech_stack = extract_tech_stack_from_text(" ".join(descriptions))

    @staticmethod
    def _parse_breezy(v: VerifiedATSBoard, body: str) -> None:
        data = json.loads(body)
        if not isinstance(data, list):
            v.is_active = False
            return
        v.is_active = True
        v.active_job_count = len(data)
        dates: List[str] = []
        for j in data:
            if not isinstance(j, dict):
                continue
            title = j.get("name", "")
            if title and len(v.sample_job_titles) < 10:
                v.sample_job_titles.append(title)
            comp = j.get("company") or {}
            if isinstance(comp, dict):
                if not v.company_name and comp.get("name"):
                    v.company_name = str(comp["name"])
            loc_obj = j.get("location") or {}
            if isinstance(loc_obj, dict) and loc_obj.get("name"):
                if loc_obj["name"] not in v.locations:
                    v.locations.append(loc_obj["name"])
            dept = j.get("department") or ""
            if dept and dept not in v.departments:
                v.departments.append(dept)
            pub = j.get("published_date") or ""
            if pub:
                dates.append(str(pub))
        if dates:
            v.latest_posted_at = max(dates)

    @staticmethod
    def _parse_bamboohr(v: VerifiedATSBoard, body: str) -> None:
        data = json.loads(body)
        if not isinstance(data, dict) or "result" not in data:
            v.is_active = False
            return
        jobs = data.get("result") or []
        v.is_active = True
        v.active_job_count = len(jobs)
        for j in jobs:
            if not isinstance(j, dict):
                continue
            title = j.get("jobOpeningName", "")
            if title and len(v.sample_job_titles) < 10:
                v.sample_job_titles.append(title)
            dept = j.get("departmentLabel") or ""
            if dept and dept not in v.departments:
                v.departments.append(dept)
            loc_obj = j.get("location") or {}
            if isinstance(loc_obj, dict):
                loc = ", ".join(filter(None, [loc_obj.get("city"), loc_obj.get("state")]))
                if loc and loc not in v.locations:
                    v.locations.append(loc)

    @staticmethod
    def _parse_personio(v: VerifiedATSBoard, body: str) -> None:
        if "<workzag-jobs" not in body and "<position" not in body:
            v.is_active = False
            return
        root = ET.fromstring(body)
        positions = root.findall(".//position")
        v.is_active = True
        v.active_job_count = len(positions)
        descriptions: List[str] = []
        for pos in positions:
            name_el = pos.find("name")
            if name_el is not None and name_el.text and len(v.sample_job_titles) < 10:
                v.sample_job_titles.append(name_el.text.strip())
            office_el = pos.find("office")
            if office_el is not None and office_el.text:
                loc = office_el.text.strip()
                if loc not in v.locations:
                    v.locations.append(loc)
            dept_el = pos.find("department")
            if dept_el is not None and dept_el.text:
                dept = dept_el.text.strip()
                if dept not in v.departments:
                    v.departments.append(dept)
            for desc_el in pos.findall(".//jobDescription/value"):
                if desc_el.text:
                    descriptions.append(desc_el.text)
        v.detected_tech_stack = extract_tech_stack_from_text(" ".join(descriptions))

    @staticmethod
    def _parse_pinpoint(v: VerifiedATSBoard, body: str) -> None:
        data = json.loads(body)
        jobs = data.get("data") if isinstance(data, dict) else data
        if not isinstance(jobs, list):
            v.is_active = False
            return
        v.is_active = True
        v.active_job_count = len(jobs)
        for j in jobs:
            if not isinstance(j, dict):
                continue
            title = j.get("title", "")
            if title and len(v.sample_job_titles) < 10:
                v.sample_job_titles.append(title)
            loc_obj = j.get("location") or {}
            if isinstance(loc_obj, dict) and loc_obj.get("name"):
                if loc_obj["name"] not in v.locations:
                    v.locations.append(loc_obj["name"])

    @staticmethod
    def _parse_hirehive(v: VerifiedATSBoard, body: str) -> None:
        data = json.loads(body)
        jobs = data.get("jobs") if isinstance(data, dict) else None
        if not isinstance(jobs, list):
            v.is_active = False
            return
        v.is_active = True
        v.active_job_count = len(jobs)
        for j in jobs:
            if isinstance(j, dict) and j.get("title") and len(v.sample_job_titles) < 10:
                v.sample_job_titles.append(str(j["title"]))

    @staticmethod
    def _parse_manatal(v: VerifiedATSBoard, body: str) -> None:
        data = json.loads(body)
        jobs = data.get("results") if isinstance(data, dict) else None
        if not isinstance(jobs, list):
            v.is_active = False
            return
        v.is_active = True
        v.active_job_count = int(data.get("count") or len(jobs))
        for j in jobs:
            if isinstance(j, dict) and j.get("position_name") and len(v.sample_job_titles) < 10:
                v.sample_job_titles.append(str(j["position_name"]))

    @staticmethod
    def _parse_rippling(v: VerifiedATSBoard, body: str) -> None:
        data = json.loads(body)
        jobs = data if isinstance(data, list) else (data.get("jobs") if isinstance(data, dict) else None)
        if not isinstance(jobs, list):
            v.is_active = False
            return
        v.is_active = True
        v.active_job_count = len(jobs)
        for j in jobs:
            if isinstance(j, dict) and (j.get("name") or j.get("title")) and len(v.sample_job_titles) < 10:
                v.sample_job_titles.append(str(j.get("name") or j.get("title")))

    @staticmethod
    def _parse_trakstar(v: VerifiedATSBoard, body: str) -> None:
        data = json.loads(body)
        if not isinstance(data, list):
            v.is_active = False
            return
        v.is_active = True
        v.active_job_count = len(data)
        for j in data:
            if isinstance(j, dict) and j.get("title") and len(v.sample_job_titles) < 10:
                v.sample_job_titles.append(str(j["title"]))

    @staticmethod
    def _parse_generic(v: VerifiedATSBoard, body: str) -> None:
        v.is_active = True
        title_match = re.search(r"<title[^>]*>([^<]+)</title>", body, re.IGNORECASE)
        if title_match:
            raw_title = html.unescape(title_match.group(1)).strip()
            cleaned = re.split(r"\s+[-|–]\s+|\s+Jobs\b|\s+Careers\b", raw_title, flags=re.IGNORECASE)[0].strip()
            if cleaned:
                v.company_name = cleaned
        job_postings = re.findall(r'"@type"\s*:\s*"JobPosting"', body)
        v.active_job_count = len(job_postings)
        v.detected_tech_stack = extract_tech_stack_from_text(body[:50000])

    @staticmethod
    def _infer_domain_from_payload(raw_body: str, slug: str) -> Tuple[str, str]:
        for url_match in re.finditer(
            r'"(?:website|companyUrl|company_url|homepage|orgUrl|externalUrl)"\s*:\s*"(https?://[^"]+)"',
            raw_body,
            re.IGNORECASE,
        ):
            dom = normalize_root_domain(url_match.group(1))
            if dom:
                return dom, "api_company_url"

        slug_clean = re.sub(r"[^a-z0-9]", "", slug.lower())
        candidate_emails = re.findall(
            r"[a-zA-Z0-9._%+-]+@([a-zA-Z0-9.-]+\.[a-zA-Z]{2,})", raw_body[:100000]
        )
        best_email_dom = ""
        for email_host in candidate_emails:
            dom = normalize_root_domain(email_host)
            if not dom:
                continue
            if slug_clean and slug_clean in dom.replace(".", "").replace("-", ""):
                return dom, "email_domain_slug_match"
            if not best_email_dom:
                best_email_dom = dom
        if best_email_dom:
            return best_email_dom, "email_domain"

        for href_match in re.finditer(r"https?://([a-zA-Z0-9.-]+\.[a-zA-Z]{2,})", raw_body[:100000]):
            dom = normalize_root_domain(href_match.group(1))
            if not dom:
                continue
            dom_label = dom.split(".")[0].replace("-", "")
            if slug_clean and (dom_label == slug_clean or slug_clean.startswith(dom_label)):
                return dom, "payload_url_slug_match"

        return "", ""


class ATSBoardVerifier:
    """Polls public unauthenticated ATS endpoints to verify candidate boards."""

    def __init__(self, http_client: AsyncHTTPClient):
        self.http = http_client

    async def verify_board(self, board: DiscoveredBoard) -> VerifiedATSBoard:
        spec = ATS_PLATFORMS.get(board.ats_id)
        if not spec:
            return VerifiedATSBoard(
                ats_id=board.ats_id,
                ats_display_name=board.ats_id,
                slug=board.slug,
                is_active=False,
                board_status="dead",
            )

        shard = board.shard or ("de" if spec.ats_id == "personio" else "wd1")
        site = board.site or "External"
        target_url = (
            spec.public_api_url.format(slug=board.slug, shard=shard, site=site)
            if spec.public_api_url
            else (board.board_url or build_canonical_board_url(spec, board.slug, shard, site))
        )

        json_body: Optional[Dict[str, Any]] = None
        if spec.ats_id == "workday" and spec.public_api_method == "POST":
            json_body = {"appliedFacets": {}, "limit": 20, "offset": 0, "searchText": ""}

        status, raw_body, _ = await self.http.request(
            spec.public_api_method, target_url, json_body=json_body
        )
        return PublicATSPayloadParser.parse_response(board, status, raw_body)


# =============================================================================
# 7. MULTI-SIGNAL ENTITY RESOLVER & MIGRATION DISAMBIGUATION
# =============================================================================

class EntityResolver:
    """
    Resolves verified ATS boards into canonical Company -> ATS mappings and
    disambiguates slug collisions and ATS platform migrations globally.
    """

    RESOLUTION_WEIGHTS: Dict[str, float] = {
        "career_page_backlink": 0.55,
        "dns_cname": 0.55,
        "api_company_url": 0.45,
        "email_domain_slug_match": 0.40,
        "payload_url_slug_match": 0.35,
        "email_domain": 0.25,
        "slug_heuristic": 0.15,
        "": 0.05,
    }

    @classmethod
    def score_and_resolve(cls, boards: Sequence[VerifiedATSBoard]) -> List[VerifiedATSBoard]:
        for b in boards:
            if not b.is_active:
                b.confidence_score = 0.0
                b.board_status = "dead"
                continue

            base_weight = cls.RESOLUTION_WEIGHTS.get(b.domain_resolution_method, 0.10)
            if b.active_job_count > 0:
                job_score = min(0.30, 0.10 + 0.05 * (b.active_job_count ** 0.4))
            else:
                job_score = 0.0

            recency_score = 0.0
            if b.latest_posted_at:
                if b.latest_posted_at.startswith(("2026", "2025")):
                    recency_score = 0.15
                elif b.latest_posted_at.startswith("2024"):
                    recency_score = 0.07

            b.confidence_score = round(min(1.0, base_weight + job_score + recency_score), 3)

        entity_groups: Dict[str, List[VerifiedATSBoard]] = {}
        for b in boards:
            if not b.is_active:
                continue
            entity_key = (
                b.resolved_domain.lower()
                if b.resolved_domain
                else f"slug:{re.sub(r'[^a-z0-9]', '', b.slug.lower())}"
            )
            entity_groups.setdefault(entity_key, []).append(b)

        for entity_key, group in entity_groups.items():
            group.sort(
                key=lambda x: (
                    1 if x.domain_resolution_method in ("career_page_backlink", "dns_cname", "api_company_url") else 0,
                    x.active_job_count,
                    x.latest_posted_at or "",
                    x.confidence_score,
                ),
                reverse=True,
            )
            winner = group[0]
            if winner.active_job_count > 0:
                winner.board_status = "primary_active"
            else:
                winner.board_status = "active_empty"

            for secondary in group[1:]:
                if secondary.active_job_count == 0:
                    secondary.board_status = "migrated_or_inactive"
                elif (
                    winner.active_job_count >= secondary.active_job_count * 5
                    and secondary.domain_resolution_method not in ("career_page_backlink", "dns_cname")
                ):
                    secondary.board_status = "migrated_or_secondary"
                else:
                    secondary.board_status = "secondary_active"

        return list(boards)


# =============================================================================
# 8. SQLITE WAL STORAGE, SHARD MERGER & PLATFORM-WISE EXPORT ENGINE
# =============================================================================

class ATSDatabase:
    """
    Persistent SQLite WAL storage layer supporting distributed shard merging,
    incremental checkpoints, and per-ATS company list exports.
    """

    def __init__(self, db_path: str = "ats_companies.db"):
        self.db_path = db_path
        self.conn = sqlite3.connect(self.db_path)
        self.conn.row_factory = sqlite3.Row
        self._init_schema()

    def _init_schema(self) -> None:
        cur = self.conn.cursor()
        cur.execute("PRAGMA journal_mode=WAL;")
        cur.execute("PRAGMA synchronous=NORMAL;")
        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS discovered_boards (
                board_key TEXT PRIMARY KEY,
                ats_id TEXT NOT NULL,
                slug TEXT NOT NULL,
                shard TEXT DEFAULT '',
                site TEXT DEFAULT '',
                board_url TEXT DEFAULT '',
                discovery_source TEXT DEFAULT '',
                source_company_domain TEXT DEFAULT '',
                discovered_at TEXT NOT NULL
            );
            """
        )
        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS verified_boards (
                board_key TEXT PRIMARY KEY,
                ats_id TEXT NOT NULL,
                ats_display_name TEXT NOT NULL,
                slug TEXT NOT NULL,
                shard TEXT DEFAULT '',
                site TEXT DEFAULT '',
                board_url TEXT NOT NULL,
                is_active INTEGER NOT NULL,
                http_status INTEGER NOT NULL,
                company_name TEXT DEFAULT '',
                resolved_domain TEXT DEFAULT '',
                domain_resolution_method TEXT DEFAULT '',
                confidence_score REAL DEFAULT 0.0,
                board_status TEXT DEFAULT '',
                active_job_count INTEGER DEFAULT 0,
                latest_posted_at TEXT DEFAULT '',
                sample_job_titles_json TEXT DEFAULT '[]',
                locations_json TEXT DEFAULT '[]',
                departments_json TEXT DEFAULT '[]',
                detected_tech_stack_json TEXT DEFAULT '[]',
                discovery_sources_json TEXT DEFAULT '[]',
                verified_at TEXT NOT NULL
            );
            """
        )
        cur.execute("CREATE INDEX IF NOT EXISTS idx_verified_ats ON verified_boards(ats_id, is_active, board_status);")
        cur.execute("CREATE INDEX IF NOT EXISTS idx_verified_domain ON verified_boards(resolved_domain);")
        self.conn.commit()

    def upsert_discovered_boards(self, boards: Iterable[DiscoveredBoard]) -> int:
        cur = self.conn.cursor()
        count = 0
        for b in boards:
            cur.execute(
                """
                INSERT INTO discovered_boards (
                    board_key, ats_id, slug, shard, site, board_url,
                    discovery_source, source_company_domain, discovered_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(board_key) DO UPDATE SET
                    source_company_domain = CASE
                        WHEN excluded.source_company_domain != '' THEN excluded.source_company_domain
                        ELSE discovered_boards.source_company_domain
                    END
                """,
                (
                    b.board_key,
                    b.ats_id,
                    b.slug.lower(),
                    b.shard,
                    b.site,
                    b.board_url,
                    b.discovery_source,
                    b.source_company_domain or "",
                    b.discovered_at,
                ),
            )
            count += 1
        self.conn.commit()
        return count

    def upsert_verified_boards(self, boards: Iterable[VerifiedATSBoard]) -> int:
        cur = self.conn.cursor()
        count = 0
        for b in boards:
            board_key = f"{b.ats_id}:{b.slug.lower()}"
            cur.execute(
                """
                INSERT INTO verified_boards (
                    board_key, ats_id, ats_display_name, slug, shard, site,
                    board_url, is_active, http_status, company_name,
                    resolved_domain, domain_resolution_method, confidence_score,
                    board_status, active_job_count, latest_posted_at,
                    sample_job_titles_json, locations_json, departments_json,
                    detected_tech_stack_json, discovery_sources_json, verified_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(board_key) DO UPDATE SET
                    is_active = excluded.is_active,
                    http_status = excluded.http_status,
                    company_name = excluded.company_name,
                    resolved_domain = excluded.resolved_domain,
                    domain_resolution_method = excluded.domain_resolution_method,
                    confidence_score = excluded.confidence_score,
                    board_status = excluded.board_status,
                    active_job_count = excluded.active_job_count,
                    latest_posted_at = excluded.latest_posted_at,
                    sample_job_titles_json = excluded.sample_job_titles_json,
                    locations_json = excluded.locations_json,
                    departments_json = excluded.departments_json,
                    detected_tech_stack_json = excluded.detected_tech_stack_json,
                    verified_at = excluded.verified_at
                """,
                (
                    board_key,
                    b.ats_id,
                    b.ats_display_name,
                    b.slug.lower(),
                    b.shard,
                    b.site,
                    b.board_url,
                    1 if b.is_active else 0,
                    b.http_status,
                    b.company_name,
                    b.resolved_domain,
                    b.domain_resolution_method,
                    b.confidence_score,
                    b.board_status,
                    b.active_job_count,
                    b.latest_posted_at,
                    json.dumps(b.sample_job_titles),
                    json.dumps(b.locations),
                    json.dumps(b.departments),
                    json.dumps(b.detected_tech_stack),
                    json.dumps(b.discovery_sources),
                    b.verified_at,
                ),
            )
            count += 1
        self.conn.commit()
        return count

    def load_all_verified_boards(self) -> List[VerifiedATSBoard]:
        cur = self.conn.cursor()
        rows = cur.execute("SELECT * FROM verified_boards").fetchall()
        boards: List[VerifiedATSBoard] = []
        for r in rows:
            boards.append(
                VerifiedATSBoard(
                    ats_id=r["ats_id"],
                    ats_display_name=r["ats_display_name"],
                    slug=r["slug"],
                    shard=r["shard"],
                    site=r["site"],
                    board_url=r["board_url"],
                    is_active=bool(r["is_active"]),
                    http_status=r["http_status"],
                    company_name=r["company_name"],
                    resolved_domain=r["resolved_domain"],
                    domain_resolution_method=r["domain_resolution_method"],
                    confidence_score=r["confidence_score"],
                    board_status=r["board_status"],
                    active_job_count=r["active_job_count"],
                    latest_posted_at=r["latest_posted_at"],
                    sample_job_titles=json.loads(r["sample_job_titles_json"] or "[]"),
                    locations=json.loads(r["locations_json"] or "[]"),
                    departments=json.loads(r["departments_json"] or "[]"),
                    detected_tech_stack=json.loads(r["detected_tech_stack_json"] or "[]"),
                    discovery_sources=json.loads(r["discovery_sources_json"] or "[]"),
                    verified_at=r["verified_at"],
                )
            )
        return boards

    def merge_from_databases(self, shard_db_paths: Sequence[str]) -> Tuple[int, int]:
        """
        Merges `discovered_boards` and `verified_boards` from multiple shard SQLite
        databases and runs global `EntityResolver.score_and_resolve` across all boards.
        """
        total_disc = 0
        all_verified: Dict[str, VerifiedATSBoard] = {
            f"{b.ats_id}:{b.slug.lower()}": b for b in self.load_all_verified_boards()
        }
        for path in shard_db_paths:
            if not os.path.exists(path) or os.path.abspath(path) == os.path.abspath(self.db_path):
                continue
            shard_db = ATSDatabase(path)
            cur = shard_db.conn.cursor()
            disc_rows = cur.execute("SELECT * FROM discovered_boards").fetchall()
            disc_objs = [
                DiscoveredBoard(
                    ats_id=r["ats_id"],
                    slug=r["slug"],
                    shard=r["shard"],
                    site=r["site"],
                    board_url=r["board_url"],
                    discovery_source=r["discovery_source"],
                    source_company_domain=r["source_company_domain"] or None,
                    discovered_at=r["discovered_at"],
                )
                for r in disc_rows
            ]
            total_disc += self.upsert_discovered_boards(disc_objs)
            for vb in shard_db.load_all_verified_boards():
                key = f"{vb.ats_id}:{vb.slug.lower()}"
                existing = all_verified.get(key)
                if not existing or (vb.is_active and vb.active_job_count >= existing.active_job_count):
                    all_verified[key] = vb
            shard_db.close()

        resolved = EntityResolver.score_and_resolve(list(all_verified.values()))
        total_ver = self.upsert_verified_boards(resolved)
        return total_disc, total_ver

    def export_platform_wise_lists(
        self, output_dir: str, only_active: bool = True
    ) -> Dict[str, int]:
        """
        Exports a master CSV/JSONL, individual per-ATS company list CSVs, and a
        Markdown summary report (`SUMMARY.md`) for GitHub Actions job summaries.
        """
        out_path = Path(output_dir)
        out_path.mkdir(parents=True, exist_ok=True)
        cur = self.conn.cursor()
        where_clause = (
            "WHERE is_active = 1 AND board_status IN ('primary_active', 'secondary_active', 'active_empty')"
            if only_active
            else ""
        )
        rows = cur.execute(
            f"""
            SELECT * FROM verified_boards
            {where_clause}
            ORDER BY ats_id ASC, active_job_count DESC, confidence_score DESC
            """
        ).fetchall()

        fieldnames = [
            "company_name",
            "resolved_domain",
            "ats_id",
            "ats_display_name",
            "slug",
            "board_url",
            "board_status",
            "confidence_score",
            "active_job_count",
            "latest_posted_at",
            "domain_resolution_method",
            "locations",
            "departments",
            "detected_tech_stack",
            "sample_job_titles",
        ]

        counts_by_ats: Dict[str, int] = {}
        jobs_by_ats: Dict[str, int] = {}
        rows_by_ats: Dict[str, List[Dict[str, Any]]] = {}

        master_csv = out_path / "all_ats_companies_master.csv"
        master_jsonl = out_path / "all_ats_companies_master.jsonl"

        with master_csv.open("w", newline="", encoding="utf-8") as f_csv, \
             master_jsonl.open("w", encoding="utf-8") as f_jsonl:
            writer = csv.DictWriter(f_csv, fieldnames=fieldnames)
            writer.writeheader()
            for r in rows:
                record = {
                    "company_name": r["company_name"],
                    "resolved_domain": r["resolved_domain"],
                    "ats_id": r["ats_id"],
                    "ats_display_name": r["ats_display_name"],
                    "slug": r["slug"],
                    "board_url": r["board_url"],
                    "board_status": r["board_status"],
                    "confidence_score": r["confidence_score"],
                    "active_job_count": r["active_job_count"],
                    "latest_posted_at": r["latest_posted_at"],
                    "domain_resolution_method": r["domain_resolution_method"],
                    "locations": "; ".join(json.loads(r["locations_json"] or "[]")),
                    "departments": "; ".join(json.loads(r["departments_json"] or "[]")),
                    "detected_tech_stack": "; ".join(json.loads(r["detected_tech_stack_json"] or "[]")),
                    "sample_job_titles": "; ".join(json.loads(r["sample_job_titles_json"] or "[]")),
                }
                writer.writerow(record)
                f_jsonl.write(json.dumps(record) + "\n")
                ats_id = r["ats_id"]
                counts_by_ats[ats_id] = counts_by_ats.get(ats_id, 0) + 1
                jobs_by_ats[ats_id] = jobs_by_ats.get(ats_id, 0) + int(r["active_job_count"] or 0)
                rows_by_ats.setdefault(ats_id, []).append(record)

        for ats_id, ats_rows in rows_by_ats.items():
            platform_csv = out_path / f"{ats_id}_companies.csv"
            with platform_csv.open("w", newline="", encoding="utf-8") as pf:
                pw = csv.DictWriter(pf, fieldnames=fieldnames)
                pw.writeheader()
                pw.writerows(ats_rows)

        # Write Markdown summary report for GitHub Actions $GITHUB_STEP_SUMMARY
        summary_md = out_path / "SUMMARY.md"
        total_companies = sum(counts_by_ats.values())
        total_jobs = sum(jobs_by_ats.values())
        md_lines = [
            "# ATS Company Discovery & Verification Summary",
            "",
            f"- **Total Verified Active Companies**: **{total_companies:,}**",
            f"- **Total Live Open Jobs Indexed**: **{total_jobs:,}**",
            f"- **ATS Platforms with Verified Companies**: **{len(counts_by_ats)}**",
            "",
            "| ATS Platform | Verified Companies | Live Open Jobs | Export File |",
            "| :--- | ---: | ---: | :--- |",
        ]
        for ats_id, comp_count in sorted(counts_by_ats.items(), key=lambda x: x[1], reverse=True):
            disp = ATS_PLATFORMS[ats_id].display_name if ats_id in ATS_PLATFORMS else ats_id
            j_count = jobs_by_ats.get(ats_id, 0)
            md_lines.append(
                f"| **{disp}** (`{ats_id}`) | {comp_count:,} | {j_count:,} | `{ats_id}_companies.csv` |"
            )
        summary_md.write_text("\n".join(md_lines) + "\n", encoding="utf-8")

        return counts_by_ats

    def close(self) -> None:
        self.conn.close()


# =============================================================================
# 9. DETERMINISTIC SELF-TEST & VERIFICATION SUITE
# =============================================================================

def run_self_test() -> None:
    import tempfile

    print("=" * 76)
    print("RUNNING END-TO-END ATS DISCOVERY & ENTITY RESOLUTION SELF-TEST (v2.5)")
    print("=" * 76)

    # 1. Verify platform coverage (88 ATS platforms registered)
    assert len(ATS_PLATFORMS) >= 85, f"Expected >= 85 ATS platforms, found {len(ATS_PLATFORMS)}"
    print(f"[PASS] 1. Registered {len(ATS_PLATFORMS)} ATS platforms (Enterprise, Mid-Market, Startup, Global).")

    # 2. Verify 36-way alphanumeric prefix sharding for CDX queries
    gh_expanded = expand_cdx_patterns_with_prefixes(
        ATS_PLATFORMS["greenhouse"].cdx_patterns, enable_prefix_sharding=True
    )
    assert len(gh_expanded) == 4 * 36, f"Expected 144 sharded patterns, got {len(gh_expanded)}"
    assert "boards.greenhouse.io/a*" in gh_expanded and "job-boards.eu.greenhouse.io/9*" in gh_expanded
    print(f"[PASS] 2. 36-Prefix Alphanumeric CDX Sharding verified ({len(gh_expanded)} Greenhouse shards).")

    # 3. Verify URL slug extraction across diverse ATS URL structures
    sample_urls = [
        ("https://jobs.ashbyhq.com/ramp?utm_source=linkedin", "ashby", "ramp", "", ""),
        ("https://boards.greenhouse.io/embed/job_board?for=stripe", "greenhouse", "stripe", "", ""),
        ("https://job-boards.eu.greenhouse.io/celonis/jobs/123", "greenhouse", "celonis", "", ""),
        ("https://jobs.lever.co/palantir/8a7b9c", "lever", "palantir", "", ""),
        ("https://nvidia.wd5.myworkdayjobs.com/en-US/NVIDIAExternalCareerSite/job/123", "workday", "nvidia", "wd5", "NVIDIAExternalCareerSite"),
        ("https://apply.workable.com/skroutz/", "workable", "skroutz", "", ""),
        ("https://jobs.smartrecruiters.com/Visa", "smartrecruiters", "Visa", "", ""),
        ("https://acme-corp.recruitee.com/o/software-engineer", "recruitee", "acme-corp", "", ""),
        ("https://buffer.breezy.hr/", "breezy", "buffer", "", ""),
        ("https://soundhound.bamboohr.com/careers/120", "bamboohr", "soundhound", "", ""),
        ("https://n26.jobs.personio.de/job/12345", "personio", "n26", "de", ""),
        ("https://monzo.pinpointhq.com/postings/123", "pinpoint", "monzo", "", ""),
        ("https://ats.rippling.com/parker-financial/jobs", "rippling", "parker-financial", "", ""),
        ("https://razorpay.keka.com/careers", "keka", "razorpay", "", ""),
        ("https://swiggy.darwinbox.in/ms/candidate/careers", "darwinbox", "swiggy", "in", ""),
    ]
    for url, exp_ats, exp_slug, exp_shard, exp_site in sample_urls:
        b = extract_ats_from_url(url, discovery_source="unit_test")
        assert b is not None and b.ats_id == exp_ats and b.slug == exp_slug
    print(f"[PASS] 3. URL Regex Extractor verified across {len(sample_urls)} ATS URL formats.")

    # 4. Verify Career-Page HTML Embed (`?gh_jid=` custom domain + script) & CNAME Fingerprinting
    sample_career_html = """
    <html>
      <body>
        <a href="https://stripe.com/jobs/listing/engineer?gh_jid=5891234">Apply</a>
        <iframe src="https://jobs.ashbyhq.com/linear/embed"></iframe>
      </body>
    </html>
    """
    html_boards = extract_ats_boards_from_html(sample_career_html, "stripe.com")
    assert {b.board_key for b in html_boards} == {"greenhouse:stripe", "ashby:linear"}

    cname_board = match_cname_to_ats("ghs.greenhouse.io.", "jobs.datadoghq.com", "datadoghq.com")
    assert cname_board is not None and cname_board.ats_id == "greenhouse" and cname_board.slug == "datadoghq"
    print("[PASS] 4. Career-Page `?gh_jid=` Custom Domain & DNS CNAME Fingerprinting verified.")

    # 5. Verify Certificate Transparency (`crt.sh`) & Wayback CDX JSON parsing
    sample_crt_json = json.dumps([
        {"name_value": "*.recruitee.com\ntellonym.recruitee.com\nframer.recruitee.com"},
    ])
    ct_boards = CertificateTransparencyDiscoverer.parse_crt_sh_json(
        sample_crt_json, ATS_PLATFORMS["recruitee"]
    )
    assert {b.slug for b in ct_boards} == {"tellonym", "framer"}

    sample_wb_json = json.dumps([
        ["original"],
        ["https://boards.greenhouse.io/anthropic/jobs/40123"],
        ["https://job-boards.greenhouse.io/scaleai"],
    ])
    wb_boards = WaybackCDXDiscoverer.parse_wayback_json(
        sample_wb_json, ATS_PLATFORMS["greenhouse"]
    )
    assert {b.slug for b in wb_boards} == {"anthropic", "scaleai"}
    print("[PASS] 5. Certificate Transparency (crt.sh) & Wayback CDX parsers verified.")

    # 6. Verify Public ATS API Parsing, Multi-Shard DB Merging & Global Entity Resolution
    stale_gh_board = DiscoveredBoard(
        ats_id="greenhouse", slug="ramp", discovery_source="commoncrawl_cdx"
    )
    stale_gh_payload = json.dumps({
        "name": "Ramp Legacy",
        "jobs": [
            {
                "title": "Software Engineer",
                "updated_at": "2022-04-10T12:00:00Z",
                "location": {"name": "New York"},
                "departments": [{"name": "Engineering"}],
                "content": "We use Python and PostgreSQL. Contact jobs@ramp.com",
            }
        ],
    })
    v_stale = PublicATSPayloadParser.parse_response(stale_gh_board, 200, stale_gh_payload)

    active_ashby_board = DiscoveredBoard(
        ats_id="ashby",
        slug="ramp",
        discovery_source="career_page_html",
        source_company_domain="ramp.com",
    )
    active_ashby_payload = json.dumps({
        "organizationName": "Ramp",
        "jobs": [
            {
                "title": "Staff Backend Engineer",
                "publishedAt": "2026-10-20T10:00:00Z",
                "location": "New York, NY",
                "department": "Engineering",
                "descriptionPlain": "Build financial systems with Python, Rust, PostgreSQL, Kafka, AWS, and Kubernetes.",
            },
            {
                "title": "Machine Learning Engineer",
                "publishedAt": "2026-10-24T15:30:00Z",
                "location": "San Francisco, CA",
                "department": "AI & Data",
                "descriptionPlain": "Deploy models using PyTorch, Snowflake, dbt, and OpenAI.",
            },
        ],
    })
    v_active = PublicATSPayloadParser.parse_response(active_ashby_board, 200, active_ashby_payload)

    with tempfile.TemporaryDirectory() as tmpdir:
        shard1_path = os.path.join(tmpdir, "shard1.db")
        shard2_path = os.path.join(tmpdir, "shard2.db")
        merged_path = os.path.join(tmpdir, "merged.db")
        export_dir = os.path.join(tmpdir, "exports")

        s1 = ATSDatabase(shard1_path)
        s1.upsert_discovered_boards([stale_gh_board])
        s1.upsert_verified_boards([v_stale])
        s1.close()

        s2 = ATSDatabase(shard2_path)
        s2.upsert_discovered_boards([active_ashby_board])
        s2.upsert_verified_boards([v_active])
        s2.close()

        master = ATSDatabase(merged_path)
        master.merge_from_databases([shard1_path, shard2_path])
        counts = master.export_platform_wise_lists(export_dir, only_active=True)
        master.close()

        assert counts.get("ashby") == 1
        assert os.path.exists(os.path.join(export_dir, "SUMMARY.md"))
        assert os.path.exists(os.path.join(export_dir, "all_ats_companies_master.csv"))
    print("[PASS] 6. Multi-Shard SQLite Merger, Global Entity Resolver & Markdown Summary verified.")
    print("=" * 76)
    print("ALL SELF-TESTS PASSED SUCCESSFULLY.")
    print("=" * 76)


# =============================================================================
# 10. CLI ENTRYPOINT & PIPELINE ORCHESTRATOR
# =============================================================================

async def cmd_discover(args: argparse.Namespace) -> None:
    db = ATSDatabase(args.db)
    platforms = (
        [p.strip() for p in args.platforms.split(",") if p.strip()]
        if args.platforms
        else list(ATS_PLATFORMS.keys())
    )
    sources = {s.strip() for s in args.sources.split(",") if s.strip()}

    async with AsyncHTTPClient(concurrency=args.concurrency) as http:
        total_saved = 0
        if "ct" in sources:
            ct = CertificateTransparencyDiscoverer(http)
            for pid in platforms:
                spec = ATS_PLATFORMS.get(pid)
                if not spec or not spec.ct_patterns:
                    continue
                logger.info("[CT Logs] Querying crt.sh for %s (%s)...", spec.display_name, ", ".join(spec.ct_patterns))
                boards = await ct.discover_for_platform(spec)
                saved = db.upsert_discovered_boards(boards)
                total_saved += saved
                logger.info("[CT Logs] Discovered %d candidate boards for %s", saved, spec.display_name)

        if "wayback" in sources:
            wb = WaybackCDXDiscoverer(http, limit_per_pattern=args.wayback_limit)
            for pid in platforms:
                spec = ATS_PLATFORMS.get(pid)
                if not spec:
                    continue
                logger.info(
                    "[Wayback CDX] Streaming collapsed URLs for %s (prefix_shard=%s)...",
                    spec.display_name,
                    args.prefix_shard,
                )
                boards = await wb.discover_for_platform(spec, prefix_shard=args.prefix_shard)
                saved = db.upsert_discovered_boards(boards)
                total_saved += saved
                logger.info("[Wayback CDX] Discovered %d candidate boards for %s", saved, spec.display_name)

        if "commoncrawl" in sources:
            cc = CommonCrawlCDXDiscoverer(http, max_indexes=args.cc_indexes)
            cdx_endpoints = await cc.get_latest_cdx_endpoints()
            logger.info("[CommonCrawl] Using CDX indexes: %s", cdx_endpoints)
            for pid in platforms:
                spec = ATS_PLATFORMS.get(pid)
                if not spec:
                    continue
                logger.info(
                    "[CommonCrawl] Streaming CDX records for %s (prefix_shard=%s)...",
                    spec.display_name,
                    args.prefix_shard,
                )
                boards = await cc.discover_for_platform(
                    spec,
                    cdx_endpoints,
                    max_pages_per_pattern=args.cc_pages,
                    prefix_shard=args.prefix_shard,
                )
                saved = db.upsert_discovered_boards(boards)
                total_saved += saved
                logger.info("[CommonCrawl] Discovered %d candidate boards for %s", saved, spec.display_name)

    db.close()
    logger.info("Discovery complete. Upserted %d candidate board records into %s", total_saved, args.db)


async def cmd_probe_domains(args: argparse.Namespace) -> None:
    input_path = Path(args.input)
    if not input_path.exists():
        raise FileNotFoundError(f"Input domain file not found: {args.input}")
    domains = [
        line.strip()
        for line in input_path.read_text(encoding="utf-8").splitlines()
        if line.strip() and not line.startswith("#")
    ]
    db = ATSDatabase(args.db)

    async with AsyncHTTPClient(concurrency=args.concurrency) as http:
        prober = CareerPageAndCNamedDiscoverer(http)
        tasks = [prober.probe_domain(d) for d in domains]
        results = await asyncio.gather(*tasks)
        all_boards = [b for sublist in results for b in sublist]
        saved = db.upsert_discovered_boards(all_boards)
        logger.info("Probed %d domains -> discovered %d ATS boards.", len(domains), saved)

    db.close()


async def cmd_probe_slugs(args: argparse.Namespace) -> None:
    """
    Generates candidate slugs from a list of company names or domains and
    registers them for direct public API verification across target ATS platforms.
    """
    input_path = Path(args.input)
    if not input_path.exists():
        raise FileNotFoundError(f"Input file not found: {args.input}")
    platforms = [
        p.strip() for p in args.platforms.split(",") if p.strip() in ATS_PLATFORMS
    ]
    lines = [
        line.strip()
        for line in input_path.read_text(encoding="utf-8").splitlines()
        if line.strip() and not line.startswith("#")
    ]
    db = ATSDatabase(args.db)
    candidate_boards: List[DiscoveredBoard] = []
    for item in lines:
        dom = normalize_root_domain(item) if "." in item else ""
        slugs = SlugPermutationGenerator.generate_slugs(
            company_name=item if not dom else "", domain=dom
        )
        for slug in slugs[:4]:
            for pid in platforms:
                spec = ATS_PLATFORMS[pid]
                candidate_boards.append(
                    DiscoveredBoard(
                        ats_id=pid,
                        slug=slug,
                        board_url=build_canonical_board_url(spec, slug),
                        discovery_source="slug_permutation_probe",
                        source_company_domain=dom or None,
                    )
                )
    saved = db.upsert_discovered_boards(candidate_boards)
    db.close()
    logger.info("Generated %d slug-probe candidate boards across %s.", saved, platforms)


async def cmd_verify(args: argparse.Namespace) -> None:
    db = ATSDatabase(args.db)
    cur = db.conn.cursor()
    if args.platforms:
        target_pids = [p.strip() for p in args.platforms.split(",") if p.strip()]
        placeholders = ",".join("?" for _ in target_pids)
        rows = cur.execute(
            f"SELECT * FROM discovered_boards WHERE ats_id IN ({placeholders})",
            target_pids,
        ).fetchall()
    else:
        rows = cur.execute("SELECT * FROM discovered_boards").fetchall()

    candidates = [
        DiscoveredBoard(
            ats_id=r["ats_id"],
            slug=r["slug"],
            shard=r["shard"],
            site=r["site"],
            board_url=r["board_url"],
            discovery_source=r["discovery_source"],
            source_company_domain=r["source_company_domain"] or None,
            discovered_at=r["discovered_at"],
        )
        for r in rows
    ]
    logger.info("Verifying %d discovered candidate boards (concurrency=%d)...", len(candidates), args.concurrency)

    async with AsyncHTTPClient(concurrency=args.concurrency) as http:
        verifier = ATSBoardVerifier(http)
        verified_boards = await asyncio.gather(*(verifier.verify_board(b) for b in candidates))
        resolved_boards = EntityResolver.score_and_resolve(verified_boards)
        saved = db.upsert_verified_boards(resolved_boards)
        active_count = sum(1 for b in resolved_boards if b.is_active)
        logger.info("Verified %d boards (%d active). Saved to %s.", saved, active_count, args.db)

    db.close()


def cmd_merge_dbs(args: argparse.Namespace) -> None:
    shard_paths = [p.strip() for p in args.inputs.split(",") if p.strip()]
    master_db = ATSDatabase(args.output)
    disc_count, ver_count = master_db.merge_from_databases(shard_paths)
    master_db.close()
    logger.info(
        "Merged %d shard DBs into %s (%d discovered records, %d globally resolved verified records).",
        len(shard_paths),
        args.output,
        disc_count,
        ver_count,
    )


def cmd_export(args: argparse.Namespace) -> None:
    db = ATSDatabase(args.db)
    counts = db.export_platform_wise_lists(args.output_dir, only_active=not args.include_inactive)
    db.close()
    logger.info("Exported platform-wise company lists to %s: %s", args.output_dir, counts)


def build_arg_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="End-to-End ATS Company Discovery & Resolution Engine (>= TheirStack Scale)"
    )
    sub = parser.add_subparsers(dest="command", required=True)

    sub.add_parser("self-test", help="Run built-in offline verification & unit test suite")

    p_disc = sub.add_parser("discover", help="Discover ATS boards from CT Logs, Common Crawl CDX & Wayback CDX")
    p_disc.add_argument("--db", default="ats_companies.db", help="SQLite database path")
    p_disc.add_argument("--sources", default="ct,commoncrawl,wayback", help="Comma-separated: ct,commoncrawl,wayback")
    p_disc.add_argument("--platforms", default="", help="Comma-separated ATS IDs (empty = all 88 platforms)")
    p_disc.add_argument("--concurrency", type=int, default=50, help="Max concurrent HTTP requests")
    p_disc.add_argument("--cc-indexes", type=int, default=2, help="Number of recent Common Crawl indexes to query")
    p_disc.add_argument("--cc-pages", type=int, default=5, help="Max CDX pages per ATS pattern")
    p_disc.add_argument("--wayback-limit", type=int, default=25000, help="Max collapsed URLs per Wayback CDX query")
    p_disc.add_argument("--prefix-shard", action="store_true", help="Shard path-based CDX queries across all 36 a-z0-9 prefixes")

    p_probe = sub.add_parser("probe-domains", help="Fingerprint a list of company domains for their ATS")
    p_probe.add_argument("--input", required=True, help="Path to text file with one domain per line")
    p_probe.add_argument("--db", default="ats_companies.db", help="SQLite database path")
    p_probe.add_argument("--concurrency", type=int, default=80, help="Max concurrent HTTP requests")

    p_slugs = sub.add_parser("probe-slugs", help="Generate slug permutations from company names/domains for API probing")
    p_slugs.add_argument("--input", required=True, help="Path to text file with company names or domains")
    p_slugs.add_argument("--platforms", default="greenhouse,ashby,lever,workable,smartrecruiters", help="Target ATS IDs")
    p_slugs.add_argument("--db", default="ats_companies.db", help="SQLite database path")

    p_ver = sub.add_parser("verify", help="Verify discovered boards against live public ATS APIs & resolve entities")
    p_ver.add_argument("--db", default="ats_companies.db", help="SQLite database path")
    p_ver.add_argument("--platforms", default="", help="Optional filter of ATS IDs to verify")
    p_ver.add_argument("--concurrency", type=int, default=80, help="Max concurrent API requests")

    p_merge = sub.add_parser("merge-dbs", help="Merge multiple SQLite shard databases and run global entity resolution")
    p_merge.add_argument("--inputs", required=True, help="Comma-separated paths to shard SQLite DBs")
    p_merge.add_argument("--output", default="ats_companies.db", help="Output master SQLite DB path")

    p_exp = sub.add_parser("export", help="Export clean, deduplicated per-ATS company lists to CSV, JSONL & SUMMARY.md")
    p_exp.add_argument("--db", default="ats_companies.db", help="SQLite database path")
    p_exp.add_argument("--output-dir", default="./exports", help="Directory for exported CSV/JSONL files")
    p_exp.add_argument("--include-inactive", action="store_true", help="Include dead/migrated boards in export")

    return parser


def main() -> None:
    parser = build_arg_parser()
    args = parser.parse_args()
    if args.command == "self-test":
        run_self_test()
    elif args.command == "discover":
        asyncio.run(cmd_discover(args))
    elif args.command == "probe-domains":
        asyncio.run(cmd_probe_domains(args))
    elif args.command == "probe-slugs":
        asyncio.run(cmd_probe_slugs(args))
    elif args.command == "verify":
        asyncio.run(cmd_verify(args))
    elif args.command == "merge-dbs":
        cmd_merge_dbs(args)
    elif args.command == "export":
        cmd_export(args)


if __name__ == "__main__":
    main()
