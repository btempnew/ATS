"""
ATS (Applicant Tracking System) Platform Definitions and URL Parsers.

Defines domain patterns, regex extractors, and metadata for major ATS platforms:
- Greenhouse
- Lever
- Ashby
- Workable
- SmartRecruiters
- Workday
- BambooHR
- Jobvite
- Rippling
- Recruitee
- JazzHR
- Breezy HR
- Pinpoint
"""

import re
from typing import Dict, Any, Optional

ATS_REGISTRY: Dict[str, Dict[str, Any]] = {
    "greenhouse": {
        "name": "Greenhouse",
        "registered_domain": "greenhouse.io",
        "tld": "io",
        "host_pattern": r"^(?:boards|job-boards(?:\.[a-z]{2,3})?)\.greenhouse\.io$",
        "url_pattern": r"^https?://(?:boards|job-boards(?:\.[a-z]{2,3})?)\.greenhouse\.io/([^/?#]+)",
        "type": "path_slug",
        "test_url": "https://boards.greenhouse.io/stripe/jobs/12345",
        "excluded_slugs": {"embed", "api", "static", "assets", "search"}
    },
    "lever": {
        "name": "Lever",
        "registered_domain": "lever.co",
        "tld": "co",
        "host_pattern": r"^(?:[a-zA-Z0-9-]+\.)*lever\.co$",
        "url_pattern": r"^https?://(?:[a-zA-Z0-9-]+\.)*lever\.co/([^/?#]+)",
        "type": "path_slug",
        "test_url": "https://jobs.lever.co/netflix/12345",
        "excluded_slugs": {"apply", "postings", "static", "assets", "help", "support"}
    },
    "ashby": {
        "name": "Ashby",
        "registered_domain": "ashbyhq.com",
        "tld": "com",
        "host_pattern": r"^(?:[a-zA-Z0-9-]+\.)*ashbyhq\.com$",
        "url_pattern": r"^https?://(?:[a-zA-Z0-9-]+\.)*ashbyhq\.com/([^/?#]+)",
        "type": "path_slug",
        "test_url": "https://jobs.ashbyhq.com/openai/12345",
        "excluded_slugs": {"api", "static", "embed", "help"}
    },
    "workable": {
        "name": "Workable",
        "registered_domain": "workable.com",
        "tld": "com",
        "host_pattern": r"^apply\.workable\.com$",
        "url_pattern": r"^https?://apply\.workable\.com/([^/?#]+)",
        "type": "path_slug",
        "test_url": "https://apply.workable.com/spotify/j/12345",
        "excluded_slugs": {"j", "api", "careers", "static"}
    },
    "smartrecruiters": {
        "name": "SmartRecruiters",
        "registered_domain": "smartrecruiters.com",
        "tld": "com",
        "host_pattern": r"^jobs\.smartrecruiters\.com$",
        "url_pattern": r"^https?://jobs\.smartrecruiters\.com/([^/?#]+)",
        "type": "path_slug",
        "test_url": "https://jobs.smartrecruiters.com/Square/12345",
        "excluded_slugs": {"oneclick", "internal", "static"}
    },
    "workday": {
        "name": "Workday",
        "registered_domain": "myworkdayjobs.com",
        "tld": "com",
        "host_pattern": r"^([^.]+)\.(?:wd\d+\.)?myworkdayjobs\.com$",
        "url_pattern": r"^https?://([^.]+)\.(?:wd\d+\.)?myworkdayjobs\.com",
        "type": "subdomain",
        "test_url": "https://salesforce.wd12.myworkdayjobs.com/External_Career_Site",
        "excluded_slugs": {"www", "wd1", "wd2", "wd3", "wd4", "wd5"}
    },
    "bamboohr": {
        "name": "BambooHR",
        "registered_domain": "bamboohr.com",
        "tld": "com",
        "host_pattern": r"^([^.]+)\.bamboohr\.com$",
        "url_pattern": r"^https?://([^.]+)\.bamboohr\.com/careers",
        "type": "subdomain",
        "test_url": "https://acme.bamboohr.com/careers/123",
        "excluded_slugs": {"www", "app", "api", "help", "status", "support", "login"}
    },
    "recruitee": {
        "name": "Recruitee",
        "registered_domain": "recruitee.com",
        "tld": "com",
        "host_pattern": r"^([^.]+)\.recruitee\.com$",
        "url_pattern": r"^https?://([^.]+)\.recruitee\.com",
        "type": "subdomain",
        "test_url": "https://techcorp.recruitee.com/o/developer",
        "excluded_slugs": {"app", "auth", "api", "blog", "careers"}
    },
    "jazzhr": {
        "name": "JazzHR",
        "registered_domain": "applytojob.com",
        "tld": "com",
        "host_pattern": r"^([^.]+)\.applytojob\.com$",
        "url_pattern": r"^https?://([^.]+)\.applytojob\.com/apply",
        "type": "subdomain",
        "test_url": "https://innovate.applytojob.com/apply/123",
        "excluded_slugs": {"app", "secure", "api", "www"}
    },
    "jobvite": {
        "name": "Jobvite",
        "registered_domain": "jobvite.com",
        "tld": "com",
        "host_pattern": r"^jobs\.jobvite\.com$",
        "url_pattern": r"^https?://jobs\.jobvite\.com/([^/?#]+)",
        "type": "path_slug",
        "test_url": "https://jobs.jobvite.com/universal/job/123",
        "excluded_slugs": {"careers", "search", "static"}
    },
    "rippling": {
        "name": "Rippling",
        "registered_domain": "rippling.com",
        "tld": "com",
        "host_pattern": r"^ats\.rippling\.com$",
        "url_pattern": r"^https?://ats\.rippling\.com/([^/?#]+)",
        "type": "path_slug",
        "test_url": "https://ats.rippling.com/startup/jobs",
        "excluded_slugs": {"api", "login", "app"}
    },
    "breezy": {
        "name": "Breezy HR",
        "registered_domain": "breezy.hr",
        "tld": "hr",
        "host_pattern": r"^([^.]+)\.breezy\.hr$",
        "url_pattern": r"^https?://([^.]+)\.breezy\.hr",
        "type": "subdomain",
        "test_url": "https://venture.breezy.hr/p/123",
        "excluded_slugs": {"app", "email", "support", "status", "www"}
    },
    "pinpoint": {
        "name": "Pinpoint",
        "registered_domain": "pinpointhq.com",
        "tld": "com",
        "host_pattern": r"^([^.]+)\.pinpointhq\.com$",
        "url_pattern": r"^https?://([^.]+)\.pinpointhq\.com",
        "type": "subdomain",
        "test_url": "https://growth.pinpointhq.com/jobs/123",
        "excluded_slugs": {"app", "api", "support", "www"}
    },
    "icims": {
        "name": "iCIMS",
        "registered_domain": "icims.com",
        "tld": "com",
        "host_pattern": r"^([^.]+)\.icims\.com$",
        "url_pattern": r"^https?://([^.]+)\.icims\.com",
        "type": "subdomain",
        "test_url": "https://careers-acme.icims.com/jobs/123",
        "excluded_slugs": {"www", "app", "api", "help", "developer", "careermaster"}
    },
    "taleo": {
        "name": "Oracle Taleo",
        "registered_domain": "taleo.net",
        "tld": "net",
        "host_pattern": r"^([^.]+)\.taleo\.net$",
        "url_pattern": r"^https?://([^.]+)\.taleo\.net",
        "type": "subdomain",
        "test_url": "https://acme.taleo.net/careersection/ex/jobsearch.ftl",
        "excluded_slugs": {"www", "app", "static", "login"}
    },
    "dayforce": {
        "name": "Dayforce",
        "registered_domain": "dayforcehcm.com",
        "tld": "com",
        "host_pattern": r"^jobs\.dayforcehcm\.com$",
        "url_pattern": r"^https?://jobs\.dayforcehcm\.com/[^/]+/([^/?#]+)",
        "type": "path_slug",
        "test_url": "https://jobs.dayforcehcm.com/en-US/acme/CANDIDATEPORTAL",
        "excluded_slugs": {"mydayforce", "static", "login"}
    },
    "ukg": {
        "name": "UKG (UltiPro)",
        "registered_domain": "ultipro.com",
        "tld": "com",
        "host_pattern": r"^recruiting2?\.ultipro\.com$",
        "url_pattern": r"^https?://recruiting2?\.ultipro\.com/([^/?#]+)",
        "type": "path_slug",
        "test_url": "https://recruiting2.ultipro.com/ACM1001/JobBoard",
        "excluded_slugs": {"login", "help", "static"}
    },
    "fountain": {
        "name": "Fountain",
        "registered_domain": "fountain.com",
        "tld": "com",
        "host_pattern": r"^apply\.fountain\.com$",
        "url_pattern": r"^https?://apply\.fountain\.com/([^/?#]+)",
        "type": "path_slug",
        "test_url": "https://apply.fountain.com/acme/opening",
        "excluded_slugs": {"static", "assets", "help"}
    },
    "trakstar": {
        "name": "Trakstar",
        "registered_domain": "recruiterbox.com",
        "tld": "com",
        "host_pattern": r"^([^.]+)\.recruiterbox\.com$",
        "url_pattern": r"^https?://([^.]+)\.recruiterbox\.com",
        "type": "subdomain",
        "test_url": "https://acme.recruiterbox.com/jobs",
        "excluded_slugs": {"app", "js", "api", "support", "www"}
    },
    "gem": {
        "name": "Gem",
        "registered_domain": "gem.com",
        "tld": "com",
        "host_pattern": r"^(?:jobs|api)\.gem\.com$",
        "url_pattern": r"^https?://(?:jobs|api)\.gem\.com/(?:job_board/v0/)?([^/?#]+)",
        "type": "path_slug",
        "test_url": "https://jobs.gem.com/bohler-",
        "excluded_slugs": {"static", "login", "docs"}
    },
    "dover": {
        "name": "Dover",
        "registered_domain": "dover.com",
        "tld": "com",
        "host_pattern": r"^app\.dover\.com$",
        "url_pattern": r"^https?://app\.dover\.com/(?:apply|careers-page)/([^/?#]+)",
        "type": "path_slug",
        "test_url": "https://app.dover.com/apply/acme",
        "excluded_slugs": {"login", "static", "api"}
    },
    "successfactors": {
        "name": "SAP SuccessFactors",
        "registered_domain": "successfactors.com",
        "tld": "com",
        "host_pattern": r"^(?:career[0-9]?|jobs[0-9]?)\.successfactors\.(?:com|eu)$",
        "url_pattern": r"[?&]company=([a-zA-Z0-9_-]+)",
        "type": "path_slug",
        "test_url": "https://career.successfactors.com/career?company=boeing",
        "excluded_slugs": {"login", "help", "static", "error", "api", "soap", "docs"}
    },
    "oraclecloud": {
        "name": "Oracle Cloud",
        "registered_domain": "oraclecloud.com",
        "tld": "com",
        "host_pattern": r"^([^.]+)\.fa\.[a-z0-9]+\.oraclecloud\.com$",
        "url_pattern": r"^https?://([^.]+)\.fa\.[a-z0-9]+\.oraclecloud\.com",
        "type": "subdomain",
        "test_url": "https://eeho.fa.us2.oraclecloud.com/hcmUI/CandidateExperience/en/sites/CX_1",
        "excluded_slugs": {"login", "static", "help", "www"}
    },
    "paycom": {
        "name": "Paycom",
        "registered_domain": "paycomonline.net",
        "tld": "net",
        "host_pattern": r"^www\.paycomonline\.net$",
        "url_pattern": r"[?&]clientkey=([a-zA-Z0-9_-]+)",
        "type": "path_slug",
        "test_url": "https://www.paycomonline.net/v4/ats/web.php/jobs?clientkey=D5700A2C9F8021BC8",
        "excluded_slugs": {"login", "static", "help"}
    },
    "paylocity": {
        "name": "Paylocity",
        "registered_domain": "paylocity.com",
        "tld": "com",
        "host_pattern": r"^recruiting\.paylocity\.com$",
        "url_pattern": r"^https?://recruiting\.paylocity\.com/recruiting/jobs/(?:Details/[^/]+/|api/jobs/list/|intro/)?([a-zA-Z0-9_-]+)",
        "type": "path_slug",
        "test_url": "https://recruiting.paylocity.com/recruiting/jobs/api/jobs/list/08e67873-6be7-4852-8700-1c05d8bcad42",
        "excluded_slugs": {"login", "static", "help", "api"}
    }
}


def extract_company_slug(url: str, host: Optional[str] = None) -> Optional[tuple[str, str]]:
    """
    Given a URL and optional host, identifies the ATS platform and extracts
    the canonical company slug.
    
    Returns:
        (platform_key, company_slug) or None if not an ATS job board.
    """
    for ats_key, config in ATS_REGISTRY.items():
        if config["type"] == "path_slug":
            m = re.search(config["url_pattern"], url, re.IGNORECASE)
            if m:
                slug = m.group(1).lower().strip()
                if slug and slug not in config["excluded_slugs"]:
                    return (ats_key, slug)
        elif config["type"] == "subdomain":
            target = host or url
            m = re.search(config["host_pattern"], target, re.IGNORECASE) if host else re.search(config["url_pattern"], url, re.IGNORECASE)
            if m:
                slug = m.group(1).lower().strip()
                if slug and slug not in config["excluded_slugs"]:
                    return (ats_key, slug)
    return None


def get_duckdb_case_expression() -> str:
    """
    Generates a SQL CASE expression to extract company slugs directly in DuckDB SQL.
    """
    clauses = []
    for key, cfg in ATS_REGISTRY.items():
        if cfg["type"] == "path_slug":
            domain = cfg["registered_domain"]
            clauses.append(
                f"WHEN url_host_registered_domain = '{domain}' "
                f"THEN regexp_extract(url, '^{cfg['url_pattern']}', 1)"
            )
        elif cfg["type"] == "subdomain":
            domain = cfg["registered_domain"]
            clauses.append(
                f"WHEN url_host_registered_domain = '{domain}' "
                f"THEN regexp_extract(url_host_name, '^{cfg['host_pattern']}', 1)"
            )
    return "CASE\n  " + "\n  ".join(clauses) + "\n  ELSE NULL END"

