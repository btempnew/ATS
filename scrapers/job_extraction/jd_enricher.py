"""
jd_enricher.py — Universal Job-Description Enrichment Engine
=============================================================
Normalises ANY board's raw job dict into the exact schema expected by
matching/ingestion.py → JobProcessor.process().

Boards handled: Greenhouse, Lever, Ashby, SmartRecruiters, Workday,
                BambooHR, Workable, iCIMS, Jobvite, Teamtailor, Dice, ADP
                + LinkedIn / Indeed

Fields guaranteed in output (all consumed by JobProcessor):
  job_title, company, location, country, description,
  posted_at, job_url, apply_url, source_board,
  salary_min, salary_max, salary_currency, salary_period,
  work_mode, is_remote, job_type, experience_min, experience_max,
  education_requirements, skills_required, skills_preferred,
  basic_qualifications, preferred_qualifications, key_responsibilities

Zero external API calls — fully offline regex + heuristic engine.

CLI:
  python jd_enricher.py input.json [output.json] [--board lever] [--audit]
"""

from __future__ import annotations
import json, logging, re, sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

log = logging.getLogger(__name__)

# ─────────────────────────────────────────────────────────────────────────────
# FIELD-NAME ALIAS TABLES
# Each board uses different key names for the same concept.
# ─────────────────────────────────────────────────────────────────────────────
_TITLE_KEYS   = ("job_title","title","jobTitle","requisitionTitle","name","position","positionTitle","job_name")
_DESC_KEYS    = ("description","jobDescription","content","body","details","job_description",
                 "requisitionDescription","fullDescription","descriptionPlain","text","summary")
_COMPANY_KEYS = ("company","companyName","employer","organization","company_name","hiringOrganization","employer_name")
_LOCATION_KEYS= ("location","locations","city","office","jobLocation","workLocation","place","job_location","city_state")
_DATE_KEYS    = ("posted_at","posting_date","publishedAt","releasedAt","created_at","scraped_at",
                 "datePosted","firstSeen","createdOn","publishDate","date_posted","updated_at","date")
_SAL_TEXT_KEYS= ("salary","compensation","pay","salaryRange","salary_text","pay_range","compensation_range","salary_string")
_WMODE_KEYS   = ("work_mode","workMode","workplaceType","remote_allowed","is_remote","remoteAllowed","workplace","job_flexibility")
_JTYPE_KEYS   = ("job_type","jobType","employmentType","employment_type","contractType","type","scheduleType","job_schedule")
_EXP_KEYS     = ("experience","yearsOfExperience","experience_required","years_experience","experience_level","exp")

# ─────────────────────────────────────────────────────────────────────────────
# 1. HTML STRIPPER
# ─────────────────────────────────────────────────────────────────────────────
_HTML_TAG = re.compile(r"<[^>]+>")
_HTML_ENT = [("&nbsp;", " "),("&amp;","&"),("&lt;","<"),("&gt;",">"),("&quot;",'"'),("&#39;","'")]

def _clean_html(text: str | None) -> str:
    if not text:
        return ""
    t = _HTML_TAG.sub(" ", str(text))
    for ent, rep in _HTML_ENT:
        t = t.replace(ent, rep)
    return re.sub(r"\s{2,}", " ", t).strip()


# ─────────────────────────────────────────────────────────────────────────────
# 2. MULTI-KEY COALESCER
# ─────────────────────────────────────────────────────────────────────────────
def _first(raw: dict, keys: tuple) -> Any:
    for k in keys:
        v = raw.get(k)
        if v is not None and v != "" and v != [] and v != {}:
            return v
    return None


# ─────────────────────────────────────────────────────────────────────────────
# 3. SALARY PARSER
# Handles: "$80k-$120k/yr", "100,000–150,000 USD annually",
#          "£50k", "€60,000 per year", "€30–€40/hr", "competitive"
# ─────────────────────────────────────────────────────────────────────────────
_CURRENCY_SYM = {"$":"USD","£":"GBP","€":"EUR","₹":"INR","¥":"JPY","A$":"AUD","C$":"CAD","NZ$":"NZD","S$":"SGD"}
_CURRENCY_CODE_RE = re.compile(r"\b(USD|EUR|GBP|INR|JPY|AUD|CAD|NZD|SGD|CHF|SEK|NOK|DKK|ZAR|HKD|MXN|BRL)\b",re.I)
_SYMBOL_RE = re.compile(r"(A\$|C\$|NZ\$|S\$|[£€₹¥$])")
_PERIOD_RE = re.compile(
    r"\b(per\s+hour|hourly|/\s*hr\b|/\s*h\b|per\s+month|monthly|/\s*mo\b|"
    r"per\s+year|annually|annual|/\s*yr\b|/\s*year|per\s+annum|p\.a\.)\b",re.I)

def _parse_amount(s: str) -> float | None:
    try:
        return float(s.replace(",","").strip())
    except ValueError:
        return None

def parse_salary(text: str | None) -> dict:
    out = {"salary_min":None,"salary_max":None,"salary_currency":None,"salary_period":None}
    if not text or not isinstance(text,str): return out
    t = text.strip()
    if not t or t.lower() in {"competitive","n/a","not specified","-","tbd","negotiable","market rate","doe"}: return out

    # Period
    pm = _PERIOD_RE.search(t)
    if pm:
        r = pm.group(0).lower()
        out["salary_period"] = "hourly" if any(x in r for x in ("hour","/hr","/h")) else \
                               "monthly" if any(x in r for x in ("month","/mo")) else "annual"

    # Currency
    sym = _SYMBOL_RE.search(t)
    if sym:
        out["salary_currency"] = _CURRENCY_SYM.get(sym.group(0),"USD")
    else:
        cm = _CURRENCY_CODE_RE.search(t)
        if cm: out["salary_currency"] = cm.group(0).upper()

    # Normalise k/M multipliers
    norm = re.sub(r"(\d[\d,.]*)[\s]*[kK]\b", lambda m: str(float(m.group(1).replace(",",""))*1000), t)
    norm = re.sub(r"(\d[\d,.]*)[\s]*[mM]\b", lambda m: str(float(m.group(1).replace(",",""))*1_000_000), norm)

    numbers = [n for n in (_parse_amount(m) for m in re.findall(r"\d[\d,.]*", norm)) if n is not None]
    # Filter: ignore values that look like year numbers (2020–2030 range)
    numbers = [n for n in numbers if not (2000 <= n <= 2100)]
    # Keep only salary-range candidates
    numbers = [n for n in numbers if n >= 1000 or (out["salary_period"]=="hourly" and n>=5)]

    if len(numbers) >= 2:
        out["salary_min"] = min(numbers[:2])
        out["salary_max"] = max(numbers[:2])
    elif len(numbers) == 1:
        out["salary_min"] = numbers[0]
        out["salary_max"] = numbers[0]

    # Annualise
    if out["salary_period"] == "hourly" and out["salary_min"] and out["salary_min"] < 500:
        out["salary_min"] = round(out["salary_min"]*2080)
        if out["salary_max"]: out["salary_max"] = round(out["salary_max"]*2080)
    if out["salary_period"] == "monthly" and out["salary_min"] and out["salary_min"] < 20000:
        out["salary_min"] = round(out["salary_min"]*12)
        if out["salary_max"]: out["salary_max"] = round(out["salary_max"]*12)

    return out


# ─────────────────────────────────────────────────────────────────────────────
# 4. DATE PARSER
# ─────────────────────────────────────────────────────────────────────────────
_DATE_FMTS = [
    "%Y-%m-%dT%H:%M:%S%z","%Y-%m-%dT%H:%M:%S.%f%z","%Y-%m-%dT%H:%M:%SZ",
    "%Y-%m-%dT%H:%M:%S.%fZ","%Y-%m-%d %H:%M:%S","%Y-%m-%d %H:%M:%S%z",
    "%Y-%m-%d","%d/%m/%Y","%m/%d/%Y","%B %d, %Y","%b %d, %Y","%d %B %Y","%d %b %Y",
]

def parse_date(value: Any) -> str | None:
    if value is None: return None
    if isinstance(value, datetime):
        dt = value if value.tzinfo else value.replace(tzinfo=timezone.utc)
        return dt.isoformat()
    raw = str(value).strip().replace("Z","+00:00")
    try:
        parsed = datetime.fromisoformat(raw)
        return (parsed if parsed.tzinfo else parsed.replace(tzinfo=timezone.utc)).isoformat()
    except ValueError: pass
    raw2 = re.sub(r"[+-]\d{2}:\d{2}$","",raw).replace("+00:00","")
    for fmt in _DATE_FMTS:
        try:
            parsed = datetime.strptime(raw2, fmt)
            return (parsed if parsed.tzinfo else parsed.replace(tzinfo=timezone.utc)).isoformat()
        except ValueError: continue
    try:
        ts = float(raw)
        if ts > 1e10: ts /= 1000
        return datetime.fromtimestamp(ts, tz=timezone.utc).isoformat()
    except (ValueError, OSError): pass
    return None


# ─────────────────────────────────────────────────────────────────────────────
# 5. WORK MODE DETECTOR
# ─────────────────────────────────────────────────────────────────────────────
_RE_REMOTE  = re.compile(r"\b(remote|fully\s+remote|work\s+from\s+home|wfh|distributed|anywhere|virtual)\b",re.I)
_RE_HYBRID  = re.compile(r"\bhybrid\b",re.I)
_RE_ONSITE  = re.compile(r"\b(on[\s\-]?site|in[\s\-]?office|in[\s\-]?person|on[\s\-]?location|office[\s\-]?based)\b",re.I)

def detect_work_mode(raw: dict) -> str | None:
    for k in _WMODE_KEYS:
        v = raw.get(k)
        if v is None: continue
        if isinstance(v,bool): return "remote" if v else None
        s = str(v).lower().strip()
        if s in ("remote","1","true","yes","fully remote","wfh"): return "remote"
        if s == "hybrid": return "hybrid"
        if s in ("onsite","on-site","on site","in office","in-office","in person","office","0","false","no"): return "onsite"
    corpus = " ".join(str(raw.get(k) or "") for k in ("job_title","title","location","description"))
    if _RE_HYBRID.search(corpus): return "hybrid"
    if _RE_REMOTE.search(corpus): return "remote"
    if _RE_ONSITE.search(corpus): return "onsite"
    return None


# ─────────────────────────────────────────────────────────────────────────────
# 6. EMPLOYMENT TYPE DETECTOR
# ─────────────────────────────────────────────────────────────────────────────
_JTYPE_PATTERNS = [
    ("full time",  re.compile(r"\b(full[\s\-]?time|permanent|ft\b)\b",re.I)),
    ("part time",  re.compile(r"\bpart[\s\-]?time\b",re.I)),
    ("contract",   re.compile(r"\b(contract|contractor|freelance|consulting|fixed[\s\-]?term|temp\b|temporary)\b",re.I)),
    ("internship", re.compile(r"\b(intern\b|internship|co[\s\-]?op|coop)\b",re.I)),
]

def detect_job_type(raw: dict) -> str | None:
    for k in _JTYPE_KEYS:
        v = raw.get(k)
        if v and isinstance(v,str) and v.strip():
            s = v.lower().strip().replace("_"," ").replace("-"," ")
            for canonical,_ in _JTYPE_PATTERNS:
                if canonical.replace(" ","") in s.replace(" ",""): return canonical
            if s: return s
    corpus = " ".join(str(raw.get(k) or "") for k in ("job_title","title","description"))
    for canonical,pat in _JTYPE_PATTERNS:
        if pat.search(corpus): return canonical
    return None


# ─────────────────────────────────────────────────────────────────────────────
# 7. EXPERIENCE RANGE EXTRACTOR
# ─────────────────────────────────────────────────────────────────────────────
_EXP_RANGE   = re.compile(r"(\d+(?:\.\d+)?)\s*[\-\u2013\u2014to]+\s*(\d+(?:\.\d+)?)\s*(?:\+)?\s*years?",re.I)
_EXP_PLUS    = re.compile(r"(\d+(?:\.\d+)?)\s*\+\s*years?",re.I)
_EXP_ATLEAST = re.compile(r"(?:at\s+least|minimum|min\.?|>)\s*(\d+(?:\.\d+)?)\s*years?",re.I)
_EXP_SINGLE  = re.compile(r"(\d+(?:\.\d+)?)\s*years?\s*(?:of\s+)?(?:experience|exp\.?)",re.I)

def extract_experience(raw: dict) -> tuple[float|None, float|None]:
    try:
        lo = raw.get("experience_min") or raw.get("min_experience")
        hi = raw.get("experience_max") or raw.get("max_experience")
        if lo is not None or hi is not None:
            return (float(lo) if lo is not None else None, float(hi) if hi is not None else None)
    except (TypeError,ValueError): pass
    exp_raw = next((str(raw.get(k)) for k in _EXP_KEYS if raw.get(k)), None)
    corpus = " ".join(filter(None,[exp_raw,
                                   str(raw.get("description") or ""),
                                   str(raw.get("basic_qualifications") or ""),
                                   str(raw.get("preferred_qualifications") or "")]))[:5000]
    m = _EXP_RANGE.search(corpus)
    if m: return float(m.group(1)), float(m.group(2))
    m = _EXP_PLUS.search(corpus)
    if m:
        v = float(m.group(1)); return v, v+3
    m = _EXP_ATLEAST.search(corpus)
    if m: return float(m.group(1)), None
    m = _EXP_SINGLE.search(corpus)
    if m: return float(m.group(1)), None
    return None, None


# ─────────────────────────────────────────────────────────────────────────────
# 8. EDUCATION EXTRACTOR
# ─────────────────────────────────────────────────────────────────────────────
_EDU_PATTERNS = [
    ("phd",        re.compile(r"\b(ph\.?d|doctorate|doctoral)\b",re.I)),
    ("masters",    re.compile(r"\b(master'?s?|m\.?s\.?\b|m\.?eng\.?\b|m\.?b\.?a\.?\b|msc)\b",re.I)),
    ("bachelors",  re.compile(r"\b(bachelor'?s?|b\.?s\.?\b|b\.?e\.?\b|b\.?eng\.?\b|b\.?tech\b|undergraduate|degree)\b",re.I)),
    ("associates", re.compile(r"\b(associate'?s?|a\.?s\.?\b|a\.?a\.?\b)\b",re.I)),
    ("bootcamp",   re.compile(r"\b(bootcamp|boot\s+camp|coding\s+school)\b",re.I)),
]

def extract_education(raw: dict) -> list[str]:
    exp = raw.get("education_requirements") or raw.get("education") or raw.get("educationRequirements")
    if exp:
        if isinstance(exp,list): return [str(e) for e in exp if e]
        return [str(exp)]
    corpus = " ".join(str(raw.get(k) or "") for k in ("description","basic_qualifications","preferred_qualifications"))[:3000]
    return [label for label,pat in _EDU_PATTERNS if pat.search(corpus)]


# ─────────────────────────────────────────────────────────────────────────────
# 9. COUNTRY INFERRER
# ─────────────────────────────────────────────────────────────────────────────
_US_STATES = {"AL","AK","AZ","AR","CA","CO","CT","DE","FL","GA","HI","ID","IL","IN","IA","KS","KY","LA","ME","MD",
              "MA","MI","MN","MS","MO","MT","NE","NV","NH","NJ","NM","NY","NC","ND","OH","OK","OR","PA","RI","SC",
              "SD","TN","TX","UT","VT","VA","WA","WV","WI","WY","DC"}
_COUNTRY_HINTS = [
    ("US",re.compile(r"\b(usa?|united\s+states?|u\.s\.a?\.?|("+"|".join(_US_STATES)+r"))\b",re.I)),
    ("GB",re.compile(r"\b(uk|united\s+kingdom|england|scotland|wales|london)\b",re.I)),
    ("CA",re.compile(r"\b(canada|ontario|british\s+columbia|alberta|quebec)\b",re.I)),
    ("AU",re.compile(r"\b(australia|sydney|melbourne|brisbane|perth)\b",re.I)),
    ("IN",re.compile(r"\b(india|bangalore|bengaluru|mumbai|hyderabad|delhi|pune|chennai)\b",re.I)),
    ("DE",re.compile(r"\b(germany|deutschland|berlin|munich|frankfurt|hamburg)\b",re.I)),
    ("SG",re.compile(r"\bsingapore\b",re.I)),
    ("NL",re.compile(r"\b(netherlands|amsterdam|rotterdam|eindhoven)\b",re.I)),
]

def infer_country(location: str|None) -> str|None:
    if not location: return None
    for code,pat in _COUNTRY_HINTS:
        if pat.search(location): return code
    return None


# ─────────────────────────────────────────────────────────────────────────────
# 10. JD SECTION SPLITTER
# Splits raw description into basic_qualifications / preferred_qualifications /
# key_responsibilities so ingestion.py can use them directly.
# ─────────────────────────────────────────────────────────────────────────────
_SEC_HEADERS = {
    "basic_qualifications": re.compile(
        r"(?m)^[\s\-\*•]*(?:required|minimum|basic|must[\s\-]have|essential|mandatory|hard)"
        r"\s*(?:qualifications?|requirements?|skills?|experience)[:\-]?\s*$", re.I),
    "preferred_qualifications": re.compile(
        r"(?m)^[\s\-\*•]*(?:preferred|nice[\s\-]to[\s\-]have|bonus|desired|additional|plus|ideal)"
        r"\s*(?:qualifications?|requirements?|skills?|experience)?[:\-]?\s*$", re.I),
    "key_responsibilities": re.compile(
        r"(?m)^[\s\-\*•]*(?:responsibilities?|duties|what\s+you.?ll\s+do|your\s+role|"
        r"the\s+role|job\s+(?:duties|summary|description)|you\s+will)[:\-]?\s*$", re.I),
}

def split_jd_sections(description: str) -> dict[str,str]:
    if not description: return {}
    positions: list[tuple[int,str]] = []
    for name,pat in _SEC_HEADERS.items():
        for m in pat.finditer(description):
            positions.append((m.start(),name))
    positions.sort()
    result: dict[str,str] = {}
    for i,(start,name) in enumerate(positions):
        end = positions[i+1][0] if i+1 < len(positions) else len(description)
        lines = description[start:end].strip().splitlines()
        content = "\n".join(lines[1:]).strip() if len(lines)>1 else ""
        if content and name not in result:
            result[name] = content
    return result


# ─────────────────────────────────────────────────────────────────────────────
# 11. CORE ENRICH FUNCTION
# ─────────────────────────────────────────────────────────────────────────────
def enrich(raw: dict[str,Any]) -> dict[str,Any]:
    """
    Takes a raw job dict from ANY board and returns a copy enriched with
    ALL fields required by matching/ingestion.py → JobProcessor.process().
    Non-destructive: original dict is not modified.
    """
    out = dict(raw)

    # 1. Title
    title = _first(raw, _TITLE_KEYS)
    out["job_title"] = _clean_html(str(title)) if title else "Untitled"

    # 2. Company
    company = _first(raw, _COMPANY_KEYS)
    out["company"] = _clean_html(str(company)) if company else ""

    # 3. Description (clean HTML from any board)
    desc_raw = _first(raw, _DESC_KEYS)
    description = _clean_html(str(desc_raw)) if desc_raw else ""
    out["description"] = description

    # 4. Location + Country
    loc_raw = _first(raw, _LOCATION_KEYS)
    if isinstance(loc_raw, list):
        loc_raw = ", ".join(str(x) for x in loc_raw if x)
    location = _clean_html(str(loc_raw)) if loc_raw else ""
    out["location"] = location
    if not out.get("country"):
        out["country"] = infer_country(location)

    # 5. Posted date (try all known date keys) — always set key, even if None
    found_date = None
    for k in _DATE_KEYS:
        v = raw.get(k)
        if v:
            d = parse_date(v)
            if d:
                found_date = d
                break
    out["posted_at"] = found_date

    # Rows that went through job_extraction.extract_job already carry its evidence-checked
    # salary, work mode, job type and experience. The guesses below are looser ("401k" read
    # as pay, "60 years of experience across industries" read as a requirement, "contract
    # negotiation" read as a contract job), so they only fill rows the extractor never saw.
    extracted = isinstance(raw.get("extraction"), dict) and str(raw["extraction"].get("version", "")).startswith("jd-")

    # 6. Salary — prefer explicit numeric fields, fall back to text parsing
    sal_min = raw.get("salary_min")
    sal_max = raw.get("salary_max")
    if extracted:
        pass
    elif sal_min is None and sal_max is None:
        sal_text = _first(raw, _SAL_TEXT_KEYS)
        if not sal_text:
            # Try to find salary mention inside description
            dm = re.search(r"(?:salary|compensation|pay(?:ing)?)[:\s]+([^\n]{5,80})", description, re.I)
            if dm: sal_text = dm.group(1)
        if sal_text:
            parsed = parse_salary(str(sal_text))
            out["salary_min"]      = parsed["salary_min"]
            out["salary_max"]      = parsed["salary_max"]
            out["salary_currency"] = parsed["salary_currency"] or out.get("salary_currency")
            out["salary_period"]   = parsed["salary_period"]   or out.get("salary_period")
    else:
        # Numeric fields exist — coerce and infer currency if missing
        try:    out["salary_min"] = float(sal_min) if sal_min is not None else None
        except (TypeError,ValueError):
            parsed = parse_salary(str(sal_min))
            out.update({k:v for k,v in parsed.items() if v is not None})
        try:    out["salary_max"] = float(sal_max) if sal_max is not None else None
        except (TypeError,ValueError): pass
        if not out.get("salary_currency"):
            sal_text = _first(raw, _SAL_TEXT_KEYS)
            if sal_text:
                out["salary_currency"] = parse_salary(str(sal_text)).get("salary_currency")

    if not extracted:
        # 7. Work mode + is_remote
        wm = detect_work_mode(raw)
        out["work_mode"] = wm
        out["is_remote"] = wm == "remote"

        # 8. Employment type
        out["job_type"] = detect_job_type(raw)

        # 9. Experience range
        exp_min, exp_max = extract_experience(raw)
        out["experience_min"] = exp_min
        out["experience_max"] = exp_max

    # 10. Education
    if not out.get("education_requirements"):
        out["education_requirements"] = extract_education(raw)

    # 11. JD section splitting (only when sections not already provided)
    if description and not any(out.get(k) for k in ("basic_qualifications","preferred_qualifications","key_responsibilities")):
        sections = split_jd_sections(description)
        for k,v in sections.items():
            out.setdefault(k, v)

    # 12. Skills — ensure list type
    for k in ("skills_required","skills_preferred"):
        v = out.get(k)
        if isinstance(v,str):
            out[k] = [s.strip() for s in re.split(r"[,;|]",v) if s.strip()]
        elif v is None:
            out[k] = []

    # 13. source_board
    if not out.get("source_board"):
        out["source_board"] = raw.get("scraper_type") or raw.get("board") or "unknown"

    # 14. URL fields
    for k in ("job_url","apply_url"):
        if not out.get(k):
            out[k] = raw.get("url") or raw.get("link") or raw.get("applicationUrl") or ""

    return out


# ─────────────────────────────────────────────────────────────────────────────
# 12. BATCH PROCESSOR
# ─────────────────────────────────────────────────────────────────────────────
def enrich_batch(jobs: list[dict[str,Any]], board_name: str|None=None) -> list[dict[str,Any]]:
    """Enrich a list of raw job dicts. Skips (warns) on individual errors."""
    enriched = []
    for job in jobs:
        try:
            e = enrich(job)
            if board_name and not e.get("source_board"):
                e["source_board"] = board_name.lower()
            enriched.append(e)
        except Exception as exc:
            log.warning("jd_enricher: skipped job %s — %s", job.get("id","?"), exc)
    return enriched


# ─────────────────────────────────────────────────────────────────────────────
# 13. COVERAGE AUDITOR
# ─────────────────────────────────────────────────────────────────────────────
MATCHING_FIELDS = [
    "job_title","company","location","country","description","posted_at","job_url","source_board",
    "salary_min","salary_max","salary_currency","salary_period",
    "work_mode","job_type","experience_min","experience_max",
    "education_requirements","skills_required","skills_preferred",
    "basic_qualifications","preferred_qualifications","key_responsibilities",
]

def audit_coverage(jobs: list[dict[str,Any]]) -> dict[str,float]:
    if not jobs: return {}
    counts = {f:0 for f in MATCHING_FIELDS}
    for job in jobs:
        for f in MATCHING_FIELDS:
            v = job.get(f)
            if v is not None and v != "" and v != [] and v != {}:
                counts[f] += 1
    return {f: round(c/len(jobs),3) for f,c in counts.items()}


# ─────────────────────────────────────────────────────────────────────────────
# 14. CLI
# ─────────────────────────────────────────────────────────────────────────────
if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser(description="Enrich a board's JSON job file for the matching algorithm.")
    parser.add_argument("input",  help="Input JSON (list of job dicts)")
    parser.add_argument("output", nargs="?", default=None, help="Output JSON (default: stdout)")
    parser.add_argument("--board", default=None, help="Override source_board tag")
    parser.add_argument("--audit", action="store_true", help="Print field coverage diff and exit")
    args = parser.parse_args()

    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
    raw_jobs: list[dict] = json.loads(Path(args.input).read_text(encoding="utf-8"))
    if not isinstance(raw_jobs, list): raw_jobs = [raw_jobs]

    enriched = enrich_batch(raw_jobs, board_name=args.board)

    if args.audit:
        before = audit_coverage(raw_jobs)
        after  = audit_coverage(enriched)
        print(f"\n{'Field':<35} {'Before':>8}  {'After':>8}  {'Δ':>8}")
        print("-"*60)
        for f in MATCHING_FIELDS:
            b, a = before.get(f,0), after.get(f,0)
            delta = a - b
            mark = "▲" if delta > 0 else ("▼" if delta < 0 else " ")
            print(f"{f:<35} {b:>7.1%}  {a:>7.1%}  {mark}{abs(delta):.1%}")
        print(f"\nTotal jobs: {len(enriched)}")
        sys.exit(0)

    out_text = json.dumps(enriched, indent=2, ensure_ascii=False, default=str)
    if args.output:
        Path(args.output).write_text(out_text, encoding="utf-8")
        log.info("Written %d enriched jobs → %s", len(enriched), args.output)
    else:
        print(out_text)


