from __future__ import annotations
from .jd_facts import extract_jd_facts

import re
from datetime import datetime, timezone
from typing import Any

from .config import MatchingSettings
from .embeddings import EmbeddingProvider
from .geo import country_of
from .hashing import (canonical_employment_type, canonical_work_mode,
                      normalize_text, relevant_source_hash, stable_hash)
from .repository import MatchingRepository
from .schemas import JobIntelligence
from .taxonomy.normalizer import Taxonomy


class JobProcessor:
    """Idempotent, artifact-level incremental global job processing."""

    def __init__(self, repository: MatchingRepository, embeddings: EmbeddingProvider,
                 taxonomy: Taxonomy, settings: MatchingSettings) -> None:
        self.repository, self.embeddings, self.taxonomy, self.settings = repository, embeddings, taxonomy, settings

    def process(self, raw: dict[str, Any]) -> tuple[JobIntelligence, set[str]]:
        job_id = str(raw.get("id") or raw.get("job_id") or "").strip()
        if not job_id:
            raise ValueError("job id is required")
        with self.repository.job_lock(job_id):
            old = self.repository.get_job(job_id)
            inputs = self._artifact_inputs(raw)
            hashes = {name: stable_hash(value) for name, value in inputs.items()}
            if old and self._fully_reusable(old, hashes):
                self._metric("job_processing_cache_hits")
                self._metric("cross_user_reuse_rate")
                return old, set()

            changed = {name for name, value in hashes.items() if not old or old.artifact_hashes.get(name) != value}
            self._metric("job_reprocessing_attempts" if old else "jobs_first_processed")
            title = str(raw.get("job_title") or raw.get("title") or "Untitled")
            cleaned = old.cleaned_text if old and "normalized" not in changed else normalize_text(raw.get("description"))
            normalized_title = old.normalized_title if old and "normalized" not in changed else normalize_text(title)

            if old and "skills" not in changed:
                required, preferred = old.required_skills, old.preferred_skills
            else:
                required = self.taxonomy.normalize_skills(_strings(raw.get("skills_required")) +
                                                          _extracted_skills(raw, "required"))
                preferred = self.taxonomy.normalize_skills(_strings(raw.get("skills_preferred")) +
                                                           _extracted_skills(raw, "preferred"))
                required = list(dict.fromkeys(required + self.taxonomy.extract_skills(str(raw.get("basic_qualifications") or ""))))
                preferred = list(dict.fromkeys(preferred + self.taxonomy.extract_skills(str(raw.get("preferred_qualifications") or ""))))
                extracted = self.taxonomy.extract_skills(cleaned)
                if not required:
                    required = extracted
                else:
                    required = list(dict.fromkeys(required + [s for s in extracted if s not in preferred]))
                preferred = [x for x in preferred if x not in required]

            if old and "role" not in changed:
                role_family, role_subfamily, domain = old.role_family, old.role_subfamily, old.domain
            else:
                role_family, role_subfamily = self.taxonomy.classify_role(title + " " + str(raw.get("role_category") or ""))
                domain = role_family

            seniority = old.seniority if old and "seniority" not in changed else _seniority(title, raw)
            exp_min, exp_max = _experience(raw)
            location = normalize_text(raw.get("location")) or None
            country = country_of(raw.get("location"), raw.get("country"))
            work_mode = canonical_work_mode(raw.get("work_mode")) or ("remote" if raw.get("is_remote") else None)
            employment = canonical_employment_type(raw.get("job_type")) or None
            salary_min, salary_max, salary_currency, salary_period = _compensation(raw)
            posted_at = _datetime(raw.get("posted_at") or raw.get("posting_date") or raw.get("date_posted"))
            education = _strings(raw.get("education_requirements")) or _extracted_education(raw)
            qualifications = " ".join(str(raw.get(key) or "") for key in
                                      ("basic_qualifications", "preferred_qualifications", "key_responsibilities"))
            matching_text = " | ".join(filter(None, [normalized_title, role_subfamily, domain,
                "required " + " ".join(required) if required else "",
                "preferred " + " ".join(preferred) if preferred else "",
                cleaned[:6000], normalize_text(qualifications)[:4000],
                f"experience {exp_min or ''} {exp_max or ''}", location, work_mode]))

            embedding_inputs = {"text": matching_text, "version": self.settings.embedding_version}
            embedding_hash = stable_hash(embedding_inputs)
            if old and old.artifact_hashes.get("embedding") == embedding_hash:
                embedding = old.embedding
                self._metric("embedding_reuse_rate")
            else:
                embedding = self.embeddings.embed([matching_text])[0]
                changed.add("embedding")
                self._metric("embeddings_generated")
            hashes["embedding"] = embedding_hash

            intelligence = JobIntelligence(
                job_id=job_id, title=title, normalized_title=normalized_title,
                company=str(raw.get("company") or ""), cleaned_text=cleaned, matching_text=matching_text,
                jd_facts=extract_jd_facts(str(raw.get('description') or ''),
                    raw.get('industry') or (raw.get('extraction') or {}).get('industry')),
                role_family=role_family, role_subfamily=role_subfamily, domain=domain, seniority=seniority,
                required_skills=required, preferred_skills=preferred,
                experience_min_years=exp_min, experience_max_years=exp_max,
                education_requirements=education, location=location, country=country,
                work_mode=work_mode, employment_type=employment, embedding=embedding,
                salary_min=salary_min, salary_max=salary_max,
                salary_currency=salary_currency, salary_period=salary_period,
                posted_at=posted_at, source_board=normalize_text(raw.get("source_board")) or None,
                first_seen_at=(old.first_seen_at if old else None) or _datetime(raw.get("first_seen_at") or raw.get("created_at")),
                job_url=str(raw.get("job_url") or raw.get("url") or "") or None,
                apply_url=str(raw.get("apply_url") or "") or None,
                lexical_terms=sorted(set(re.findall(r"[\w+#.]+", matching_text))),
                active=str(raw.get("status") or "active").casefold() not in {"expired", "deleted", "closed"},
                source_hash=relevant_source_hash(raw), artifact_hashes=hashes,
                processing_version=self.settings.processing_version,
                normalization_version=self.settings.normalization_version,
                embedding_model=self.embeddings.model_name, embedding_version=self.settings.embedding_version,
            )
            self.repository.save_job(intelligence)
            return intelligence, changed

    def _artifact_inputs(self, raw: dict[str, Any]) -> dict[str, Any]:
        return {
            "jd_facts": ['jd-facts-v1', raw.get('description'), raw.get('industry'),
                         (raw.get('extraction') or {}).get('industry')],
            "normalized": [self.settings.normalization_version, raw.get("job_title") or raw.get("title"), raw.get("description"),
                           raw.get("basic_qualifications"), raw.get("preferred_qualifications"), raw.get("key_responsibilities")],
            "skills": [self.settings.processing_version, self.taxonomy.version, raw.get("description"), raw.get("skills_required"), raw.get("skills_preferred"),
                       raw.get("basic_qualifications"), raw.get("preferred_qualifications"), raw.get("extraction")],
            "role": [self.settings.processing_version, self.taxonomy.version, raw.get("job_title") or raw.get("title"), raw.get("role_category")],
            "seniority": [self.settings.processing_version, raw.get("job_title") or raw.get("title"), raw.get("experience_min"), raw.get("experience_max"), raw.get("experience")],
            "experience": [self.settings.processing_version, raw.get("experience_min"), raw.get("experience_max"), raw.get("experience")],
            "location": [self.settings.processing_version, raw.get("location"), raw.get("country"), raw.get("work_mode"), raw.get("is_remote"), raw.get("job_type")],
            "education": [self.settings.processing_version, raw.get("education_requirements"), raw.get("extraction")],
            "status": raw.get("status") or "active",
            "compensation": [self.settings.processing_version, raw.get("salary_min"), raw.get("salary_max"),
                             raw.get("salary_currency"), raw.get("salary_period")],
            "freshness": ['publication-v2', raw.get("posted_at"), raw.get("posting_date"),
                          raw.get("date_posted"), raw.get("first_seen_at"), raw.get("created_at")],
            "source": raw.get("source_board"),
            "display": [raw.get("job_url"), raw.get("url"), raw.get("apply_url")],
        }

    def _fully_reusable(self, old: JobIntelligence, hashes: dict[str, str]) -> bool:
        return (all(old.artifact_hashes.get(k) == v for k, v in hashes.items())
                and old.processing_version == self.settings.processing_version
                and old.embedding_version == self.settings.embedding_version)

    def _metric(self, key: str) -> None:
        metrics = getattr(self.repository, "metrics", None)
        if metrics is not None:
            metrics[key] += 1


def _strings(value: Any) -> list[str]:
    if not value:
        return []
    if isinstance(value, str):
        return [v.strip() for v in re.split(r"[,;|]", value) if v.strip()]
    if isinstance(value, list):
        return [str(v.get("name") if isinstance(v, dict) else v).strip() for v in value if v]
    return []


def _extracted_skills(raw: dict[str, Any], requirement: str) -> list[str]:
    extraction = raw.get("extraction") if isinstance(raw.get("extraction"), dict) else {}
    return [str(item.get("name")) for item in extraction.get("skills", [])
            if isinstance(item, dict) and item.get("name") and item.get("requirement") == requirement]


def _extracted_education(raw: dict[str, Any]) -> list[str]:
    extraction = raw.get("extraction") if isinstance(raw.get("extraction"), dict) else {}
    return list(dict.fromkeys(str(item.get("value")) for item in extraction.get("education", [])
                              if isinstance(item, dict) and item.get("value")))


def _experience(raw: dict[str, Any]) -> tuple[float | None, float | None]:
    try:
        low = float(raw["experience_min"]) if raw.get("experience_min") is not None else None
        high = float(raw["experience_max"]) if raw.get("experience_max") is not None else None
        return low, high
    except (TypeError, ValueError):
        return None, None


def _seniority(title: str, raw: dict[str, Any]) -> str | None:
    text = normalize_text(title)
    for value, pattern in (("principal", r"\bprincipal\b"), ("lead", r"\blead\b"),
                           ("senior", r"\b(?:senior|sr)\b"), ("junior", r"\b(?:junior|jr)\b"),
                           ("intern", r"\b(?:intern|co-op)\b")):
        if re.search(pattern, text): return value
    low, _ = _experience(raw)
    if low is None: return None
    return "entry-level" if low < 2 else "mid" if low < 5 else "senior"


def _number(value: Any) -> float | None:
    try:
        return float(value) if value is not None and value != "" else None
    except (TypeError, ValueError):
        return None


def _compensation(raw: dict[str, Any]) -> tuple[float | None, float | None, str | None, str | None]:
    low, high = _number(raw.get("salary_min")), _number(raw.get("salary_max"))
    currency = str(raw.get("salary_currency") or "").upper() or None
    period = normalize_text(raw.get("salary_period")) or None
    salary_text = str(raw.get("salary") or "")
    # Historical scraper exports may have interpreted the retirement benefit
    # token "401K" as $401,000. Discard only that proven corrupt signature;
    # do not guess a replacement salary.
    if re.search(r'\b401\s*\(?k\)?\b', salary_text, re.I) and 401000 in {low, high}:
        return None, None, None, None
    values = [value for value in (low, high) if value is not None]
    if low is not None and high is not None and low > high:
        return None, None, None, None
    if values and currency in {"USD", "CAD", "GBP", "EUR", "AUD", "NZD"} and max(values) > 2_000_000:
        return None, None, None, None
    if values and period in {"hour", "hourly"} and max(values) > 1_000:
        return None, None, None, None
    if values and period in {"year", "annual", "annually"} and max(values) < 1_000:
        return None, None, None, None
    return low, high, currency, period


def _datetime(value: Any) -> datetime | None:
    if isinstance(value, datetime):
        return value if value.tzinfo else value.replace(tzinfo=timezone.utc)
    if not value:
        return None
    try:
        parsed = datetime.fromisoformat(str(value).strip().replace("Z", "+00:00"))
        return parsed if parsed.tzinfo else parsed.replace(tzinfo=timezone.utc)
    except ValueError:
        return None
