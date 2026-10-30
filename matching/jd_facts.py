"""Conservative, evidence-backed JD facts shared by extraction and SQL filters.

No company sponsorship history or silence in a posting establishes H-1B support.
Patterns intentionally use PostgreSQL/Python-compatible syntax.
"""
from __future__ import annotations

from html import unescape
import re
from typing import Literal

from pydantic import BaseModel, Field

SponsorshipStatus = Literal['yes', 'no', 'conditional', 'conflicting', 'unknown']


class JDFacts(BaseModel):
    industry: str | None = None
    industry_evidence: str | None = None
    reports_to: str | None = None
    reports_to_evidence: str | None = None
    h1b_status: SponsorshipStatus = 'unknown'
    visa_sponsorship_status: SponsorshipStatus = 'unknown'
    sponsorship_evidence: list[str] = Field(default_factory=list)


H1 = r'h[ ‐‑–—-]?1[ ‐‑–—-]?b'
VISA = r'(visa|immigration|employment visa|work visa|' + H1 + r')'
GAP = r'[^.!?;\n]{0,65}'
NEG = r'(unable to|not able to|cannot|can not|will not|do not|does not|won.t|don.t|doesn.t)'
WITHOUT = r'without\s+(the need for\s+|requiring\s+)?((current|now)\s+or\s+(in\s+the\s+)?future\s+)?'
# Availability statements only. Questions about the applicant are not statements.
NO = '|'.join([
    NEG + r'\s+(currently\s+)?((provide|offer|support)\s+)?' + VISA + r'?\s*sponsor(ship)?',
    r'no\s+(current or future\s+)?' + VISA + r'?\s*sponsorship(?!\s+(required|needed|restrictions|limitations))',
    VISA + r'?\s*sponsorship\s+(is\s+|will\s+)?(not\s+(be\s+)?(currently\s+)?(available|provided|offered|supported|considered|possible)|unavailable|unsupported)',
    r'(must|need to|required to)\s+be\s+[^.!?;\n]{0,60}authorized[^.!?;\n]{0,65}' + WITHOUT + VISA + r'?\s*sponsorship',
    r'(applicants|candidates)\s+(must|should)\s+[^.!?;\n]{0,80}' + WITHOUT + VISA + r'?\s*sponsorship',
    r'(not eligible|ineligible)\s+for\s+' + VISA + r'?\s*sponsorship',
    VISA + r'\s+sponsorship\s*:\s*no($|[^a-z])',
    VISA + r'\s+sponsorship' + GAP + r'not (available )?for (this|the) (role|position)',
    r'(candidates|applicants)\s+(requiring|who require|who need)\s+' + VISA + r'?\s*sponsorship\s+(are\s+)?(not eligible|ineligible|not considered)',
    NEG + r'\s+(sponsor|sponsor or transfer)\s+' + VISA,
])
YES = '|'.join([
    VISA + r'\s+sponsorship\s+(is\s+|will be\s+)?(available|provided|offered|supported)',
    r'(we|the company|this employer)\s+(will|can|does)\s+(provide\s+|offer\s+|support\s+)?' + VISA + r'\s+sponsor(ship)?',
    r'(we|the company|this employer)\s+(provide|offer|support)s?\s+' + VISA + r'\s+sponsorship',
    H1 + r'\s+(sponsorship\s+)?(is\s+)?(available|provided|offered|supported)',
    r'(we|the company|this employer)\s+(will|can)\s+sponsor\s+' + H1,
    r'(we|the company|this employer)\s+sponsors?\s+' + VISA,
    VISA + r'\s+sponsorship\s*:\s*yes($|[^a-z])',
])
H1_YES = YES.replace(VISA, H1)
CONDITIONAL = '|'.join([
    VISA + GAP + r'(may be|might be|could be|case.by.case|subject to|depending on|considered|consideration|eligible candidates|qualified candidates)',
    r'(may|might|could)\s+(provide\s+|offer\s+|support\s+)?' + VISA + r'\s+sponsorship',
    H1 + GAP + r'(transfer|existing|current holders)',
    r'(consider|accept|welcome)\s+[^.!?;\n]{0,30}' + H1 + r'\s+(candidates|applicants|holders)',
])
H1_NO = '|'.join([
    NEG + GAP + H1 + r'\s*(visa\s+)?(sponsor|applications|candidates)',
    r'no\s+' + H1 + r'\s+(visas?|sponsorship)(?!\s+(required|needed|restrictions|limitations))',
])
QUESTION = re.compile(r'^(?:\W)*(do you|will you|would you|are you|does the applicant|will the applicant)\b', re.I)
NON_EMPLOYMENT_PATTERN = r'(^|[^a-z])(event sponsorship|charitable sponsorship|donor sponsorship|sports sponsorship|conference sponsorship|sponsorship for an? export (license|licence))($|[^a-z])'
NON_EMPLOYMENT = re.compile(NON_EMPLOYMENT_PATTERN, re.I)


def plain_text(value: str) -> str:
    value = re.sub(r'<(?:br\s*/?|/p|/div|/li|/h[1-6])\s*>', '\n', value or '', flags=re.I)
    value = unescape(re.sub(r'<[^>]+>', ' ', value))
    return re.sub(r'[\t\r ]+', ' ', value).strip()


def plain_sql(body: str) -> str:
    body = f"regexp_replace(regexp_replace({body}, '<(br[^>]*|/p|/div|/li|/h[1-6])>', E'\\n', 'gi'), '<[^>]+>', ' ', 'g')"
    for entity, replacement in [('&nbsp;', ' '), ('&#160;', ' '), ('&#xA0;', ' '), ('&amp;', '&'), ('&quot;', '"'), ('&#39;', "'")]:
        replacement = replacement.replace("'", "''")
        body = f"replace({body}, '{entity}', '{replacement}')"
    return body


def sponsorship(text: str) -> dict:
    positive, negative, conditional, h1_positive, h1_negative, h1_conditional = [], [], [], [], [], []
    for sentence in re.split(r'(?<=[.!?;])\s+|\n+', plain_text(text)):
        sentence = sentence.strip(' •-*\t')
        if not sentence or QUESTION.search(sentence) or NON_EMPLOYMENT.search(sentence):
            continue
        no, yes, maybe = (bool(re.search(pattern, sentence, re.I)) for pattern in (NO, YES, CONDITIONAL))
        h1 = bool(re.search(H1, sentence, re.I))
        # Refusals take precedence inside a sentence; contradictory sentences
        # are preserved as conflicting rather than silently choosing one.
        if no:
            negative.append(sentence)
            h1_negative.append(sentence)  # a blanket visa refusal also excludes H-1B
        elif re.search(H1_NO, sentence, re.I):
            h1_negative.append(sentence)
        elif maybe:
            conditional.append(sentence)
            if h1:
                h1_conditional.append(sentence)
        elif yes:
            positive.append(sentence)
            if re.search(H1_YES, sentence, re.I):
                h1_positive.append(sentence)

    def status(yes, no, maybe):
        if no and (yes or maybe): return 'conflicting'
        if no: return 'no'
        if maybe: return 'conditional'
        if yes: return 'yes'
        return 'unknown'

    return dict(h1b_status=status(h1_positive, h1_negative, h1_conditional),
                visa_sponsorship_status=status(positive, negative, conditional),
                sponsorship_evidence=list(dict.fromkeys(positive + negative + conditional + h1_negative + h1_conditional)))


# Only explicit employer/industry statements count. Skills, customers and the
# job's department must not be mistaken for the employer's industry.
INDUSTRIES = {
    'Manufacturing': r'manufacturing|industrial manufacturing',
    'Technology': r'software|technology|information technology|saas',
    'Semiconductors & Electronics': r'semiconductor|semiconductors|electronics',
    'Healthcare': r'healthcare|health care|hospital|medical devices',
    'Pharmaceuticals & Biotechnology': r'pharmaceutical|pharmaceuticals|biotechnology|biotech',
    'Financial Services': r'financial services|banking|insurance|fintech',
    'Retail & E-commerce': r'retail|retailing|e-commerce|ecommerce',
    'Energy & Utilities': r'energy|utilities|oil and gas|renewable energy',
    'Aerospace & Defense': r'aerospace|defense|defence',
    'Automotive': r'automotive|automobile',
    'Transportation & Logistics': r'transportation|logistics|freight',
    'Food & Consumer Goods': r'food and beverage|consumer goods|food manufacturing',
    'Construction & Real Estate': r'construction|real estate',
    'Consulting & Professional Services': r'consulting|professional services|staffing',
    'Education': r'education|higher education',
    'Telecommunications': r'telecommunications|telecom',
}


def industry_pattern(terms: str) -> str:
    return (r'(industry\s*:\s*(' + terms + r')($|[^a-z])|'
            r'(we are|is a|is an)\s+(a\s+|an\s+)?(global\s+|leading\s+|premier\s+|innovative\s+)*('
            + terms + r')\s+(company|business|firm|provider|organization|manufacturer)|'
            r'(global |world |industry )?leader in (the )?(' + terms + r')($|[^a-z]))')


def industry(text: str, structured: str | None = None) -> tuple[str | None, str | None]:
    if isinstance(structured, str) and structured:
        for label, terms in INDUSTRIES.items():
            if structured.casefold() == label.casefold() or re.fullmatch(terms, structured, re.I):
                return label, 'Employer industry: ' + structured
    found = []
    for label, terms in INDUSTRIES.items():
        match = re.search(industry_pattern(terms), text, re.I)
        if match:
            found.append((label, match.group()))
    return found[0] if len(found) == 1 else (None, None)


def industry_sql(prefix: str = '') -> str:
    """Same conservative industry rules, applied before SQL pagination."""
    structured = f"coalesce({prefix}extraction->>'industry', '')"
    prepared = plain_sql(f"coalesce({prefix}description, '')")
    body = 'industry_jd_text'
    structured_cases = ' '.join(f"when {structured} ~* '^({terms}|{label})$' then '{label}'" for label, terms in INDUSTRIES.items())
    predicates = [(label, f"({body} ~* '{industry_pattern(terms)}')")
                  for label, terms in INDUSTRIES.items()]
    # Ambiguous employer text stays unknown, consistently with Python.
    hits = ' + '.join(f'({condition})::int' for _, condition in predicates)
    cases = ' '.join(f"when {condition} then '{label}'" for label, condition in predicates)
    any_industry = industry_pattern('|'.join(INDUSTRIES.values()))
    return (f"(select case {structured_cases} when {body} !~* '{any_industry}' then null "
            f'else (case when ({hits}) = 1 then (case {cases} else null end) else null end) end '
            f'from (select {prepared} as {body} offset 0) as industry_source)')


def h1b_sql(prefix: str = '') -> str:
    """Mirror sponsorship() over full JD sentences before pagination."""
    prepared = plain_sql(f"coalesce({prefix}description, '')")
    body = 'sponsorship_jd_text'
    def has(pattern):
        return "sentence ~* '" + pattern.replace("'", "''") + "'"
    no = f'({has(NO)} or {has(H1_NO)})'
    maybe = f'({has(H1)} and {has(CONDITIONAL)} and not {no})'
    yes = f'({has(H1_YES)} and not {no} and not {has(CONDITIONAL)})'
    summary = ("(select case when bool_or(" + no + ") and (bool_or(" + yes + ") or bool_or(" + maybe + ")) then 'conflicting' "
            "when bool_or(" + no + ") then 'no' when bool_or(" + maybe + ") then 'conditional' "
            "when bool_or(" + yes + ") then 'yes' else 'unknown' end "
            f"from regexp_split_to_table({body}, '(?<=[.!?;])\\s+|\\n+') as chunks(sentence) "
            "where sentence !~* '^[- •*[:space:]]*(do you|will you|would you|are you|does the applicant|will the applicant)([^a-z]|$)' "
            "and sentence !~* '" + NON_EMPLOYMENT_PATTERN + "' "
            "and sentence ~* '(sponsor|" + H1 + ")')")
    return (f"(select case when {body} !~* '(sponsor|{H1})' then 'unknown' else {summary} end "
            f'from (select {prepared} as {body} offset 0) as sponsorship_source)')


def industry_filter_sql(label: str) -> str:
    if label == 'Unknown':
        return f'({industry_sql()} is null)'
    terms = INDUSTRIES[label]  # labels are validated by the API; no user SQL
    structured = "coalesce(extraction->>'industry', '')"
    body = plain_sql("coalesce(description, '')")
    recognized = '|'.join(f'({pattern}|{name})' for name, pattern in INDUSTRIES.items())
    # Apply a cheap sector test before normalizing/splitting full descriptions.
    # CASE preserves evaluation order; SQL may reorder ordinary AND predicates.
    return (f"(case when {structured} ~* '^({terms}|{label})$' then true "
            f"when {structured} ~* '^({recognized})$' then false "
            f"when coalesce(description, '') !~* '({terms})' then false "
            f"when {body} !~* '{industry_pattern(terms)}' then false "
            f"else {industry_sql()} = '{label}' end)")


_REPORT = re.compile(r'\b(?:reports?\s+(?:(?:directly|functionally|administratively)\s+)?to|reporting\s+(?:(?:directly|functionally|administratively)\s+)?to|reporting line)\s*:?\s*(?:the\s+)?([^\n.;!?]{3,160})', re.I)
_MANAGER_TITLE = re.compile(r'\b(manager|director|head|president|officer|supervisor|lead|ceo|cfo|cto|coo|cpo|vp|svp|evp)\b', re.I)


def reporting_line(text: str) -> tuple[str | None, str | None]:
    found = []
    for match in _REPORT.finditer(text):
        title = re.split(r',?\s+(?:and (?:will|is|be|you|has|have|works|manages|supports|oversees|leads|provides)|who\b|while\b|where\b|with responsibility\b)', match[1], maxsplit=1, flags=re.I)[0].strip(' ,:')
        # A JD sometimes describes previous experience: "experience reporting to...".
        before = text[max(0, match.start()-65):match.start()]
        if re.search(r'(experience|previously|previous role|must have)\s+[^.!?\n]*$', before, re.I):
            continue
        if _MANAGER_TITLE.search(title) and not re.search(r'\b(you|your|customers?|clients?|regulators?)\b', title, re.I):
            found.append((title, match.group().strip()))
    unique = {title.casefold() for title, _ in found}
    return found[0] if len(unique) == 1 else (None, None)


def extract_jd_facts(text: str, structured_industry: str | None = None) -> JDFacts:
    text = plain_text(text)
    label, industry_evidence = industry(text, structured_industry)
    reports_to, reports_evidence = reporting_line(text)
    return JDFacts(industry=label, industry_evidence=industry_evidence,
                   reports_to=reports_to, reports_to_evidence=reports_evidence,
                   **sponsorship(text))
