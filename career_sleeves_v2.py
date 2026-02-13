import re


def _normalize_for_match(value):
    text = str(value or "").lower()
    text = re.sub(r"[^\w\s]+", " ", text)
    text = re.sub(r"\s+", " ", text).strip()
    return text


def _prepare_text(value):
    normalized = _normalize_for_match(value)
    return f" {normalized} " if normalized else " "


def _phrase_in_text(prepared_text, phrase):
    normalized_phrase = _normalize_for_match(phrase)
    if not normalized_phrase:
        return False
    return f" {normalized_phrase} " in prepared_text


def _find_hits(prepared_text, phrases):
    return {phrase for phrase in phrases if _phrase_in_text(prepared_text, phrase)}


SCHEMA_VERSION = "1.2"
ALLOW_LANGUAGES = ["nl", "en"]
VALID_SLEEVES = {"A", "B", "C", "D", "E"}
MIN_PRIMARY_SLEEVE_SCORE_TO_SHOW = 3
MIN_ABROAD_SCORE_TO_PASS = 1
ABROAD_SCORE_CAP = 4

RANKING_WEIGHTS = {
    "abroad_score": 0.35,
    "primary_sleeve_score": 0.45,
    "synergy_score": 0.20,
}

SOFT_PENALTIES = [
    {
        "if_any": [
            "Account Executive",
            "SDR",
            "Sales Development",
            "cold calling",
        ],
        "penalty_points": 15,
        "reason": "Sales-heavy role signal detected.",
    }
]

ABROAD_SIGNALS = {
    "remote_or_hybrid": {
        "positive": [
            "remote",
            "hybrid",
            "work from home",
            "wfh",
            "distributed team",
            "fully remote",
        ],
        "negative": [
            "on-site only",
            "on site only",
            "100% on site",
            "must be onsite",
            "no remote",
        ],
        "score": {"positive_hit": 2, "negative_hit": -3},
    },
    "work_from_abroad_policy": {
        "positive": [
            "work from abroad",
            "workations",
            "workation",
            "remote abroad",
            "eu remote",
            "international remote",
        ],
        "score": {"positive_hit": 2},
    },
    "travel_component": {
        "positive": [
            "travel",
            "international travel",
            "emea travel",
            "site visits",
            "client sites",
            "op locatie",
            "klantlocatie",
            "touring",
            "events on location",
        ],
        "score": {"positive_hit": 1},
    },
}

SYNERGY_SIGNALS = {
    "positive": [
        "events",
        "music",
        "live",
        "av",
        "workflow",
        "automation",
        "creative",
        "producer",
        "community",
        "partnerships",
    ],
    "cap_max": 5,
}

SLEEVE_CONFIG = {
    "A": {
        "name": "Show Systems & Venue Operations",
        "must_haves": {
            "min_title_hits": 1,
            "min_total_hits": 2,
            "must_not_have_any": [
                "IT support",
                "software developer",
                "Account Executive",
                "Technical Account Manager",
            ],
        },
        "keywords": {
            "title_positive": [
                "AV technician",
                "audiovisual technician",
                "event technician",
                "show technician",
                "venue technician",
                "house technician",
                "stagehand",
                "showcrew",
                "live sound",
                "sound technician",
                "lighting technician",
                "video technician",
                "LED technician",
                "FOH",
                "front of house",
                "monitor engineer",
                "rigging",
                "podiumtechniek",
                "geluidstechnicus",
                "lichttechnicus",
            ],
            "context_positive": [
                "festival",
                "venue",
                "theater",
                "poppodium",
                "concert",
                "tour",
                "touring",
                "club",
                "events",
                "livemuziek",
                "showcontrol",
            ],
            "negative": [
                "account manager",
                "inside sales",
                "customer success",
                "callcenter",
            ],
        },
        "scoring": {
            "points": {"title_hit": 3, "context_hit": 2, "negative_hit": -4},
            "cap_max": 5,
        },
    },
    "B": {
        "name": "Workflow Ops & Automation",
        "must_haves": {
            "min_title_hits": 1,
            "min_total_hits": 2,
            "must_have_any": ["SaaS", "workflow", "automation", "integrations"],
            "must_not_have_any": ["Account Executive", "Technical Account Manager"],
        },
        "keywords": {
            "title_positive": [
                "implementation consultant",
                "solutions engineer",
                "operations specialist",
                "business operations",
                "product operations",
                "revops",
                "customer enablement",
                "process improvement",
                "workflow",
                "automation",
                "integrations",
                "tooling",
                "systems analyst",
                "business analyst",
                "data operations",
            ],
            "context_positive": [
                "SaaS",
                "B2B",
                "platform",
                "APIs",
                "Zapier",
                "Make",
                "Airtable",
                "Notion",
                "Jira",
                "Confluence",
                "service management",
                "AI",
            ],
            "negative": [
                "pure sales",
                "quota",
                "cold outreach",
                "door-to-door",
            ],
        },
        "scoring": {
            "points": {"title_hit": 3, "context_hit": 2, "negative_hit": -5},
            "cap_max": 5,
        },
    },
    "C": {
        "name": "Experience Production & Creative Tech",
        "must_haves": {
            "min_title_hits": 1,
            "min_total_hits": 2,
            "must_not_have_any": [
                "Account Executive",
                "Technical Account Manager",
                "IT support",
            ],
        },
        "keywords": {
            "title_positive": [
                "creative producer",
                "technical producer",
                "production coordinator",
                "project producer",
                "experience producer",
                "event producer",
                "creative technologist",
                "experience designer",
                "immersive",
                "exhibition",
                "activation",
                "installation",
            ],
            "context_positive": [
                "brand experience",
                "interactive",
                "museum",
                "expo",
                "themapark",
                "scenography",
                "content production",
                "concept-to-delivery",
            ],
            "negative": ["account manager", "inside sales", "helpdesk"],
        },
        "scoring": {
            "points": {"title_hit": 3, "context_hit": 2, "negative_hit": -4},
            "cap_max": 5,
        },
    },
    "D": {
        "name": "Field Systems Engineering",
        "must_haves": {
            "min_title_hits": 1,
            "min_total_hits": 2,
            "must_have_any": [
                "installation",
                "commissioning",
                "field",
                "service",
                "inbedrijfstelling",
            ],
            "must_not_have_any": ["Technical Account Manager", "Account Executive"],
        },
        "keywords": {
            "title_positive": [
                "field service engineer",
                "service engineer",
                "service technician",
                "commissioning engineer",
                "commissioning",
                "inbedrijfstelling",
                "installation engineer",
                "maintenance technician",
                "troubleshooting",
                "on-site support",
                "systems integrator",
            ],
            "context_positive": [
                "site visits",
                "client sites",
                "op locatie",
                "klantlocatie",
                "rollout",
                "SLA",
                "incident response",
            ],
            "negative": ["callcenter", "pure sales", "account manager"],
        },
        "scoring": {
            "points": {"title_hit": 3, "context_hit": 2, "negative_hit": -4},
            "cap_max": 5,
        },
    },
    "E": {
        "name": "Scene Growth: Partnerships, Community & Programming",
        "must_haves": {
            "min_title_hits": 1,
            "min_total_hits": 2,
            "must_have_any": [
                "partnership",
                "community",
                "event",
                "program",
                "sponsorship",
                "artist relations",
            ],
            "must_not_have_any": [
                "Technical Account Manager",
                "Account Executive",
                "Inside Sales",
                "SDR",
                "Sales Development",
            ],
        },
        "keywords": {
            "title_positive": [
                "partnerships manager",
                "partnership manager",
                "community manager",
                "community lead",
                "program coordinator",
                "program manager",
                "event marketing manager",
                "sponsorship manager",
                "artist relations",
                "talent buyer",
                "booker",
            ],
            "context_positive": [
                "events",
                "festival",
                "nightlife",
                "culture",
                "music",
                "creator economy",
                "brand activations",
            ],
            "negative": ["Technical Account Manager", "account manager"],
        },
        "scoring": {
            "points": {"title_hit": 3, "context_hit": 2, "negative_hit": -6},
            "cap_max": 5,
        },
    },
}

SLEEVE_SEARCH_TERMS = {
    "A": [
        "av technician",
        "audiovisual technician",
        "show technician",
        "venue technician",
        "live sound technician",
        "lighting technician",
        "video technician",
        "stagehand",
    ],
    "B": [
        "implementation consultant",
        "solutions engineer",
        "workflow automation",
        "product operations",
        "business operations specialist",
        "systems analyst",
        "integrations specialist",
        "revops",
    ],
    "C": [
        "creative producer",
        "technical producer",
        "experience producer",
        "production coordinator",
        "creative technologist",
        "immersive producer",
        "exhibition producer",
        "event producer",
    ],
    "D": [
        "field service engineer",
        "service technician",
        "commissioning engineer",
        "installation engineer",
        "on-site support engineer",
        "systems integrator technician",
        "inbedrijfstelling",
        "service engineer",
    ],
    "E": [
        "partnerships manager",
        "community manager",
        "program coordinator",
        "sponsorship manager",
        "artist relations",
        "talent buyer",
        "booker",
        "event marketing manager",
    ],
}

LANGUAGE_REQUIRED_MARKERS = [
    "required",
    "must have",
    "mandatory",
    "fluent",
    "native",
    "c1",
    "c2",
    "vereist",
    "verplicht",
    "moet",
    "vloeiend",
    "moedertaal",
    "b2",
    "c1-niveau",
    "c2-niveau",
]

LANGUAGE_PREFERRED_MARKERS = [
    "preferred",
    "nice to have",
    "a plus",
    "bonus",
    "pre",
    "voorkeur",
    "pluspunt",
]

LANGUAGE_CATALOG = [
    {"code": "en", "names": ["english", "engels"]},
    {"code": "nl", "names": ["dutch", "nederlands"]},
    {"code": "de", "names": ["german", "duits", "deutsch"]},
    {"code": "fr", "names": ["french", "frans", "francais"]},
    {"code": "es", "names": ["spanish", "spaans", "espanol"]},
    {"code": "it", "names": ["italian", "italiaans"]},
    {"code": "pt", "names": ["portuguese", "portugees"]},
    {"code": "pl", "names": ["polish", "pools"]},
    {"code": "cs", "names": ["czech", "tsjechisch"]},
    {"code": "ro", "names": ["romanian", "roemeens"]},
    {"code": "hu", "names": ["hungarian", "hongaars"]},
    {"code": "sv", "names": ["swedish", "zweeds"]},
    {"code": "no", "names": ["norwegian", "noors"]},
    {"code": "da", "names": ["danish", "deens"]},
    {"code": "fi", "names": ["finnish", "fins"]},
    {"code": "tr", "names": ["turkish", "turks"]},
    {"code": "ru", "names": ["russian", "russisch"]},
    {"code": "uk", "names": ["ukrainian", "oekraiens"]},
    {"code": "ar", "names": ["arabic", "arabisch"]},
    {"code": "he", "names": ["hebrew", "hebreeuws"]},
    {"code": "fa", "names": ["persian", "perzisch", "farsi"]},
    {"code": "hi", "names": ["hindi"]},
    {"code": "ur", "names": ["urdu"]},
    {"code": "bn", "names": ["bengali", "bengaals"]},
    {"code": "ta", "names": ["tamil"]},
    {"code": "te", "names": ["telugu"]},
    {"code": "ja", "names": ["japanese", "japans"]},
    {"code": "ko", "names": ["korean", "koreaans"]},
    {"code": "zh", "names": ["chinese", "chinees", "mandarin", "cantonese"]},
    {"code": "vi", "names": ["vietnamese", "vietnamees"]},
    {"code": "th", "names": ["thai", "thais"]},
    {"code": "id", "names": ["indonesian", "indonesisch"]},
    {"code": "ms", "names": ["malay", "maleis"]},
    {"code": "tl", "names": ["tagalog", "filipino"]},
    {"code": "sw", "names": ["swahili"]},
    {"code": "am", "names": ["amharic", "amhaars"]},
    {"code": "so", "names": ["somali", "somalisch"]},
    {"code": "el", "names": ["greek", "grieks"]},
    {"code": "bg", "names": ["bulgarian", "bulgaars"]},
    {"code": "sr", "names": ["serbian", "servisch"]},
    {"code": "hr", "names": ["croatian", "kroatisch"]},
    {"code": "sl", "names": ["slovenian", "sloveens"]},
    {"code": "sk", "names": ["slovak", "slowaaks"]},
    {"code": "lt", "names": ["lithuanian", "litouws"]},
    {"code": "lv", "names": ["latvian", "lets"]},
    {"code": "et", "names": ["estonian", "estisch"]},
    {"code": "ga", "names": ["irish", "iers"]},
    {"code": "cy", "names": ["welsh"]},
    {"code": "is", "names": ["icelandic", "ijslands"]},
    {"code": "mt", "names": ["maltese", "maltees"]},
    {"code": "ca", "names": ["catalan", "catalaans"]},
    {"code": "eu", "names": ["basque", "baskisch"]},
    {"code": "gl", "names": ["galician", "galicisch"]},
    {"code": "af", "names": ["afrikaans"]},
    {"code": "zu", "names": ["zulu"]},
    {"code": "xh", "names": ["xhosa"]},
    {"code": "yo", "names": ["yoruba"]},
    {"code": "ha", "names": ["hausa"]},
    {"code": "ig", "names": ["igbo"]},
    {"code": "sq", "names": ["albanian", "albanees"]},
]

_LANGUAGE_LOOKUP = {}
for language in LANGUAGE_CATALOG:
    code = language.get("code")
    for name in language.get("names", []):
        normalized_name = _normalize_for_match(name)
        if not normalized_name:
            continue
        _LANGUAGE_LOOKUP[normalized_name] = code


def detect_language_flags(raw_text):
    prepared_text = _prepare_text(raw_text)
    required_marker = any(
        _phrase_in_text(prepared_text, marker) for marker in LANGUAGE_REQUIRED_MARKERS
    )
    preferred_marker = any(
        _phrase_in_text(prepared_text, marker) for marker in LANGUAGE_PREFERRED_MARKERS
    )

    extra_languages = set()
    for normalized_name, code in _LANGUAGE_LOOKUP.items():
        if code in ALLOW_LANGUAGES:
            continue
        if _phrase_in_text(prepared_text, normalized_name):
            extra_languages.add(normalized_name)

    ordered_languages = sorted(extra_languages)
    language_label = ", ".join(ordered_languages)
    required = bool(ordered_languages) and required_marker
    preferred = bool(ordered_languages) and (preferred_marker or not required_marker)
    notes = []
    if required:
        notes.append(f"Let op: deze vacature vereist ook {language_label} (naast NL/EN).")
    elif preferred:
        notes.append(
            f"Kanttekening: extra taal genoemd ({language_label}); check of het vereist is."
        )

    return {
        "extra_language_required": required,
        "extra_language_preferred": preferred,
        "extra_languages": ordered_languages,
        "allow_languages": list(ALLOW_LANGUAGES),
    }, notes


def score_abroad(raw_text):
    prepared_text = _prepare_text(raw_text)
    total = 0
    badges = []
    details = {}

    for signal_id, config in ABROAD_SIGNALS.items():
        positive_hits = _find_hits(prepared_text, config.get("positive", []))
        negative_hits = _find_hits(prepared_text, config.get("negative", []))
        score_cfg = config.get("score", {})
        total += len(positive_hits) * score_cfg.get("positive_hit", 0)
        total += len(negative_hits) * score_cfg.get("negative_hit", 0)
        if positive_hits:
            badges.append(signal_id)
        details[signal_id] = {
            "positive_hits": sorted(positive_hits),
            "negative_hits": sorted(negative_hits),
        }

    return max(0, min(ABROAD_SCORE_CAP, total)), badges, details


def score_synergy(raw_text):
    prepared_text = _prepare_text(raw_text)
    hits = _find_hits(prepared_text, SYNERGY_SIGNALS["positive"])
    return min(SYNERGY_SIGNALS["cap_max"], len(hits)), sorted(hits)


def score_sleeve(sleeve_id, raw_text, raw_title):
    config = SLEEVE_CONFIG[sleeve_id]
    keywords = config["keywords"]
    must_haves = config["must_haves"]
    points = config["scoring"]["points"]
    cap = config["scoring"]["cap_max"]

    prepared_text = _prepare_text(raw_text)
    prepared_title = _prepare_text(raw_title)

    title_hits_in_title = _find_hits(prepared_title, keywords["title_positive"])
    title_hits_in_text = _find_hits(prepared_text, keywords["title_positive"])
    context_hits = _find_hits(prepared_text, keywords["context_positive"])
    negative_hits = _find_hits(prepared_text, keywords["negative"])

    total_positive_hits = len(title_hits_in_text.union(context_hits))
    if len(title_hits_in_title) < must_haves.get("min_title_hits", 0):
        return 0, {
            "reason": "min_title_hits",
            "title_hits": sorted(title_hits_in_title),
            "context_hits": sorted(context_hits),
            "negative_hits": sorted(negative_hits),
        }
    if total_positive_hits < must_haves.get("min_total_hits", 0):
        return 0, {
            "reason": "min_total_hits",
            "title_hits": sorted(title_hits_in_title),
            "context_hits": sorted(context_hits),
            "negative_hits": sorted(negative_hits),
        }

    must_have_any = must_haves.get("must_have_any") or []
    if must_have_any and not _find_hits(prepared_text, must_have_any):
        return 0, {
            "reason": "must_have_any",
            "title_hits": sorted(title_hits_in_title),
            "context_hits": sorted(context_hits),
            "negative_hits": sorted(negative_hits),
        }

    must_not_have_any = must_haves.get("must_not_have_any") or []
    if _find_hits(prepared_text, must_not_have_any):
        return 0, {
            "reason": "must_not_have_any",
            "title_hits": sorted(title_hits_in_title),
            "context_hits": sorted(context_hits),
            "negative_hits": sorted(negative_hits),
        }

    score = (
        len(title_hits_in_title) * points.get("title_hit", 0)
        + len(context_hits) * points.get("context_hit", 0)
        + len(negative_hits) * points.get("negative_hit", 0)
    )
    score = max(0, min(cap, score))

    return score, {
        "reason": "ok",
        "title_hits": sorted(title_hits_in_title),
        "context_hits": sorted(context_hits),
        "negative_hits": sorted(negative_hits),
    }


def score_all_sleeves(raw_text, raw_title):
    scores = {}
    details = {}
    for sleeve_id in sorted(SLEEVE_CONFIG):
        sleeve_score, sleeve_details = score_sleeve(sleeve_id, raw_text, raw_title)
        scores[sleeve_id] = sleeve_score
        details[sleeve_id] = sleeve_details
    return scores, details


def evaluate_soft_penalties(raw_text):
    prepared_text = _prepare_text(raw_text)
    total_penalty = 0
    reasons = []
    for rule in SOFT_PENALTIES:
        hits = _find_hits(prepared_text, rule.get("if_any", []))
        if not hits:
            continue
        total_penalty += int(rule.get("penalty_points", 0))
        reasons.append(rule.get("reason") or f"Penalty hits: {', '.join(sorted(hits))}")
    return total_penalty, reasons

