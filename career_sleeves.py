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
    if " " in normalized_phrase:
        return f" {normalized_phrase} " in prepared_text

    if normalized_phrase.endswith("y") and len(normalized_phrase) > 3:
        plural_variant = normalized_phrase[:-1] + "ies"
        pattern = rf"\b(?:{re.escape(normalized_phrase)}|{re.escape(plural_variant)})\b"
        return bool(re.search(pattern, prepared_text))

    pattern = rf"\b{re.escape(normalized_phrase)}(?:s|es)?\b"
    return bool(re.search(pattern, prepared_text))


def _find_hits(prepared_text, phrases):
    return {phrase for phrase in phrases if _phrase_in_text(prepared_text, phrase)}


SCHEMA_VERSION = "2.0"
ALLOW_LANGUAGES = ["nl", "en"]
VALID_SLEEVES = {"A", "B", "C", "D", "E"}
MIN_PRIMARY_SLEEVE_SCORE_TO_SHOW = 3
MIN_ABROAD_SCORE_TO_PASS = 1
ABROAD_SCORE_CAP = 4
MIN_TOTAL_HITS_TO_SHOW = 2
MIN_PRIMARY_SLEEVE_SCORE_TO_MAYBE = 2
MIN_TOTAL_HITS_TO_MAYBE = 1

RANKING_WEIGHTS = {
    "abroad_score": 0.28,
    "primary_sleeve_score": 0.47,
    "synergy_score": 0.15,
    "location_proximity_score": 0.10,
}

SOFT_PENALTIES = [
    {
        "if_any": [
            "Account Executive",
            "SDR",
            "Sales Development",
        ],
        "penalty_points": 12,
        "reason": "Sales-heavy role signal detected.",
    },
    {
        "if_any": [
            "cold calling",
            "commission only",
            "door-to-door",
        ],
        "penalty_points": 10,
        "reason": "High-friction sales context detected.",
    },
    {
        "if_any": [
            "Technical Account Manager",
        ],
        "penalty_points": 20,
        "reason": "Technical Account Manager profile likely outside target fit.",
    }
]

HARD_REJECT_TITLE_PATTERNS = [
    "Account Executive",
    "SDR",
    "Sales Development",
    "Sales Development Representative",
    "Technical Account Manager",
]

HARD_REJECT_TEXT_PATTERNS = [
    "door-to-door",
    "commission only",
]

HARD_REJECT_COLD_CALLING_CONTEXT = [
    "sales",
    "quota",
    "business development",
]

ABROAD_SIGNALS = {
    "remote_or_hybrid": {
        "positive": [
            "remote",
            "hybrid",
            "hybride",
            "work from home",
            "thuiswerk",
            "op afstand",
            "wfh",
            "remote first",
            "remote-first",
            "distributed team",
            "fully remote",
        ],
        "negative": [
            "on-site only",
            "on site only",
            "100% on site",
            "must be onsite",
            "must be on site",
            "no remote",
            "office based",
            "op locatie vereist",
            "kantoorplicht",
        ],
        "score": {"positive_hit": 2, "negative_hit": -3},
    },
    "work_from_abroad_policy": {
        "positive": [
            "work from abroad",
            "work from anywhere",
            "work anywhere",
            "workations",
            "workation",
            "remote abroad",
            "werken vanuit buitenland",
            "werk vanuit het buitenland",
            "werken vanuit europa",
            "remote within europe",
            "anywhere in eu",
            "eu remote",
            "international remote",
            "international mobility",
            "digital nomad",
        ],
        "score": {"positive_hit": 2},
    },
    "travel_component": {
        "positive": [
            "travel",
            "travelling",
            "international travel",
            "reisbereid",
            "reizen",
            "internationaal reizen",
            "emea travel",
            "site visits",
            "site visit",
            "client sites",
            "client site",
            "op locatie",
            "on location",
            "klantlocatie",
            "klantbezoek",
            "touring",
            "events on location",
            "multi-site",
            "cross-site",
            "field visits",
        ],
        "negative": [
            "no travel required",
            "geen reisbereidheid nodig",
            "without travel",
            "zonder reizen",
        ],
        "score": {"positive_hit": 1},
    },
}

SYNERGY_SIGNALS = {
    "positive": [
        "international",
        "operations",
        "delivery",
        "multi-site",
        "travel",
        "on-site",
        "stakeholder",
        "workflow",
        "reliability",
        "commissioning",
        "ecosystem",
        "infrastructure",
    ],
    "cap_max": 5,
}

SLEEVE_CONFIG = {
    "A": {
        "name": "International Music Events & Festivals",
        "tagline": "Creative + operations roles in international live music where shows, teams, and on-site delivery must align.",
        "must_haves": {
            "min_title_hits": 1,
            "min_total_hits": 2,
            "bonus_signals": [
                "touring",
                "international travel",
                "festival operations",
                "artist liaison",
                "show operations",
            ],
        },
        "keywords": {
            "title_positive": [
                "event producer",
                "festival producer",
                "tour manager",
                "production manager",
                "show caller",
                "stage manager",
                "artist liaison",
                "technical producer",
                "venue operations manager",
                "event operations manager",
                "live production manager",
                "music events coordinator",
                "festival operations manager",
                "show operations manager",
                "concert production manager",
            ],
            "context_positive": [
                "festival",
                "concert",
                "tour",
                "touring",
                "live music",
                "music events",
                "crew scheduling",
                "artist hospitality",
                "backstage",
                "show control",
                "load-in",
                "load out",
                "production office",
                "stakeholder alignment",
                "on-site",
                "travel rotations",
                "international events",
                "FOH",
                "lighting",
                "audio",
            ],
            "negative": [
                "inside sales",
                "account executive",
                "sdr",
                "door-to-door",
                "callcenter",
            ],
        },
        "scoring": {
            "points": {
                "title_hit": 3,
                "context_hit": 2,
                "bonus_hit": 1,
                "negative_hit": -3,
                "title_gate_bonus": 1,
                "coverage_bonus": 1,
            },
            "cap_max": 5,
        },
    },
    "B": {
        "name": "International Theme Parks & Destinations",
        "tagline": "Experience delivery + operations discipline for theme parks and immersive destinations in multi-site international settings.",
        "must_haves": {
            "min_title_hits": 1,
            "min_total_hits": 2,
            "bonus_signals": [
                "theme park",
                "attractions",
                "guest flow",
                "safety",
                "show quality",
            ],
        },
        "keywords": {
            "title_positive": [
                "theme park operations manager",
                "attractions operations manager",
                "guest experience manager",
                "show operations manager",
                "ride operations manager",
                "destination operations manager",
                "park operations supervisor",
                "entertainment operations manager",
                "experience operations manager",
                "resort operations manager",
                "site operations manager",
                "operations duty manager",
            ],
            "context_positive": [
                "theme park",
                "amusement park",
                "attractions",
                "immersive destination",
                "guest flow",
                "queue management",
                "safety",
                "maintenance window",
                "show quality",
                "multi-site",
                "destination",
                "resort",
                "park opening",
                "park closing",
                "staffing plan",
                "SOP",
            ],
            "negative": [
                "inside sales",
                "account executive",
                "sdr",
                "callcenter",
            ],
        },
        "scoring": {
            "points": {
                "title_hit": 3,
                "context_hit": 2,
                "bonus_hit": 1,
                "negative_hit": -5,
                "title_gate_bonus": 1,
                "coverage_bonus": 1,
            },
            "cap_max": 5,
        },
    },
    "C": {
        "name": "International Data Centers & Facilities",
        "tagline": "AI/compute infrastructure operations where reliability, safety, scaling, and execution across sites are central.",
        "must_haves": {
            "min_title_hits": 1,
            "min_total_hits": 2,
            "bonus_signals": [
                "data center",
                "critical facilities",
                "commissioning",
                "uptime",
                "compute infrastructure",
            ],
        },
        "keywords": {
            "title_positive": [
                "data center operations",
                "data centre operations",
                "facility operations manager",
                "critical facilities technician",
                "site operations manager",
                "commissioning engineer",
                "commissioning manager",
                "capacity planner",
                "infrastructure operations manager",
                "mission critical operations",
                "facilities coordinator",
                "technical program manager",
                "operations program manager",
                "vendor manager",
                "site reliability engineer",
            ],
            "context_positive": [
                "data center",
                "data centre",
                "colocation",
                "hyperscale",
                "uptime",
                "availability",
                "redundancy",
                "mission critical",
                "commissioning",
                "capacity expansion",
                "build-out",
                "vendor coordination",
                "change management",
                "maintenance window",
                "ai infrastructure",
                "gpu cluster",
                "compute infrastructure",
                "facility reliability",
                "site visit",
            ],
            "negative": [
                "inside sales",
                "account executive",
                "sdr",
                "callcenter",
            ],
        },
        "scoring": {
            "points": {
                "title_hit": 3,
                "context_hit": 2,
                "bonus_hit": 1,
                "negative_hit": -4,
                "title_gate_bonus": 1,
                "coverage_bonus": 1,
            },
            "cap_max": 5,
        },
    },
    "D": {
        "name": "International Supply Chains & Ecosystems",
        "tagline": "Operations roles where cross-site flow, vendor ecosystems, and delivery reliability need structured execution.",
        "must_haves": {
            "min_title_hits": 1,
            "min_total_hits": 2,
            "bonus_signals": [
                "supply chain",
                "vendor management",
                "partner ecosystem",
                "rollout",
                "implementation",
            ],
        },
        "keywords": {
            "title_positive": [
                "supply chain manager",
                "supply chain operations",
                "logistics operations manager",
                "ecosystem manager",
                "partner operations manager",
                "vendor operations manager",
                "implementation manager",
                "rollout manager",
                "program manager supply chain",
                "operations coordinator",
                "network operations manager",
                "procurement operations",
                "distribution operations",
                "fulfillment operations",
                "delivery operations",
            ],
            "context_positive": [
                "supply chain",
                "logistics",
                "inventory flow",
                "demand planning",
                "vendor management",
                "partner ecosystem",
                "cross-site rollout",
                "multi-site",
                "implementation",
                "standard operating procedures",
                "workflow",
                "rollout",
                "reliability",
                "service levels",
                "distribution center",
                "transport planning",
                "last mile",
                "global trade",
                "international suppliers",
                "ecosystem",
            ],
            "negative": [
                "inside sales",
                "account executive",
                "sdr",
                "cold calling",
            ],
        },
        "scoring": {
            "points": {
                "title_hit": 3,
                "context_hit": 2,
                "bonus_hit": 1,
                "negative_hit": -4,
                "title_gate_bonus": 1,
                "coverage_bonus": 1,
            },
            "cap_max": 5,
        },
    },
    "E": {
        "name": "Custom / User-defined Sleeve",
        "tagline": "User-configurable sleeve for custom role archetypes, keywords, exclusions, and locations.",
        "must_haves": {
            "min_title_hits": 0,
            "min_total_hits": 1,
            "bonus_signals": [],
        },
        "keywords": {
            "title_positive": [
                "operations manager",
                "program manager",
                "project manager",
                "implementation manager",
                "operations coordinator",
                "consultant",
                "specialist",
                "producer",
                "engineer",
                "analyst",
            ],
            "context_positive": [
                "operations",
                "delivery",
                "workflow",
                "process",
                "implementation",
                "coordination",
                "stakeholder",
                "cross-functional",
                "site",
                "quality",
                "reliability",
                "planning",
                "execution",
            ],
            "negative": [
                "inside sales",
                "account executive",
                "sdr",
                "cold calling",
                "door-to-door",
            ],
        },
        "scoring": {
            "points": {
                "title_hit": 3,
                "context_hit": 2,
                "bonus_hit": 1,
                "negative_hit": -4,
                "title_gate_bonus": 1,
                "coverage_bonus": 1,
            },
            "cap_max": 5,
        },
    },
}

SLEEVE_SEARCH_TERMS = {
    "A": [
        "music event operations",
        "festival producer",
        "tour",
        "concert production",
        "live production",
        "artist liaison",
        "show operations",
        "event operations",
    ],
    "B": [
        "theme park operations",
        "attractions operations",
        "guest experience",
        "show operations",
        "ride operations",
        "destination operations",
        "resort operations",
        "immersive destination operations",
    ],
    "C": [
        "data center operations",
        "critical facilities technician",
        "facility operations",
        "commissioning engineer data center",
        "mission critical operations",
        "compute infrastructure operations",
        "ai infrastructure operations",
        "capacity expansion data center",
    ],
    "D": [
        "supply chain operations",
        "logistics operations",
        "vendor operations",
        "partner operations",
        "ecosystem operations",
        "rollout",
        "implementation supply chain",
        "distribution operations",
    ],
    "E": [
        # Intentionally empty: fully user-defined sleeve.
    ],
}

SEARCH_LOCATIONS = {
    "nl": ["Netherlands"],
    "emea": ["Europe", "Amsterdam", "Berlin", "Barcelona", "Milan", "Warsaw"],
}

LOCATION_MODE_PASSES = {
    "nl_only": ["nl"],
    "nl_eu": ["nl", "emea"],
    "global": ["nl", "emea"],
}

LOCATION_MODE_LABELS = {
    "nl_only": "Netherlands focus",
    "nl_eu": "Netherlands + EMEA",
    "global": "Global (NL + EMEA discovery)",
}

BLOCKED_PAGE_HINTS = [
    "captcha",
    "are you a robot",
    "access denied",
    "blocked",
    "security check",
    "unusual traffic",
    "verify you are human",
    "sign in to continue",
]

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
    {"code": "aa", "names": ["afar"]},
    {"code": "ab", "names": ["abkhazian", "abkhaz"]},
    {"code": "ae", "names": ["avestan"]},
    {"code": "ak", "names": ["akan"]},
    {"code": "an", "names": ["aragonese"]},
    {"code": "as", "names": ["assamese"]},
    {"code": "av", "names": ["avaric", "avar"]},
    {"code": "ay", "names": ["aymara"]},
    {"code": "az", "names": ["azerbaijani", "azeri"]},
    {"code": "ba", "names": ["bashkir"]},
    {"code": "bh", "names": ["bihari"]},
    {"code": "bi", "names": ["bislama"]},
    {"code": "bm", "names": ["bambara"]},
    {"code": "bo", "names": ["tibetan"]},
    {"code": "br", "names": ["breton"]},
    {"code": "bs", "names": ["bosnian"]},
    {"code": "ce", "names": ["chechen"]},
    {"code": "ch", "names": ["chamorro"]},
    {"code": "co", "names": ["corsican"]},
    {"code": "cr", "names": ["cree"]},
    {"code": "cu", "names": ["church slavic", "old slavonic"]},
    {"code": "cv", "names": ["chuvash"]},
    {"code": "dv", "names": ["divehi", "maldivian"]},
    {"code": "dz", "names": ["dzongkha"]},
    {"code": "ee", "names": ["ewe"]},
    {"code": "eo", "names": ["esperanto"]},
    {"code": "ff", "names": ["fulah", "fulfulde"]},
    {"code": "fj", "names": ["fijian"]},
    {"code": "fo", "names": ["faroese"]},
    {"code": "fy", "names": ["frisian", "western frisian"]},
    {"code": "gd", "names": ["scottish gaelic", "gaelic"]},
    {"code": "gn", "names": ["guarani"]},
    {"code": "gu", "names": ["gujarati"]},
    {"code": "gv", "names": ["manx"]},
    {"code": "ho", "names": ["hiri motu"]},
    {"code": "hy", "names": ["armenian"]},
    {"code": "hz", "names": ["herero"]},
    {"code": "ia", "names": ["interlingua"]},
    {"code": "ie", "names": ["interlingue"]},
    {"code": "ii", "names": ["sichuan yi", "yi"]},
    {"code": "ik", "names": ["inupiaq"]},
    {"code": "io", "names": ["ido"]},
    {"code": "iu", "names": ["inuktitut"]},
    {"code": "jv", "names": ["javanese"]},
    {"code": "ka", "names": ["georgian"]},
    {"code": "kg", "names": ["kongo"]},
    {"code": "ki", "names": ["kikuyu"]},
    {"code": "kj", "names": ["kuanyama"]},
    {"code": "kk", "names": ["kazakh"]},
    {"code": "kl", "names": ["kalaallisut", "greenlandic"]},
    {"code": "km", "names": ["khmer", "cambodian"]},
    {"code": "kn", "names": ["kannada"]},
    {"code": "kr", "names": ["kanuri"]},
    {"code": "ks", "names": ["kashmiri"]},
    {"code": "ku", "names": ["kurdish"]},
    {"code": "kv", "names": ["komi"]},
    {"code": "kw", "names": ["cornish"]},
    {"code": "ky", "names": ["kyrgyz", "kirghiz"]},
    {"code": "la", "names": ["latin"]},
    {"code": "lb", "names": ["luxembourgish", "letzeburgesch"]},
    {"code": "lg", "names": ["ganda", "luganda"]},
    {"code": "li", "names": ["limburgan", "limburgish"]},
    {"code": "ln", "names": ["lingala"]},
    {"code": "lo", "names": ["lao"]},
    {"code": "lu", "names": ["luba-katanga"]},
    {"code": "mg", "names": ["malagasy"]},
    {"code": "mh", "names": ["marshallese"]},
    {"code": "mi", "names": ["maori"]},
    {"code": "mk", "names": ["macedonian"]},
    {"code": "ml", "names": ["malayalam"]},
    {"code": "mn", "names": ["mongolian"]},
    {"code": "mr", "names": ["marathi"]},
    {"code": "my", "names": ["burmese", "myanmar"]},
    {"code": "na", "names": ["nauru"]},
    {"code": "nb", "names": ["norwegian bokmal", "bokmal"]},
    {"code": "nd", "names": ["north ndebele"]},
    {"code": "ne", "names": ["nepali"]},
    {"code": "ng", "names": ["ndonga"]},
    {"code": "nn", "names": ["norwegian nynorsk", "nynorsk"]},
    {"code": "nr", "names": ["south ndebele"]},
    {"code": "nv", "names": ["navajo"]},
    {"code": "ny", "names": ["chichewa", "nyanja"]},
    {"code": "oc", "names": ["occitan"]},
    {"code": "oj", "names": ["ojibwa", "ojibwe"]},
    {"code": "om", "names": ["oromo"]},
    {"code": "or", "names": ["odia", "oriya"]},
    {"code": "os", "names": ["ossetian"]},
    {"code": "pa", "names": ["punjabi", "panjabi"]},
    {"code": "pi", "names": ["pali"]},
    {"code": "ps", "names": ["pashto", "pushto"]},
    {"code": "qu", "names": ["quechua"]},
    {"code": "rm", "names": ["romansh"]},
    {"code": "rn", "names": ["rundi"]},
    {"code": "rw", "names": ["kinyarwanda"]},
    {"code": "sa", "names": ["sanskrit"]},
    {"code": "sc", "names": ["sardinian"]},
    {"code": "sd", "names": ["sindhi"]},
    {"code": "se", "names": ["northern sami"]},
    {"code": "sg", "names": ["sango"]},
    {"code": "si", "names": ["sinhala", "sinhalese"]},
    {"code": "sm", "names": ["samoan"]},
    {"code": "sn", "names": ["shona"]},
    {"code": "ss", "names": ["swati"]},
    {"code": "st", "names": ["southern sotho", "sotho"]},
    {"code": "su", "names": ["sundanese"]},
    {"code": "tg", "names": ["tajik"]},
    {"code": "ti", "names": ["tigrinya"]},
    {"code": "tk", "names": ["turkmen"]},
    {"code": "tn", "names": ["tswana"]},
    {"code": "to", "names": ["tonga"]},
    {"code": "ts", "names": ["tsonga"]},
    {"code": "tt", "names": ["tatar"]},
    {"code": "tw", "names": ["twi"]},
    {"code": "ty", "names": ["tahitian"]},
    {"code": "ug", "names": ["uighur", "uyghur"]},
    {"code": "uz", "names": ["uzbek"]},
    {"code": "ve", "names": ["venda"]},
    {"code": "vo", "names": ["volapuk"]},
    {"code": "wa", "names": ["walloon"]},
    {"code": "wo", "names": ["wolof"]},
    {"code": "yi", "names": ["yiddish"]},
    {"code": "za", "names": ["zhuang"]},
]

_LANGUAGE_LOOKUP = {}
for language in LANGUAGE_CATALOG:
    code = language.get("code")
    for name in language.get("names", []):
        normalized_name = _normalize_for_match(name)
        if not normalized_name:
            continue
        _LANGUAGE_LOOKUP[normalized_name] = code


def detect_hard_reject(raw_title, raw_text):
    prepared_title = _prepare_text(raw_title)
    prepared_text = _prepare_text(raw_text)

    for phrase in HARD_REJECT_TITLE_PATTERNS:
        if _phrase_in_text(prepared_title, phrase):
            return f"hard_reject_title:{phrase}"

    for phrase in HARD_REJECT_TEXT_PATTERNS:
        if _phrase_in_text(prepared_text, phrase):
            return f"hard_reject_text:{phrase}"

    if _phrase_in_text(prepared_text, "cold calling"):
        if _find_hits(prepared_text, HARD_REJECT_COLD_CALLING_CONTEXT):
            return "hard_reject_text:cold calling sales context"
    return ""


def detect_blocked_html(html_text):
    prepared = _prepare_text(html_text)
    if not prepared:
        return False

    strong_markers = [
        "captcha",
        "are you a robot",
        "access denied",
        "security check",
        "unusual traffic",
        "verify you are human",
    ]
    if any(_phrase_in_text(prepared, marker) for marker in strong_markers):
        return True

    weak_markers = [
        "blocked",
        "sign in to continue",
    ]
    weak_hits = sum(1 for marker in weak_markers if _phrase_in_text(prepared, marker))
    return weak_hits >= 2


def normalize_for_match(value):
    return _normalize_for_match(value)


def prepare_text(value):
    return _prepare_text(value)


def find_hits(prepared_text, phrases):
    return _find_hits(prepared_text, phrases)


def _phrase_spans(normalized_text, phrase):
    normalized_phrase = _normalize_for_match(phrase)
    if not normalized_phrase:
        return []
    parts = [re.escape(part) for part in normalized_phrase.split()]
    if not parts:
        return []
    pattern = r"\b" + r"\s+".join(parts) + r"\b"
    return [match.span() for match in re.finditer(pattern, normalized_text)]


def _phrase_token_positions(normalized_text, phrase):
    phrase_tokens = _normalize_for_match(phrase).split()
    text_tokens = normalized_text.split()
    if not phrase_tokens or not text_tokens:
        return []
    positions = []
    span_len = len(phrase_tokens)
    for idx in range(0, len(text_tokens) - span_len + 1):
        if text_tokens[idx : idx + span_len] == phrase_tokens:
            positions.append(idx)
    return positions


def _is_language_required_in_context(raw_text, language_phrase):
    raw_value = str(raw_text or "").lower()
    clauses = [segment.strip() for segment in re.split(r"[.;:!\n\r]+", raw_value) if segment.strip()]
    for clause in clauses:
        normalized_clause = _normalize_for_match(clause)
        prepared_clause = _prepare_text(normalized_clause)
        if not _phrase_in_text(prepared_clause, language_phrase):
            continue
        for marker in LANGUAGE_REQUIRED_MARKERS:
            if _phrase_in_text(prepared_clause, marker):
                return True
    return False


def detect_language_flags(raw_text):
    normalized_text = _normalize_for_match(raw_text)
    prepared_text = _prepare_text(normalized_text)

    extra_languages = set()
    required_languages = set()
    for normalized_name, code in _LANGUAGE_LOOKUP.items():
        if code in ALLOW_LANGUAGES:
            continue
        if _phrase_in_text(prepared_text, normalized_name):
            extra_languages.add(normalized_name)
            if _is_language_required_in_context(raw_text, normalized_name):
                required_languages.add(normalized_name)

    ordered_languages = sorted(extra_languages)
    ordered_required = sorted(required_languages)
    language_label = ", ".join(ordered_languages)
    required = bool(ordered_required)
    preferred = bool(ordered_languages) and not required
    notes = []
    if required:
        notes.append(f"Let op: vereist ook {', '.join(ordered_required)} (naast NL/EN).")
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
    bonus_hits = _find_hits(prepared_text, must_haves.get("bonus_signals") or [])
    total_positive_hits = len(title_hits_in_text.union(context_hits))

    score = (
        len(title_hits_in_title) * points.get("title_hit", 0)
        + len(context_hits) * points.get("context_hit", 0)
        + len(bonus_hits) * points.get("bonus_hit", 0)
        + len(negative_hits) * points.get("negative_hit", 0)
    )
    if len(title_hits_in_title) >= must_haves.get("min_title_hits", 0):
        score += points.get("title_gate_bonus", 0)
    if total_positive_hits >= must_haves.get("min_total_hits", 0):
        score += points.get("coverage_bonus", 0)
    score = max(0, min(cap, score))

    return score, {
        "reason": "ok",
        "title_hits": sorted(title_hits_in_title),
        "context_hits": sorted(context_hits),
        "negative_hits": sorted(negative_hits),
        "bonus_hits": sorted(bonus_hits),
        "title_hit_count": len(title_hits_in_title),
        "total_positive_hits": total_positive_hits,
        "min_title_hits": int(must_haves.get("min_title_hits", 0)),
        "min_total_hits": int(must_haves.get("min_total_hits", 0)),
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

