from __future__ import annotations

import json
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any
from urllib.parse import urlparse

from ..config import AppConfig
from ..paths import ProjectPaths

DEFAULT_RULES = {
    "brand_terms": [
        "international surrogacy",
        "international-surrogacy",
        "international surrogacy center",
        "international-surrogacy.com",
    ],
    "intent_rules": {
        "lead_intent": [
            "contact",
            "consultation",
            "consult",
            "apply",
            "book",
            "appointment",
            "start now",
            "get started",
            "agency",
            "clinic",
        ],
        "commercial": [
            "cost",
            "price",
            "pricing",
            "best",
            "top",
            "compare",
            "comparison",
            "vs",
            "review",
            "reviews",
            "package",
            "packages",
            "service",
            "services",
            "program",
            "programs",
        ],
        "informational": [
            "what",
            "how",
            "when",
            "why",
            "who",
            "guide",
            "meaning",
            "law",
            "laws",
            "requirements",
            "process",
            "faq",
            "risks",
            "benefits",
            "difference",
        ],
        "navigational": [
            "official",
            "website",
            "site",
            "homepage",
            "about",
            "blog",
            "login",
        ],
    },
    "page_segments": {
        "blog_prefixes": ["/blog/"],
        "commercial_exact": ["/", "/contact", "/pricing", "/services", "/apply"],
        "commercial_prefixes": [
            "/contact",
            "/pricing",
            "/service",
            "/services",
            "/program",
            "/programs",
            "/apply",
            "/consultation",
            "/surrogacy",
            "/egg-donation",
            "/donor",
            "/agency",
        ],
    },
}

RULES_FILENAME = "seo_rules.json"


def _string(value: Any) -> str:
    return str(value or "").strip()


def _normalized_text(value: str) -> str:
    return " ".join(_string(value).lower().replace("-", " ").replace("_", " ").split())


def _contains_phrase(text: str, phrase: str) -> bool:
    return phrase in text if phrase else False


def _first_path_segment(path: str) -> str:
    parts = [part for part in _string(path).split("/") if part]
    return parts[0] if parts else "root"


@dataclass(slots=True)
class InterpretationService:
    config: AppConfig
    paths: ProjectPaths
    logger: Any
    rules_file: Path | None = None
    _rules_cache: dict[str, Any] | None = field(init=False, default=None, repr=False)

    def __post_init__(self) -> None:
        if self.rules_file is None:
            self.rules_file = self.paths.config_dir / RULES_FILENAME

    def load_rules(self) -> dict[str, Any]:
        if self._rules_cache is not None:
            return self._rules_cache

        if self.rules_file and self.rules_file.exists():
            try:
                payload = json.loads(self.rules_file.read_text(encoding="utf-8"))
                if isinstance(payload, dict):
                    self._rules_cache = payload
                    return payload
            except Exception as exc:  # pragma: no cover - defensive guard
                self.logger.warning("Failed to read interpretation rules from %s: %s", self.rules_file, exc)

        self.logger.warning("Using built-in interpretation defaults because %s is missing or invalid.", self.rules_file)
        self._rules_cache = DEFAULT_RULES
        return self._rules_cache

    def classify_query_brand(self, query: str) -> dict[str, Any]:
        text = _normalized_text(query)
        if not text:
            return {
                "query_brand_classification": "mixed",
                "query_brand_confidence": 0.0,
                "query_brand_matches": [],
                "query_brand_source": "empty_query",
            }

        brand_terms = [
            _normalized_text(term)
            for term in self.load_rules().get("brand_terms", [])
            if _normalized_text(term)
        ]
        matches = [term for term in brand_terms if _contains_phrase(text, term)]

        if not matches:
            return {
                "query_brand_classification": "non_brand",
                "query_brand_confidence": 0.9,
                "query_brand_matches": [],
                "query_brand_source": "rule_absence",
            }

        exact = any(text == term for term in matches)
        text_tokens = text.split()
        if exact or len(text_tokens) <= len(matches[0].split()) + 1:
            classification = "brand"
            confidence = 0.97 if exact else 0.88
        else:
            classification = "mixed"
            confidence = 0.62

        return {
            "query_brand_classification": classification,
            "query_brand_confidence": confidence,
            "query_brand_matches": matches,
            "query_brand_source": "brand_terms",
        }

    def classify_query_intent(self, query: str, brand_classification: str) -> dict[str, Any]:
        text = _normalized_text(query)
        rules = self.load_rules().get("intent_rules", {})
        matches = {
            intent: [
                keyword
                for keyword in (_string(value) for value in rules.get(intent, []))
                if keyword and _contains_phrase(text, _normalized_text(keyword))
            ]
            for intent in ("lead_intent", "commercial", "informational", "navigational")
        }

        if brand_classification in {"brand", "mixed"} and not any(matches.values()):
            return {
                "query_intent": "navigational",
                "query_intent_confidence": 0.7,
                "query_intent_matches": [],
                "query_intent_source": "brand_fallback",
            }

        if brand_classification == "brand" and len(text.split()) <= 3:
            return {
                "query_intent": "navigational",
                "query_intent_confidence": 0.9,
                "query_intent_matches": [],
                "query_intent_source": "brand_exact",
            }

        for intent in ("lead_intent", "commercial", "informational", "navigational"):
            if matches[intent]:
                confidence = 0.88 if len(matches[intent]) == 1 else 0.95
                return {
                    "query_intent": intent,
                    "query_intent_confidence": confidence,
                    "query_intent_matches": matches[intent],
                    "query_intent_source": "keyword_rules",
                }

        return {
            "query_intent": "unknown",
            "query_intent_confidence": 0.35,
            "query_intent_matches": [],
            "query_intent_source": "no_rule_match",
        }

    def classify_page_segment(self, url_or_path: str) -> dict[str, Any]:
        raw = _string(url_or_path)
        parsed = urlparse(raw) if raw.startswith(("http://", "https://")) else None
        path = _string(parsed.path if parsed else raw) or "/"

        rules = self.load_rules().get("page_segments", {})
        blog_prefixes = [_string(value) for value in rules.get("blog_prefixes", []) if _string(value)]
        commercial_exact = {_string(value) for value in rules.get("commercial_exact", []) if _string(value)}
        commercial_prefixes = [_string(value) for value in rules.get("commercial_prefixes", []) if _string(value)]

        if any(path == prefix.rstrip("/") or path.startswith(prefix) for prefix in blog_prefixes):
            segment = "blog"
            confidence = 0.98
            rule = "blog_prefix"
        elif path in commercial_exact or any(path.startswith(prefix) for prefix in commercial_prefixes):
            segment = "commercial"
            confidence = 0.92
            rule = "commercial_rule"
        else:
            segment = "other"
            confidence = 0.6
            rule = "fallback_other"

        return {
            "page_segment": segment,
            "page_segment_confidence": confidence,
            "page_segment_source": rule,
            "page_directory_group": _first_path_segment(path),
        }

    def enrich_query_rows(self, rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
        enriched: list[dict[str, Any]] = []
        for row in rows:
            query = _string(row.get("query"))
            brand_data = self.classify_query_brand(query)
            intent_data = self.classify_query_intent(query, brand_data["query_brand_classification"])
            enriched.append({**row, **brand_data, **intent_data})
        return enriched

    def enrich_page_rows(self, rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
        enriched: list[dict[str, Any]] = []
        for row in rows:
            page_hint = _string(row.get("normalized_page_url") or row.get("normalized_page_path") or row.get("page_original_gsc") or row.get("page_original_ga4"))
            segment_data = self.classify_page_segment(page_hint)
            enriched.append({**row, **segment_data})
        return enriched

    def attribute_map_by_path(self, rows: list[dict[str, Any]]) -> dict[str, dict[str, Any]]:
        mapping: dict[str, dict[str, Any]] = {}
        for row in rows:
            key = _string(row.get("normalized_page_path"))
            if not key:
                continue
            mapping[key] = {
                "page_segment": _string(row.get("page_segment")) or "other",
                "page_segment_confidence": row.get("page_segment_confidence", 0.0),
                "page_segment_source": _string(row.get("page_segment_source")),
                "page_directory_group": _string(row.get("page_directory_group")) or "root",
            }
        return mapping

    def enrich_rows_with_page_attributes(
        self,
        rows: list[dict[str, Any]],
        page_attribute_map: dict[str, dict[str, Any]],
    ) -> list[dict[str, Any]]:
        enriched: list[dict[str, Any]] = []
        for row in rows:
            key = _string(row.get("normalized_page_path"))
            page_attributes = page_attribute_map.get(key, {})
            enriched.append(
                {
                    **row,
                    "page_segment": _string(page_attributes.get("page_segment")) or "other",
                    "page_segment_confidence": page_attributes.get("page_segment_confidence", 0.0),
                    "page_segment_source": _string(page_attributes.get("page_segment_source")) or "fallback_other",
                    "page_directory_group": _string(page_attributes.get("page_directory_group")) or "root",
                }
            )
        return enriched
