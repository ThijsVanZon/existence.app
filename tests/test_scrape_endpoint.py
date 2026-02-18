import tempfile
import time
import unittest
from pathlib import Path
from unittest.mock import patch

import main


class TestScrapeEndpoint(unittest.TestCase):
    def setUp(self):
        self.client = main.app.test_client()
        with main.source_health_lock:
            main.source_health.clear()
        with main.source_cache_lock:
            main.source_cache.clear()

    def test_summary_uses_total_pages_attempted_per_source(self):
        original_fetch = main.fetch_jobs_from_sources
        original_rank = main.rank_and_filter_jobs
        try:
            def fake_fetch(*args, **kwargs):
                items = [
                    {
                        "title": "AV Technician",
                        "company": "A",
                        "location": "Amsterdam, Netherlands",
                        "snippet": "Remote role",
                        "link": "https://example.com/a",
                        "source": "Indeed",
                    }
                ]
                diagnostics = main._new_diagnostics()
                diagnostics["source_query_summary"] = {
                    "Indeed|q1|Netherlands": {
                        "source": "Indeed",
                        "query": "q1",
                        "location": "Netherlands",
                        "pages_attempted": 2,
                        "raw_count": 10,
                        "parsed_count": 8,
                        "new_unique_count": 7,
                        "detailpages_fetched": 5,
                        "full_description_count": 3,
                        "error_count": 0,
                        "blocked_detected": False,
                    },
                    "Indeed|q2|Netherlands": {
                        "source": "Indeed",
                        "query": "q2",
                        "location": "Netherlands",
                        "pages_attempted": 2,
                        "raw_count": 10,
                        "parsed_count": 8,
                        "new_unique_count": 7,
                        "detailpages_fetched": 5,
                        "full_description_count": 3,
                        "error_count": 0,
                        "blocked_detected": False,
                    },
                    "LinkedIn|q1|Netherlands": {
                        "source": "LinkedIn",
                        "query": "q1",
                        "location": "Netherlands",
                        "pages_attempted": 3,
                        "raw_count": 12,
                        "parsed_count": 10,
                        "new_unique_count": 9,
                        "detailpages_fetched": 6,
                        "full_description_count": 4,
                        "error_count": 0,
                        "blocked_detected": False,
                    },
                }
                return items, [], ["indeed_web", "linkedin_web"], diagnostics

            def fake_rank(*args, **kwargs):
                return {
                    "jobs": [
                        {
                            "title": "AV Technician",
                            "company": "A",
                            "location": "Amsterdam, Netherlands",
                            "url": "https://example.com/a",
                            "source": "Indeed",
                            "decision": "PASS",
                            "language_flags": {},
                            "language_notes": [],
                            "reasons": [],
                            "hard_reject_reason": None,
                            "career_sleeve_scores": {"A": 5, "B": 0, "C": 0, "D": 0, "E": 0},
                            "primary_career_sleeve_id": "A",
                            "primary_career_sleeve_score": 5,
                            "abroad_score": 2,
                            "abroad_badges": [],
                            "raw_text": "",
                            "prepared_text": "",
                            "snippet": "",
                            "full_description": "",
                            "canonical_url_or_job_id": "https://example.com/a",
                        }
                    ],
                    "funnel": {
                        "raw": 20,
                        "after_dedupe": 15,
                        "pass_count": 1,
                        "maybe_count": 2,
                        "fail_count": 12,
                        "full_description_count": 5,
                        "full_description_coverage": 0.3333,
                        "top_fail_reasons": [],
                    },
                    "top_fail_reasons": [],
                    "fallbacks_applied": [],
                }

            main.fetch_jobs_from_sources = fake_fetch
            main.rank_and_filter_jobs = fake_rank

            response = self.client.get("/scrape?career_sleeve=A&target_raw=150")
            self.assertEqual(response.status_code, 200)
            payload = response.get_json()
            summary = payload["summary"]

            self.assertEqual(summary["targets"]["pages_attempted_per_source"]["Indeed"], 4)
            self.assertEqual(summary["targets"]["pages_attempted_per_source"]["LinkedIn"], 3)
            self.assertFalse(summary["targets"]["raw_or_pages_goal_met"])
            self.assertFalse(summary["kpi_gate_passed"])
            self.assertIn("config_version", summary)
        finally:
            main.fetch_jobs_from_sources = original_fetch
            main.rank_and_filter_jobs = original_rank

    def test_fetch_jobs_enforces_mvp_source_bundle_only(self):
        original_fetch_source = main._fetch_source_with_cache
        try:
            seen_sources = []

            def fake_fetch_source(source_key, *args, **kwargs):
                seen_sources.append(source_key)
                diagnostics = main._new_diagnostics()
                if source_key == "indeed_web":
                    return (
                        [
                            {
                                "title": "Dup",
                                "company": "Same",
                                "location": "Amsterdam",
                                "link": "https://example.com/dup",
                                "source": "Indeed",
                            }
                        ],
                        None,
                        diagnostics,
                    )
                return ([], None, diagnostics)

            main._fetch_source_with_cache = fake_fetch_source

            items, errors, used_sources, diagnostics = main.fetch_jobs_from_sources(
                ["indeed_web", "linkedin_web", "serpapi"],
                career_sleeve_key="A",
                target_raw=10,
                allow_failover=True,
            )
            self.assertEqual(errors, [])
            self.assertEqual(
                used_sources,
                ["indeed_web", "linkedin_web", "nl_web_openings"],
            )
            self.assertEqual(
                seen_sources,
                ["indeed_web", "linkedin_web", "nl_web_openings"],
            )
            self.assertEqual(len(items), 1)
            self.assertEqual(diagnostics["auto_failover"], [])
        finally:
            main._fetch_source_with_cache = original_fetch_source

    def test_backend_enforces_full_mvp_source_bundle_even_when_one_requested(self):
        original_fetch_source = main._fetch_source_with_cache
        try:
            seen_sources = []

            def fake_fetch_source(source_key, *args, **kwargs):
                seen_sources.append(source_key)
                diagnostics = main._new_diagnostics()
                if source_key == "linkedin_web":
                    return (
                        [
                            {
                                "title": "Dup",
                                "company": "Same",
                                "location": "Amsterdam",
                                "link": "https://example.com/dup",
                                "source": "Indeed",
                            }
                        ],
                        None,
                        diagnostics,
                    )
                return ([], None, diagnostics)

            main._fetch_source_with_cache = fake_fetch_source

            items, errors, used_sources, diagnostics = main.fetch_jobs_from_sources(
                ["linkedin_web"],
                career_sleeve_key="A",
                target_raw=10,
                allow_failover=True,
            )
            self.assertEqual(errors, [])
            self.assertEqual(
                used_sources,
                ["indeed_web", "linkedin_web", "nl_web_openings"],
            )
            self.assertEqual(
                seen_sources,
                ["indeed_web", "linkedin_web", "nl_web_openings"],
            )
            self.assertEqual(len(items), 1)
            self.assertEqual(diagnostics["auto_failover"], [])
        finally:
            main._fetch_source_with_cache = original_fetch_source

    def test_incremental_filter_skips_previously_seen_jobs(self):
        original_state_path = main.SEEN_JOBS_STATE_PATH
        try:
            with tempfile.TemporaryDirectory() as temp_dir:
                main.SEEN_JOBS_STATE_PATH = Path(temp_dir) / "seen_jobs_state.json"
                jobs = [
                    {
                        "title": "Job A",
                        "company": "Company A",
                        "location": "Amsterdam",
                        "link": "https://example.com/a",
                    },
                    {
                        "title": "Job B",
                        "company": "Company B",
                        "location": "Amsterdam",
                        "link": "https://example.com/b",
                    },
                ]

                first_run, first_skipped = main._apply_incremental_filter(jobs, window_days=14)
                second_run, second_skipped = main._apply_incremental_filter(jobs, window_days=14)

                self.assertEqual(len(first_run), 2)
                self.assertEqual(first_skipped, 0)
                self.assertEqual(len(second_run), 0)
                self.assertEqual(second_skipped, 2)
        finally:
            main.SEEN_JOBS_STATE_PATH = original_state_path

    def test_company_opening_redirects_direct_company_url(self):
        response = self.client.get(
            "/company-opening?company_url=https://careers.example.com/openings"
        )
        self.assertEqual(response.status_code, 302)
        self.assertEqual(
            response.headers.get("Location"),
            "https://careers.example.com/openings",
        )

    def test_company_opening_resolves_indeed_redirect_query_url(self):
        response = self.client.get(
            "/company-opening?indeed_url=https://nl.indeed.com/rc/clk?dest=https%3A%2F%2Fcareers.example.com%2Fjobs"
        )
        self.assertEqual(response.status_code, 302)
        self.assertEqual(
            response.headers.get("Location"),
            "https://careers.example.com/jobs",
        )

    def test_company_opening_resolves_linkedin_detail_page_to_company_url(self):
        class FakeResponse:
            ok = True
            status_code = 200
            text = (
                "<html><body>"
                "<a href='https://careers.example.com/jobs/apply'>"
                "Apply on company website"
                "</a>"
                "</body></html>"
            )
            url = "https://www.linkedin.com/jobs/view/123"
            headers = {}

        with patch("main.requests.get", return_value=FakeResponse()) as mocked_get:
            response = self.client.get(
                "/company-opening?linkedin_url=https://www.linkedin.com/jobs/view/123"
            )

        self.assertEqual(response.status_code, 302)
        self.assertEqual(
            response.headers.get("Location"),
            "https://careers.example.com/jobs/apply",
        )
        mocked_get.assert_called_once()

    def test_company_opening_rejects_userinfo_host_confusion(self):
        with patch("main.requests.get") as mocked_get:
            response = self.client.get(
                "/company-opening?linkedin_url=https://linkedin.com@127.0.0.1/private&format=json"
            )
        self.assertEqual(response.status_code, 400)
        payload = response.get_json() or {}
        self.assertEqual(payload.get("code"), "company_opening_unresolved")
        self.assertIn("Missing usable URL inputs", payload.get("error", ""))
        mocked_get.assert_not_called()

    def test_fetch_source_stale_cache_fallback_is_age_limited(self):
        source_key = "indeed_web"
        original_fetcher = main.SOURCE_REGISTRY[source_key]["fetcher"]
        original_max_stale = main.MAX_STALE_CACHE_FALLBACK_SECONDS
        try:
            def seed_fetcher(*args, **kwargs):
                diagnostics = kwargs.get("diagnostics") or main._new_diagnostics()
                diagnostics["source_query_summary"] = {
                    "Indeed|seed|Netherlands": {
                        "source": "Indeed",
                        "query": "seed",
                        "location": "Netherlands",
                        "pages_attempted": 1,
                        "raw_count": 1,
                        "parsed_count": 1,
                        "new_unique_count": 1,
                        "detailpages_fetched": 0,
                        "full_description_count": 0,
                        "error_count": 0,
                        "blocked_detected": False,
                    }
                }
                return (
                    [
                        {
                            "title": "Seed",
                            "company": "Cache Co",
                            "location": "Amsterdam",
                            "link": "https://example.com/seed",
                            "source": "Indeed",
                        }
                    ],
                    diagnostics,
                )

            main.SOURCE_REGISTRY[source_key]["fetcher"] = seed_fetcher
            main.MAX_STALE_CACHE_FALLBACK_SECONDS = 30

            seed_items, seed_error, _ = main._fetch_source_with_cache(
                source_key,
                career_sleeve_key="A",
                location_mode=main.MVP_LOCATION_MODE,
                force_refresh=True,
                max_pages=1,
                target_raw=10,
                no_new_unique_pages=1,
            )
            self.assertEqual(len(seed_items), 1)
            self.assertIsNone(seed_error)

            cache_key = main._cache_key_for(
                source_key,
                "A",
                main.MVP_LOCATION_MODE,
                1,
                10,
                1,
                search_queries=None,
                extra_queries=None,
            )
            with main.source_cache_lock:
                main.source_cache[cache_key]["fetched_at"] = time.time() - 120

            def failing_fetcher(*args, **kwargs):
                raise RuntimeError("fetch_failed")

            main.SOURCE_REGISTRY[source_key]["fetcher"] = failing_fetcher

            items, error, _ = main._fetch_source_with_cache(
                source_key,
                career_sleeve_key="A",
                location_mode=main.MVP_LOCATION_MODE,
                force_refresh=True,
                max_pages=1,
                target_raw=10,
                no_new_unique_pages=1,
            )
            self.assertEqual(items, [])
            self.assertIn("fetch_failed", str(error))
        finally:
            main.SOURCE_REGISTRY[source_key]["fetcher"] = original_fetcher
            main.MAX_STALE_CACHE_FALLBACK_SECONDS = original_max_stale

    def test_fetch_source_recent_stale_cache_fallback_updates_health(self):
        source_key = "indeed_web"
        original_fetcher = main.SOURCE_REGISTRY[source_key]["fetcher"]
        original_max_stale = main.MAX_STALE_CACHE_FALLBACK_SECONDS
        try:
            def seed_fetcher(*args, **kwargs):
                diagnostics = kwargs.get("diagnostics") or main._new_diagnostics()
                diagnostics["source_query_summary"] = {
                    "Indeed|seed|Netherlands": {
                        "source": "Indeed",
                        "query": "seed",
                        "location": "Netherlands",
                        "pages_attempted": 1,
                        "raw_count": 1,
                        "parsed_count": 1,
                        "new_unique_count": 1,
                        "detailpages_fetched": 0,
                        "full_description_count": 0,
                        "error_count": 0,
                        "blocked_detected": False,
                    }
                }
                return (
                    [
                        {
                            "title": "Seed",
                            "company": "Cache Co",
                            "location": "Amsterdam",
                            "link": "https://example.com/seed",
                            "source": "Indeed",
                        }
                    ],
                    diagnostics,
                )

            main.SOURCE_REGISTRY[source_key]["fetcher"] = seed_fetcher
            main.MAX_STALE_CACHE_FALLBACK_SECONDS = 300

            seed_items, seed_error, _ = main._fetch_source_with_cache(
                source_key,
                career_sleeve_key="A",
                location_mode=main.MVP_LOCATION_MODE,
                force_refresh=True,
                max_pages=1,
                target_raw=10,
                no_new_unique_pages=1,
            )
            self.assertEqual(len(seed_items), 1)
            self.assertIsNone(seed_error)

            cache_key = main._cache_key_for(
                source_key,
                "A",
                main.MVP_LOCATION_MODE,
                1,
                10,
                1,
                search_queries=None,
                extra_queries=None,
            )
            with main.source_cache_lock:
                main.source_cache[cache_key]["fetched_at"] = time.time() - 5

            def failing_fetcher(*args, **kwargs):
                raise RuntimeError("fetch_failed")

            main.SOURCE_REGISTRY[source_key]["fetcher"] = failing_fetcher

            items, error, _ = main._fetch_source_with_cache(
                source_key,
                career_sleeve_key="A",
                location_mode=main.MVP_LOCATION_MODE,
                force_refresh=True,
                max_pages=1,
                target_raw=10,
                no_new_unique_pages=1,
            )
            self.assertEqual(len(items), 1)
            self.assertIsNone(error)
            status = main._source_health_status(source_key)
            self.assertGreaterEqual(int(status.get("failure_streak", 0)), 1)
            self.assertIn("fetch_failed", status.get("last_error", ""))
        finally:
            main.SOURCE_REGISTRY[source_key]["fetcher"] = original_fetcher
            main.MAX_STALE_CACHE_FALLBACK_SECONDS = original_max_stale

    def test_scrape_defaults_failover_off_when_sources_are_explicit(self):
        original_fetch = main.fetch_jobs_from_sources
        original_rank = main.rank_and_filter_jobs
        try:
            captured = {"allow_failover": None}

            def fake_fetch(*args, **kwargs):
                captured["allow_failover"] = kwargs.get("allow_failover")
                return [], [], ["serpapi"], main._new_diagnostics()

            def fake_rank(*args, **kwargs):
                return {
                    "jobs": [],
                    "funnel": {
                        "raw": 0,
                        "after_dedupe": 0,
                        "pass_count": 0,
                        "maybe_count": 0,
                        "fail_count": 0,
                        "full_description_count": 0,
                        "full_description_coverage": 0.0,
                        "top_fail_reasons": [],
                    },
                    "top_fail_reasons": [],
                    "fallbacks_applied": [],
                }

            main.fetch_jobs_from_sources = fake_fetch
            main.rank_and_filter_jobs = fake_rank

            response = self.client.get("/scrape?career_sleeve=A&sources=serpapi")
            self.assertEqual(response.status_code, 200)
            self.assertFalse(captured["allow_failover"])
        finally:
            main.fetch_jobs_from_sources = original_fetch
            main.rank_and_filter_jobs = original_rank

    def test_mvp_profile_exposes_mvp_source_bundle(self):
        config = main._public_scrape_config()

        available_ids = [source["id"] for source in config["sources"] if source["available"]]
        self.assertEqual(
            available_ids,
            ["indeed_web", "linkedin_web", "nl_web_openings"],
        )
        self.assertEqual(config["profile"], "mvp")
        self.assertEqual([mode["id"] for mode in config["location_modes"]], ["nl_vn"])

    def test_scrape_forces_nl_vn_and_failover_off_in_mvp(self):
        original_fetch = main.fetch_jobs_from_sources
        original_rank = main.rank_and_filter_jobs
        try:
            captured = {"allow_failover": None, "location_mode": None}

            def fake_fetch(*args, **kwargs):
                captured["allow_failover"] = kwargs.get("allow_failover")
                captured["location_mode"] = kwargs.get("location_mode")
                return [], [], ["indeed_web"], main._new_diagnostics()

            def fake_rank(*args, **kwargs):
                return {
                    "jobs": [],
                    "funnel": {
                        "raw": 0,
                        "after_dedupe": 0,
                        "pass_count": 0,
                        "maybe_count": 0,
                        "fail_count": 0,
                        "full_description_count": 0,
                        "full_description_coverage": 0.0,
                        "top_fail_reasons": [],
                    },
                    "top_fail_reasons": [],
                    "fallbacks_applied": [],
                }

            main.fetch_jobs_from_sources = fake_fetch
            main.rank_and_filter_jobs = fake_rank

            response = self.client.get(
                "/scrape?career_sleeve=A&sources=indeed_web&location_mode=global&failover=1"
            )

            self.assertEqual(response.status_code, 200)
            self.assertFalse(captured["allow_failover"])
            self.assertEqual(captured["location_mode"], "nl_vn")
        finally:
            main.fetch_jobs_from_sources = original_fetch
            main.rank_and_filter_jobs = original_rank

    def test_scrape_default_variant_uses_parallel_mvp_bundle(self):
        original_fetch = main.fetch_jobs_from_sources
        original_rank = main.rank_and_filter_jobs
        try:
            captured = {
                "selected_sources": None,
                "enforce_mvp_bundle": None,
                "parallel_fetch": None,
            }

            def fake_fetch(*args, **kwargs):
                captured["selected_sources"] = list(args[0]) if args else []
                captured["enforce_mvp_bundle"] = kwargs.get("enforce_mvp_bundle")
                captured["parallel_fetch"] = kwargs.get("parallel_fetch")
                return [], [], ["indeed_web", "linkedin_web", "nl_web_openings"], main._new_diagnostics()

            def fake_rank(*args, **kwargs):
                return {
                    "jobs": [],
                    "funnel": {
                        "raw": 0,
                        "after_dedupe": 0,
                        "pass_count": 0,
                        "maybe_count": 0,
                        "fail_count": 0,
                        "full_description_count": 0,
                        "full_description_coverage": 0.0,
                        "top_fail_reasons": [],
                    },
                    "top_fail_reasons": [],
                    "fallbacks_applied": [],
                }

            main.fetch_jobs_from_sources = fake_fetch
            main.rank_and_filter_jobs = fake_rank

            response = self.client.get("/scrape?career_sleeve=A")
            self.assertEqual(response.status_code, 200)
            payload = response.get_json() or {}
            summary = payload.get("summary") or {}
            self.assertEqual(summary.get("scrape_variant"), "default")
            self.assertEqual(
                captured["selected_sources"],
                ["indeed_web", "linkedin_web", "nl_web_openings"],
            )
            self.assertTrue(captured["enforce_mvp_bundle"])
            self.assertTrue(captured["parallel_fetch"])
        finally:
            main.fetch_jobs_from_sources = original_fetch
            main.rank_and_filter_jobs = original_rank

    def test_scrape_ultra_fast_variant_uses_linkedin_only(self):
        original_fetch = main.fetch_jobs_from_sources
        original_rank = main.rank_and_filter_jobs
        try:
            captured = {
                "selected_sources": None,
                "enforce_mvp_bundle": None,
                "parallel_fetch": None,
                "max_pages": None,
                "target_raw": None,
            }

            def fake_fetch(*args, **kwargs):
                captured["selected_sources"] = list(args[0]) if args else []
                captured["enforce_mvp_bundle"] = kwargs.get("enforce_mvp_bundle")
                captured["parallel_fetch"] = kwargs.get("parallel_fetch")
                captured["max_pages"] = kwargs.get("max_pages")
                captured["target_raw"] = kwargs.get("target_raw")
                return [], [], ["linkedin_web"], main._new_diagnostics()

            def fake_rank(*args, **kwargs):
                return {
                    "jobs": [],
                    "funnel": {
                        "raw": 0,
                        "after_dedupe": 0,
                        "pass_count": 0,
                        "maybe_count": 0,
                        "fail_count": 0,
                        "full_description_count": 0,
                        "full_description_coverage": 0.0,
                        "top_fail_reasons": [],
                    },
                    "top_fail_reasons": [],
                    "fallbacks_applied": [],
                }

            main.fetch_jobs_from_sources = fake_fetch
            main.rank_and_filter_jobs = fake_rank

            response = self.client.get("/scrape?career_sleeve=A&scrape_variant=ultra_fast&max_pages=8&target_raw=200")
            self.assertEqual(response.status_code, 200)
            payload = response.get_json() or {}
            summary = payload.get("summary") or {}
            self.assertEqual(summary.get("scrape_variant"), "ultra_fast")
            self.assertEqual(captured["selected_sources"], ["linkedin_web"])
            self.assertFalse(captured["enforce_mvp_bundle"])
            self.assertFalse(captured["parallel_fetch"])
            self.assertLessEqual(int(captured["max_pages"]), 2)
            self.assertLessEqual(int(captured["target_raw"]), 90)
        finally:
            main.fetch_jobs_from_sources = original_fetch
            main.rank_and_filter_jobs = original_rank

    def test_scrape_passes_search_queries(self):
        original_fetch = main.fetch_jobs_from_sources
        original_rank = main.rank_and_filter_jobs
        try:
            captured = {"search_queries": None}

            def fake_fetch(*args, **kwargs):
                captured["search_queries"] = kwargs.get("search_queries")
                return [], [], ["indeed_web"], main._new_diagnostics()

            def fake_rank(*args, **kwargs):
                return {
                    "jobs": [],
                    "funnel": {
                        "raw": 0,
                        "after_dedupe": 0,
                        "pass_count": 0,
                        "maybe_count": 0,
                        "fail_count": 0,
                        "full_description_count": 0,
                        "full_description_coverage": 0.0,
                        "top_fail_reasons": [],
                    },
                    "top_fail_reasons": [],
                    "fallbacks_applied": [],
                }

            main.fetch_jobs_from_sources = fake_fetch
            main.rank_and_filter_jobs = fake_rank

            response = self.client.get(
                "/scrape?career_sleeve=A&search_queries=festival+producer,artist+liaison,festival+producer"
            )

            self.assertEqual(response.status_code, 200)
            self.assertEqual(captured["search_queries"], ["festival producer", "artist liaison"])
        finally:
            main.fetch_jobs_from_sources = original_fetch
            main.rank_and_filter_jobs = original_rank

    def test_scrape_rejects_custom_mode_without_queries(self):
        response = self.client.get("/scrape?career_sleeve=E&custom_mode=1")
        self.assertEqual(response.status_code, 400)
        payload = response.get_json() or {}
        self.assertIn("at least one search query", payload.get("error", "").lower())

    def test_scrape_returns_200_with_source_errors_when_all_sources_fail(self):
        original_fetch = main.fetch_jobs_from_sources
        original_rank = main.rank_and_filter_jobs
        try:
            def fake_fetch(*args, **kwargs):
                return [], ["indeed_web: blocked_detected"], ["indeed_web"], main._new_diagnostics()

            def fake_rank(*args, **kwargs):
                return {
                    "jobs": [],
                    "funnel": {
                        "raw": 0,
                        "after_dedupe": 0,
                        "pass_count": 0,
                        "maybe_count": 0,
                        "fail_count": 0,
                        "full_description_count": 0,
                        "full_description_coverage": 0.0,
                        "top_fail_reasons": [],
                    },
                    "top_fail_reasons": [],
                    "fallbacks_applied": [],
                }

            main.fetch_jobs_from_sources = fake_fetch
            main.rank_and_filter_jobs = fake_rank

            response = self.client.get("/scrape?career_sleeve=A")
            self.assertEqual(response.status_code, 200)
            payload = response.get_json()
            self.assertIn("summary", payload)
            self.assertEqual(
                payload["summary"].get("source_errors"),
                ["indeed_web: blocked_detected"],
            )
        finally:
            main.fetch_jobs_from_sources = original_fetch
            main.rank_and_filter_jobs = original_rank

    def test_synergy_sleeves_exposes_fixed_a_to_d(self):
        response = self.client.get("/synergy-sleeves")
        self.assertEqual(response.status_code, 200)
        payload = response.get_json()
        fixed_letters = [entry.get("letter") for entry in payload.get("fixed", [])]
        self.assertEqual(fixed_letters, ["A", "B", "C", "D"])

    def test_synergy_sleeves_rejects_overwriting_fixed_letters(self):
        response = self.client.post(
            "/synergy-sleeves",
            json={
                "letter": "A",
                "title": "Attempt overwrite",
                "queries": ["festival producer"],
            },
        )
        self.assertEqual(response.status_code, 409)
        payload = response.get_json()
        self.assertIn("cannot be overwritten", payload.get("error", ""))

    def test_synergy_sleeves_requires_minimum_one_query(self):
        response = self.client.post(
            "/synergy-sleeves",
            json={
                "title": "Custom Without Queries",
                "queries": [],
            },
        )
        self.assertEqual(response.status_code, 400)
        payload = response.get_json()
        self.assertIn("At least one search query", payload.get("error", ""))

    def test_synergy_sleeves_saves_and_deletes_custom_records(self):
        original_custom_state_path = main.CUSTOM_SLEEVES_STATE_PATH
        try:
            with tempfile.TemporaryDirectory() as temp_dir:
                main.CUSTOM_SLEEVES_STATE_PATH = Path(temp_dir) / "custom_sleeves_state.json"

                create_response = self.client.post(
                    "/synergy-sleeves",
                    json={
                        "letter": "E",
                        "title": "Custom Pipeline Sleeve",
                        "queries": ["workflow operations", "delivery operations"],
                    },
                )
                self.assertEqual(create_response.status_code, 200)
                created_payload = create_response.get_json()
                self.assertTrue(created_payload.get("ok"))

                fetch_response = self.client.get("/synergy-sleeves")
                self.assertEqual(fetch_response.status_code, 200)
                fetch_payload = fetch_response.get_json()
                custom_entries = fetch_payload.get("custom") or []
                self.assertEqual(len(custom_entries), 1)
                self.assertEqual(custom_entries[0].get("letter"), "E")
                self.assertEqual(custom_entries[0].get("title"), "Custom Pipeline Sleeve")
                self.assertEqual(
                    custom_entries[0].get("queries"),
                    ["workflow operations", "delivery operations"],
                )

                delete_response = self.client.delete("/synergy-sleeves/E")
                self.assertEqual(delete_response.status_code, 200)
                delete_payload = delete_response.get_json()
                self.assertTrue(delete_payload.get("ok"))

                fetch_after_delete = self.client.get("/synergy-sleeves")
                self.assertEqual(fetch_after_delete.status_code, 200)
                after_payload = fetch_after_delete.get_json()
                self.assertEqual(after_payload.get("custom"), [])
        finally:
            main.CUSTOM_SLEEVES_STATE_PATH = original_custom_state_path

    def test_synergy_sleeves_auto_assigns_next_custom_letter(self):
        original_custom_state_path = main.CUSTOM_SLEEVES_STATE_PATH
        try:
            with tempfile.TemporaryDirectory() as temp_dir:
                main.CUSTOM_SLEEVES_STATE_PATH = Path(temp_dir) / "custom_sleeves_state.json"

                first_response = self.client.post(
                    "/synergy-sleeves",
                    json={
                        "title": "Auto Custom One",
                        "queries": ["custom one term"],
                    },
                )
                self.assertEqual(first_response.status_code, 200)
                first_payload = first_response.get_json()
                self.assertEqual((first_payload.get("saved") or {}).get("letter"), "E")

                second_response = self.client.post(
                    "/synergy-sleeves",
                    json={
                        "title": "Auto Custom Two",
                        "queries": ["custom two term"],
                    },
                )
                self.assertEqual(second_response.status_code, 200)
                second_payload = second_response.get_json()
                self.assertEqual((second_payload.get("saved") or {}).get("letter"), "F")

                duplicate_response = self.client.post(
                    "/synergy-sleeves",
                    json={
                        "letter": "E",
                        "title": "Should Not Overwrite",
                        "queries": ["duplicate custom term"],
                    },
                )
                self.assertEqual(duplicate_response.status_code, 409)
        finally:
            main.CUSTOM_SLEEVES_STATE_PATH = original_custom_state_path


if __name__ == "__main__":
    unittest.main()



