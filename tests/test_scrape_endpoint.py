import tempfile
import unittest
from pathlib import Path

import main


class TestScrapeEndpoint(unittest.TestCase):
    def setUp(self):
        self.client = main.app.test_client()
        main.source_health.clear()
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
                            "sleeve_scores": {"A": 5, "B": 0, "C": 0, "D": 0, "E": 0},
                            "primary_sleeve_id": "A",
                            "primary_sleeve_score": 5,
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

            response = self.client.get("/scrape?sleeve=A&target_raw=150")
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

    def test_fetch_jobs_enforces_mvp_direct_sources_only(self):
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
                sleeve_key="A",
                target_raw=10,
                allow_failover=True,
            )
            self.assertEqual(errors, [])
            self.assertEqual(used_sources, ["indeed_web", "linkedin_web"])
            self.assertEqual(seen_sources, ["indeed_web", "linkedin_web"])
            self.assertEqual(len(items), 1)
            self.assertEqual(diagnostics["auto_failover"], [])
        finally:
            main._fetch_source_with_cache = original_fetch_source

    def test_backend_enforces_both_mvp_sources_even_when_one_requested(self):
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
                sleeve_key="A",
                target_raw=10,
                allow_failover=True,
            )
            self.assertEqual(errors, [])
            self.assertEqual(used_sources, ["indeed_web", "linkedin_web"])
            self.assertEqual(seen_sources, ["indeed_web", "linkedin_web"])
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

            response = self.client.get("/scrape?sleeve=A&sources=serpapi")
            self.assertEqual(response.status_code, 200)
            self.assertFalse(captured["allow_failover"])
        finally:
            main.fetch_jobs_from_sources = original_fetch
            main.rank_and_filter_jobs = original_rank

    def test_mvp_profile_exposes_direct_sources(self):
        config = main._public_scrape_config()

        available_ids = [source["id"] for source in config["sources"] if source["available"]]
        self.assertEqual(available_ids, ["indeed_web", "linkedin_web"])
        self.assertEqual(config["profile"], "mvp")
        self.assertEqual([mode["id"] for mode in config["location_modes"]], ["nl_only"])

    def test_scrape_forces_nl_only_and_failover_off_in_mvp(self):
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
                "/scrape?sleeve=A&sources=indeed_web&location_mode=global&failover=1"
            )

            self.assertEqual(response.status_code, 200)
            self.assertFalse(captured["allow_failover"])
            self.assertEqual(captured["location_mode"], "nl_only")
        finally:
            main.fetch_jobs_from_sources = original_fetch
            main.rank_and_filter_jobs = original_rank

    def test_scrape_passes_query_terms(self):
        original_fetch = main.fetch_jobs_from_sources
        original_rank = main.rank_and_filter_jobs
        try:
            captured = {"query_terms": None}

            def fake_fetch(*args, **kwargs):
                captured["query_terms"] = kwargs.get("query_terms")
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
                "/scrape?sleeve=A&query_terms=festival+producer,artist+liaison,festival+producer"
            )

            self.assertEqual(response.status_code, 200)
            self.assertEqual(captured["query_terms"], ["festival producer", "artist liaison"])
        finally:
            main.fetch_jobs_from_sources = original_fetch
            main.rank_and_filter_jobs = original_rank

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

            response = self.client.get("/scrape?sleeve=A")
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
                "terms": ["festival producer"],
            },
        )
        self.assertEqual(response.status_code, 409)
        payload = response.get_json()
        self.assertIn("cannot be overwritten", payload.get("error", ""))

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
                        "terms": ["workflow operations", "delivery operations"],
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
                    custom_entries[0].get("terms"),
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
                    },
                )
                self.assertEqual(first_response.status_code, 200)
                first_payload = first_response.get_json()
                self.assertEqual((first_payload.get("saved") or {}).get("letter"), "E")

                second_response = self.client.post(
                    "/synergy-sleeves",
                    json={
                        "title": "Auto Custom Two",
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
                    },
                )
                self.assertEqual(duplicate_response.status_code, 409)
        finally:
            main.CUSTOM_SLEEVES_STATE_PATH = original_custom_state_path


if __name__ == "__main__":
    unittest.main()
