import unittest
from unittest.mock import patch

import main


class TestMainRanking(unittest.TestCase):
    def _job(self, company, snippet, title="AV Technician", location="Amsterdam, Netherlands"):
        return {
            "title": title,
            "company": company,
            "location": location,
            "snippet": snippet,
            "link": f"https://example.com/{company.lower()}",
            "date": "2026-02-01",
            "source": "test",
        }

    def test_abroad_signal_is_no_longer_a_hard_gate(self):
        jobs = [
            self._job(
                "NoAbroad",
                "Festival venue role with show control and live production.",
            )
        ]
        ranked = main.rank_and_filter_jobs(
            jobs,
            target_sleeve="A",
            min_target_score=3,
            location_mode="global",
            strict_sleeve=False,
        )
        self.assertEqual(len(ranked), 1)
        self.assertEqual(ranked[0]["decision"], "PASS")
        self.assertEqual(ranked[0]["abroad_score"], 0)

    def test_sales_soft_penalty_demotes_but_does_not_hide_fit(self):
        base_snippet = (
            "Remote hybrid AV role for festival venue events with travel and show control."
        )
        jobs = [
            self._job("NoPenalty", base_snippet),
            self._job("WithPenalty", base_snippet + " Includes SDR motions and cold calling."),
        ]
        ranked = main.rank_and_filter_jobs(
            jobs,
            target_sleeve="A",
            min_target_score=3,
            location_mode="global",
            strict_sleeve=False,
        )

        self.assertEqual(len(ranked), 2)
        self.assertEqual(ranked[0]["company"], "NoPenalty")
        self.assertEqual(ranked[1]["company"], "WithPenalty")

    def test_output_contains_sleeve_name_and_tagline(self):
        jobs = [
            self._job(
                "MetaCheck",
                "Remote AV festival role with travel and live events support.",
            )
        ]
        ranked = main.rank_and_filter_jobs(
            jobs,
            target_sleeve="A",
            min_target_score=3,
            location_mode="global",
            strict_sleeve=False,
        )

        self.assertEqual(len(ranked), 1)
        item = ranked[0]
        self.assertIn("primary_sleeve_name", item)
        self.assertIn("primary_sleeve_tagline", item)
        self.assertTrue(item["primary_sleeve_name"])
        self.assertTrue(item["primary_sleeve_tagline"])
        self.assertLessEqual(len(item.get("reasons") or []), 3)

    def test_dedupe_by_title_company_and_canonical_url(self):
        jobs = [
            {
                "title": "AV Technician",
                "company": "SameCo",
                "location": "Amsterdam, Netherlands",
                "snippet": "Remote hybrid festival role with travel.",
                "link": "https://example.com/jobs/1?utm_source=foo",
                "date": "2026-02-01",
                "source": "test",
            },
            {
                "title": "AV Technician",
                "company": "SameCo",
                "location": "Amsterdam Netherlands",
                "snippet": "Remote hybrid festival role with travel.",
                "link": "https://example.com/jobs/1?utm_source=bar",
                "date": "2026-02-01",
                "source": "test",
            },
        ]
        ranked = main.rank_and_filter_jobs(
            jobs,
            target_sleeve="A",
            min_target_score=3,
            location_mode="global",
            strict_sleeve=False,
        )
        self.assertEqual(len(ranked), 1)

    def test_output_contract_contains_decision_and_text_fields(self):
        jobs = [
            self._job(
                "ContractCheck",
                "Remote workflow automation role with integrations and business analysis.",
                title="Implementation Consultant",
            )
        ]
        ranked = main.rank_and_filter_jobs(
            jobs,
            target_sleeve="B",
            min_target_score=3,
            location_mode="global",
            strict_sleeve=False,
        )

        self.assertEqual(len(ranked), 1)
        item = ranked[0]
        self.assertIn("decision", item)
        self.assertIn(item["decision"], {"PASS", "MAYBE", "FAIL"})
        self.assertIn("raw_text", item)
        self.assertIn("prepared_text", item)
        self.assertIn("language_flags", item)
        self.assertIn("hard_reject_reason", item)

    def test_nl_only_filters_out_us_locations(self):
        jobs = [
            self._job(
                "USRole",
                "Remote workflow automation role.",
                title="Implementation Consultant",
                location="Denver, CO, United States",
            )
        ]
        ranked = main.rank_and_filter_jobs(
            jobs,
            target_sleeve="B",
            min_target_score=3,
            location_mode="nl_only",
            strict_sleeve=False,
        )
        self.assertEqual(ranked, [])

    def test_nl_only_uses_nl_market_settings_for_sources(self):
        self.assertEqual(
            main._indeed_search_url_for_mode("nl_only"),
            "https://nl.indeed.com/jobs",
        )
        self.assertEqual(main._linkedin_geo_id_for_mode("nl_only"), "102890719")

    def test_serpapi_nl_market_params_are_localized(self):
        params = main._serpapi_market_params_for_mode("nl_only")
        self.assertEqual(params.get("google_domain"), "google.nl")
        self.assertEqual(params.get("gl"), "nl")
        self.assertEqual(params.get("hl"), "nl")

    def test_extract_abroad_metadata_detects_percentage_and_geo(self):
        raw_text = (
            "Remote AV role with up to 40% international travel across EMEA, "
            "Germany and Spain."
        )
        meta = main._extract_abroad_metadata(raw_text)
        self.assertEqual(meta["percentage"], 40)
        self.assertEqual(meta["percentage_text"], "40%")
        self.assertIn("EMEA", meta["regions"])
        self.assertIn("Germany", meta["countries"])
        self.assertIn("Spain", meta["countries"])

    def test_extract_abroad_metadata_supports_ranges(self):
        raw_text = "Travel requirement 30-50% on client sites across Europe and North America."
        meta = main._extract_abroad_metadata(raw_text)
        self.assertEqual(meta["percentage"], 50)
        self.assertEqual(meta["percentage_text"], "30-50%")
        self.assertIn("Europe", meta["continents"])
        self.assertIn("North America", meta["continents"])

    def test_profile_defaults_to_mvp(self):
        with patch.dict(main.os.environ, {"SCRAPE_PROFILE": ""}, clear=False):
            self.assertEqual(main._active_scrape_profile(), "mvp")

    def test_full_profile_requires_explicit_opt_in(self):
        with patch.dict(
            main.os.environ,
            {"SCRAPE_PROFILE": "full", "SCRAPE_FULL_PROFILE_ENABLED": "0"},
            clear=False,
        ):
            self.assertEqual(main._active_scrape_profile(), "mvp")
        with patch.dict(
            main.os.environ,
            {"SCRAPE_PROFILE": "full", "SCRAPE_FULL_PROFILE_ENABLED": "1"},
            clear=False,
        ):
            self.assertEqual(main._active_scrape_profile(), "full")

    def test_canonicalize_relative_url_returns_empty(self):
        self.assertEqual(main._canonicalize_url("/rc/clk?jk=abc123"), "")

    def test_extract_indeed_links_from_detail_prefers_external_apply(self):
        html = """
        <html>
          <body>
            <a data-testid="apply-button" href="https://company.example/jobs/42">Apply now</a>
            <a href="https://nl.indeed.com/viewjob?jk=abc123">View on Indeed</a>
          </body>
        </html>
        """
        links = main._extract_indeed_links_from_detail(
            html,
            "https://nl.indeed.com/viewjob?jk=abc123",
        )
        self.assertEqual(links["indeed_url"], "https://nl.indeed.com/viewjob?jk=abc123")
        self.assertEqual(links["company_url"], "https://company.example/jobs/42")

    def test_extract_external_destination_from_redirect_url(self):
        redirect = (
            "https://nl.indeed.com/pagead/clk"
            "?jk=abc123&adurl=https%3A%2F%2Fcompany.example%2Fcareers%2F42"
        )
        self.assertEqual(
            main._extract_external_destination_from_url(redirect),
            "https://company.example/careers/42",
        )

    def test_ranking_preserves_raw_link_for_opening(self):
        jobs = [
            self._job(
                "LinkCheck",
                "Remote workflow automation role.",
                title="Implementation Consultant",
            )
        ]
        jobs[0]["link"] = "https://nl.indeed.com/viewjob?jk=abc123&utm_source=test"
        jobs[0]["source"] = "Indeed"
        ranked = main.rank_and_filter_jobs(
            jobs,
            target_sleeve="B",
            min_target_score=3,
            location_mode="global",
            strict_sleeve=False,
        )
        self.assertTrue(ranked)
        self.assertEqual(
            ranked[0]["url"],
            "https://nl.indeed.com/viewjob?jk=abc123&utm_source=test",
        )
        self.assertEqual(
            ranked[0]["indeed_url"],
            "https://nl.indeed.com/viewjob?jk=abc123&utm_source=test",
        )

    def test_company_url_never_uses_indeed_host(self):
        jobs = [
            self._job(
                "CompanyLinkCheck",
                "Remote workflow automation role.",
                title="Implementation Consultant",
            )
        ]
        jobs[0]["source"] = "Indeed"
        jobs[0]["indeed_url"] = "https://nl.indeed.com/viewjob?jk=abc123"
        jobs[0]["company_url"] = "https://nl.indeed.com/rc/clk?jk=abc123"
        ranked = main.rank_and_filter_jobs(
            jobs,
            target_sleeve="B",
            min_target_score=3,
            location_mode="global",
            strict_sleeve=False,
        )
        self.assertTrue(ranked)
        self.assertEqual(ranked[0]["company_url"], "")


if __name__ == "__main__":
    unittest.main()
