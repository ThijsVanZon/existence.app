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

    def test_location_proximity_extracts_distance_from_den_bosch_anchor(self):
        profile = main._score_location_proximity("Amsterdam, Netherlands")
        self.assertIsNotNone(profile["distance_km"])
        self.assertGreater(profile["distance_km"], 0)
        self.assertEqual(profile["anchor"], "Home")

    def test_ranking_prefers_jobs_closer_to_den_bosch_when_fit_is_equal(self):
        snippet = "Event operations role with festival delivery and stakeholder coordination."
        jobs = [
            self._job("Near", snippet, location="Den Bosch, Netherlands"),
            self._job("Far", snippet, location="Groningen, Netherlands"),
        ]
        ranked = main.rank_and_filter_jobs(
            jobs,
            target_sleeve="A",
            min_target_score=3,
            location_mode="nl_only",
            strict_sleeve=False,
        )
        self.assertEqual(len(ranked), 2)
        self.assertEqual(ranked[0]["company"], "Near")
        self.assertLess(
            float(ranked[0].get("distance_from_home_km") or 0),
            float(ranked[1].get("distance_from_home_km") or 999),
        )

    def test_output_contract_contains_location_proximity_fields(self):
        jobs = [
            self._job(
                "LocationMeta",
                "Workflow automation operations role with hybrid setup.",
                title="Implementation Consultant",
                location="Utrecht, Netherlands",
            )
        ]
        ranked = main.rank_and_filter_jobs(
            jobs,
            target_sleeve="B",
            min_target_score=3,
            location_mode="nl_only",
            strict_sleeve=False,
        )
        self.assertEqual(len(ranked), 1)
        item = ranked[0]
        self.assertIn("main_location", item)
        self.assertIn("distance_from_home_km", item)
        self.assertIn("distance_anchor", item)
        self.assertIn("proximity_score", item)

    def test_enhanced_abroad_score_includes_percentage_and_geo_scope(self):
        raw_text = (
            "Hybrid role with 40% international travel across EMEA, Germany and Spain."
        )
        meta = main._extract_abroad_metadata(raw_text)
        score, badges = main._enhance_abroad_score(2, ["remote_or_hybrid"], meta, raw_text)
        self.assertGreaterEqual(score, 3)
        self.assertIn("travel_percentage", badges)
        self.assertIn("geo_scope", badges)

    def test_infer_work_mode_supports_dutch_terms(self):
        self.assertEqual(main._infer_work_mode("Hybride werken in Amsterdam"), "Hybrid")
        self.assertEqual(main._infer_work_mode("Op afstand in Nederland"), "Remote")
        self.assertEqual(main._infer_work_mode("Op locatie in Utrecht"), "On-site")

    def test_parse_indeed_cards_extracts_salary_and_mode_hint(self):
        html = """
        <div class="job_seen_beacon">
          <h2 class="jobTitle"><a href="/rc/clk?jk=abc123">Role</a></h2>
          <span data-testid="company-name">Company</span>
          <div data-testid="text-location">Hybride werken in Amsterdam</div>
          <div data-testid="attribute_snippet_testid">€ 3.500 - € 4.200 per maand</div>
          <div class="job-snippet"><ul><li>Workflow en operations</li></ul></div>
          <span class="date">Vandaag</span>
        </div>
        """
        selector = main.Selector(text=html)
        _, parsed = main._parse_indeed_cards(selector, "https://nl.indeed.com/jobs")
        self.assertEqual(len(parsed), 1)
        item = parsed[0]
        self.assertEqual(item["location"], "Amsterdam")
        self.assertEqual(item["salary"], "€ 3.500 - € 4.200 per maand")
        self.assertIn("hybrid", item.get("work_mode_hint", "").lower())

    def test_scrape_config_mode_is_mvp(self):
        config = main._public_scrape_config()
        self.assertEqual(config["profile"], "mvp")
        self.assertEqual(config["defaults"]["sources"], ["indeed_web", "linkedin_web"])
        self.assertEqual(config["defaults"]["location_mode"], "nl_only")

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

    def test_company_posting_route_redirects_when_external_company_url_exists(self):
        with main.app.test_client() as client:
            response = client.get(
                "/company-posting",
                query_string={"company_url": "https://company.example/jobs/42"},
            )
        self.assertEqual(response.status_code, 302)
        self.assertEqual(response.headers.get("Location"), "https://company.example/jobs/42")

    def test_company_posting_route_returns_error_when_unresolved(self):
        with patch("main.requests.get", side_effect=main.requests.RequestException("network blocked")):
            with main.app.test_client() as client:
                response = client.get(
                    "/company-posting",
                    query_string={"indeed_url": "https://nl.indeed.com/viewjob?jk=abc123"},
                )
        self.assertEqual(response.status_code, 424)
        self.assertIn("company posting url not found", response.get_data(as_text=True).lower())

    def test_company_posting_route_resolves_from_job_url_redirect_param(self):
        with main.app.test_client() as client:
            response = client.get(
                "/company-posting",
                query_string={
                    "job_url": (
                        "https://nl.indeed.com/pagead/clk?"
                        "jk=abc123&adurl=https%3A%2F%2Fcompany.example%2Fcareers%2F42"
                    )
                },
            )
        self.assertEqual(response.status_code, 302)
        self.assertEqual(response.headers.get("Location"), "https://company.example/careers/42")

    def test_custom_mode_uses_query_terms_for_generic_ranking(self):
        jobs = [
            self._job(
                "CustomFit",
                "Cross-functional workflow for vendor rollout and ecosystem operations.",
                title="Custom Ecosystem Operations Specialist",
            )
        ]
        ranked = main.rank_and_filter_jobs(
            jobs,
            target_sleeve="E",
            min_target_score=3,
            location_mode="global",
            strict_sleeve=False,
            custom_mode=True,
            custom_query_terms=[
                "ecosystem operations",
                "vendor rollout",
                "workflow",
            ],
        )
        self.assertEqual(len(ranked), 1)
        self.assertIn(ranked[0]["decision"], {"PASS", "MAYBE"})
        self.assertGreaterEqual(ranked[0]["primary_sleeve_score"], 1)


if __name__ == "__main__":
    unittest.main()
