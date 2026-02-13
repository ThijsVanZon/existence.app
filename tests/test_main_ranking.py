import unittest

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


if __name__ == "__main__":
    unittest.main()
