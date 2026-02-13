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

    def test_abroad_gate_filters_jobs_without_abroad_signal(self):
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
        self.assertEqual(ranked, [])

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

    def test_dedupe_by_company_title_city(self):
        jobs = [
            {
                "title": "AV Technician",
                "company": "SameCo",
                "location": "Amsterdam, Netherlands",
                "snippet": "Remote hybrid festival role with travel.",
                "link": "https://example.com/1",
                "date": "2026-02-01",
                "source": "test",
            },
            {
                "title": "AV Technician",
                "company": "SameCo",
                "location": "Amsterdam Netherlands",
                "snippet": "Remote hybrid festival role with travel.",
                "link": "https://example.com/2",
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


if __name__ == "__main__":
    unittest.main()
