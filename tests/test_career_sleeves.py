import unittest

import career_sleeves as sleeves


class TestCareerSleeves(unittest.TestCase):
    def test_every_career_sleeve_has_tagline(self):
        for sleeve_id, config in sleeves.CAREER_SLEEVE_CONFIG.items():
            self.assertTrue(config.get("name"), f"missing name for {sleeve_id}")
            self.assertTrue(config.get("tagline"), f"missing tagline for {sleeve_id}")

    def test_language_required_vs_preferred_flags(self):
        required_flags, _ = sleeves.detect_language_flags(
            "German required. Must have fluent German."
        )
        preferred_flags, _ = sleeves.detect_language_flags(
            "German language is a plus for this role."
        )

        self.assertTrue(required_flags["extra_language_required"])
        self.assertFalse(required_flags["extra_language_preferred"])
        self.assertIn("german", required_flags["extra_languages"])

        self.assertFalse(preferred_flags["extra_language_required"])
        self.assertTrue(preferred_flags["extra_language_preferred"])
        self.assertIn("german", preferred_flags["extra_languages"])

    def test_language_required_marker_must_be_near_language(self):
        flags, _ = sleeves.detect_language_flags(
            "Must have strong stakeholder management. German is a plus."
        )
        self.assertFalse(flags["extra_language_required"])
        self.assertTrue(flags["extra_language_preferred"])

    def test_plural_matching_improves_keyword_detection(self):
        score, details = sleeves.score_career_sleeve(
            "A",
            (
                "Event producer role across festivals and concerts with "
                "crew coordination and on-site delivery."
            ),
            "Event Producer",
        )
        self.assertGreaterEqual(score, 3)
        self.assertIn("festival", details["context_hits"])
        self.assertEqual(details["reason"], "ok")

    def test_abroad_component_scoring_exposes_remote_mobility_and_visa(self):
        components, badges, details = sleeves.score_abroad_components(
            (
                "Hybrid role with international travel, site visits, relocation package, "
                "and visa sponsorship available."
            )
        )
        self.assertGreater(components["remote_flex_score"], 0)
        self.assertGreater(components["mobility_score"], 0)
        self.assertGreater(components["visa_score"], 0)
        self.assertLessEqual(components["abroad_score"], sleeves.ABROAD_SCORE_CAP)
        self.assertIn("remote_flex", badges)
        self.assertIn("mobility", badges)
        self.assertIn("visa_support", badges)
        self.assertIn("components", details)

    def test_hard_reject_detects_sales_titles(self):
        reason = sleeves.detect_hard_reject(
            "Account Executive",
            "Enterprise quota carrying role with cold calling.",
        )
        self.assertTrue(reason.startswith("hard_reject_title"))

    def test_workflow_role_scores_without_hard_must_have_gate(self):
        score, details = sleeves.score_career_sleeve(
            "D",
            "Implementation manager for supply chain workflow and vendor coordination.",
            "Implementation Manager",
        )
        self.assertGreaterEqual(score, 3)
        self.assertEqual(details["reason"], "ok")

    def test_domain_anchor_gate_caps_career_sleeve_c_when_anchor_missing(self):
        score, details = sleeves.score_career_sleeve(
            "C",
            "Commissioning engineer for HVAC reliability and electrical maintenance.",
            "Commissioning Engineer",
        )
        self.assertEqual(details["reason"], "missing_domain_anchors")
        self.assertLessEqual(
            score,
            int(sleeves.CAREER_SLEEVE_CONFIG["C"]["must_haves"]["anchor_cap_score"]),
        )

    def test_blocked_detection_requires_stronger_signal(self):
        self.assertFalse(
            sleeves.detect_blocked_html(
                "<html><body><a>Sign in to continue</a><div>Jobs list</div></body></html>"
            )
        )
        self.assertTrue(
            sleeves.detect_blocked_html(
                "<html><body><h1>Captcha</h1><p>Verify you are human</p></body></html>"
            )
        )


if __name__ == "__main__":
    unittest.main()

