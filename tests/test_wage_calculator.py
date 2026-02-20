import unittest

import main
import wage_calculator as wagecalc


class TestWageCalculator(unittest.TestCase):
    def setUp(self):
        self.client = main.app.test_client()
        self._original_auth_enforce = main.AUTH_ENFORCE
        main.AUTH_ENFORCE = False

    def tearDown(self):
        main.AUTH_ENFORCE = self._original_auth_enforce

    def test_calculate_payroll_mode(self):
        result, error = wagecalc.calculate(
            "payroll",
            {
                "payroll_gross_yearly": 90000,
                "payroll_net_yearly": 55000,
                "fringe_benefits_yearly": 10000,
                "freelance_net_yearly": 65000,
            },
        )
        self.assertIsNone(error)
        self.assertEqual(result["payroll"]["gross"]["yearly"], 90000.0)
        self.assertEqual(result["freelance"]["gross"]["yearly"], 100000.0)
        self.assertEqual(result["payroll"]["expenses_daily_budget"], 150.58)

    def test_calculate_expenses_mode(self):
        result, error = wagecalc.calculate(
            "expenses",
            {
                "expenses_daily_budget": 160,
                "payroll_gross_yearly": 82000,
                "fringe_benefits_yearly": 12000,
                "freelance_net_yearly": 68000,
            },
        )
        self.assertIsNone(error)
        self.assertEqual(result["payroll"]["net"]["yearly"], 58438.8)
        self.assertEqual(result["freelance"]["gross"]["yearly"], 94000.0)

    def test_freelance_mode_rejects_negative_payroll_gross(self):
        result, error = wagecalc.calculate(
            "freelance",
            {
                "freelance_gross_hourly": 15,
                "freelance_net_yearly": 30000,
                "fringe_benefits_yearly": 50000,
                "payroll_net_yearly": 26000,
            },
        )
        self.assertIsNone(result)
        self.assertEqual(error.get("code"), "wagecalculator_negative_payroll_gross")

    def test_wagecalculator_page_renders(self):
        response = self.client.get("/wagecalculator")
        self.assertEqual(response.status_code, 200)
        self.assertIn("Wage Calculator", response.get_data(as_text=True))

    def test_wagecalculator_api_returns_calculation(self):
        response = self.client.post(
            "/wagecalculator/calculate",
            json={
                "mode": "freelance",
                "inputs": {
                    "freelance_gross_hourly": 80,
                    "freelance_net_yearly": 90000,
                    "fringe_benefits_yearly": 12000,
                    "payroll_net_yearly": 61000,
                },
            },
        )
        self.assertEqual(response.status_code, 200)
        payload = response.get_json() or {}
        self.assertTrue(payload.get("ok"))
        self.assertEqual(
            (payload.get("result") or {}).get("payroll", {}).get("gross", {}).get("yearly"),
            154968.0,
        )

    def test_wagecalculator_api_requires_auth_when_enforced(self):
        main.AUTH_ENFORCE = True
        response = self.client.post(
            "/wagecalculator/calculate",
            json={"mode": "payroll", "inputs": {}},
        )
        self.assertEqual(response.status_code, 401)


if __name__ == "__main__":
    unittest.main()

