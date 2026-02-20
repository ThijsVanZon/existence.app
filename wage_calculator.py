"""Wage calculator domain logic for payroll, freelance, and expenses scenarios."""

from __future__ import annotations

from typing import Dict, Tuple

YEARLY_WORK_HOURS = 2087.1
YEARLY_DAYS = 365.2425
YEARLY_MONTHS = 12.0

SUPPORTED_MODES = ("payroll", "expenses", "freelance")

_MODE_REQUIREMENTS = {
    "payroll": (
        "payroll_gross_yearly",
        "payroll_net_yearly",
        "fringe_benefits_yearly",
        "freelance_net_yearly",
    ),
    "expenses": (
        "expenses_daily_budget",
        "payroll_gross_yearly",
        "fringe_benefits_yearly",
        "freelance_net_yearly",
    ),
    "freelance": (
        "freelance_gross_hourly",
        "freelance_net_yearly",
        "fringe_benefits_yearly",
        "payroll_net_yearly",
    ),
}


def _round_money(value: float) -> float:
    return round(float(value) + 1e-9, 2)


def _to_number(value) -> float:
    return float(str(value).strip())


def _rates_from_yearly(yearly: float, hourly_override: float | None = None) -> Dict[str, float]:
    yearly_value = float(yearly)
    monthly_value = yearly_value / YEARLY_MONTHS
    hourly_value = float(hourly_override) if hourly_override is not None else yearly_value / YEARLY_WORK_HOURS
    return {
        "yearly": _round_money(yearly_value),
        "monthly": _round_money(monthly_value),
        "hourly": _round_money(hourly_value),
    }


def _parse_inputs(mode: str, raw_inputs: Dict) -> Tuple[Dict[str, float] | None, Dict | None]:
    required_fields = _MODE_REQUIREMENTS.get(mode)
    if not required_fields:
        return None, {
            "code": "wagecalculator_invalid_mode",
            "error": f"Unsupported mode. Use one of: {', '.join(SUPPORTED_MODES)}.",
        }

    parsed = {}
    missing = []
    invalid = []
    for field_name in required_fields:
        raw_value = (raw_inputs or {}).get(field_name)
        if raw_value is None or str(raw_value).strip() == "":
            missing.append(field_name)
            continue
        try:
            parsed[field_name] = _to_number(raw_value)
        except (TypeError, ValueError):
            invalid.append(field_name)

    if missing:
        return None, {
            "code": "wagecalculator_missing_inputs",
            "error": f"Missing required inputs: {', '.join(missing)}.",
            "required_inputs": list(required_fields),
        }
    if invalid:
        return None, {
            "code": "wagecalculator_invalid_inputs",
            "error": f"Invalid numeric inputs: {', '.join(invalid)}.",
            "required_inputs": list(required_fields),
        }

    for field_name, field_value in parsed.items():
        if field_name == "fringe_benefits_yearly":
            if field_value < 0:
                return None, {
                    "code": "wagecalculator_negative_fringe",
                    "error": "fringe_benefits_yearly cannot be negative.",
                }
        elif field_value <= 0:
            return None, {
                "code": "wagecalculator_non_positive_input",
                "error": f"{field_name} must be greater than zero.",
            }

    return parsed, None


def calculate(mode: str, raw_inputs: Dict | None) -> Tuple[Dict | None, Dict | None]:
    normalized_mode = str(mode or "").strip().lower()
    parsed, parse_error = _parse_inputs(normalized_mode, raw_inputs or {})
    if parse_error:
        return None, parse_error

    payroll_gross_yearly = 0.0
    payroll_net_yearly = 0.0
    payroll_expenses_daily_budget = 0.0
    freelance_gross_yearly = 0.0
    freelance_gross_hourly_override = None
    freelance_net_yearly = 0.0

    if normalized_mode == "payroll":
        payroll_gross_yearly = parsed["payroll_gross_yearly"]
        payroll_net_yearly = parsed["payroll_net_yearly"]
        payroll_expenses_daily_budget = payroll_net_yearly / YEARLY_DAYS
        freelance_gross_yearly = payroll_gross_yearly + parsed["fringe_benefits_yearly"]
        freelance_net_yearly = parsed["freelance_net_yearly"]
    elif normalized_mode == "expenses":
        payroll_expenses_daily_budget = parsed["expenses_daily_budget"]
        payroll_net_yearly = payroll_expenses_daily_budget * YEARLY_DAYS
        payroll_gross_yearly = parsed["payroll_gross_yearly"]
        freelance_gross_yearly = payroll_gross_yearly + parsed["fringe_benefits_yearly"]
        freelance_net_yearly = parsed["freelance_net_yearly"]
    elif normalized_mode == "freelance":
        freelance_gross_hourly_override = parsed["freelance_gross_hourly"]
        freelance_gross_yearly = freelance_gross_hourly_override * YEARLY_WORK_HOURS
        payroll_gross_yearly = freelance_gross_yearly - parsed["fringe_benefits_yearly"]
        if payroll_gross_yearly <= 0:
            return None, {
                "code": "wagecalculator_negative_payroll_gross",
                "error": "Computed payroll gross yearly rate is not positive. Lower fringe_benefits_yearly or raise freelance_gross_hourly.",
            }
        payroll_net_yearly = parsed["payroll_net_yearly"]
        payroll_expenses_daily_budget = payroll_net_yearly / YEARLY_DAYS
        freelance_net_yearly = parsed["freelance_net_yearly"]
    else:
        return None, {
            "code": "wagecalculator_invalid_mode",
            "error": f"Unsupported mode. Use one of: {', '.join(SUPPORTED_MODES)}.",
        }

    result = {
        "mode": normalized_mode,
        "constants": {
            "yearly_work_hours": YEARLY_WORK_HOURS,
            "yearly_days": YEARLY_DAYS,
            "yearly_months": YEARLY_MONTHS,
        },
        "inputs": {key: _round_money(value) for key, value in parsed.items()},
        "payroll": {
            "gross": _rates_from_yearly(payroll_gross_yearly),
            "net": _rates_from_yearly(payroll_net_yearly),
            "expenses_daily_budget": _round_money(payroll_expenses_daily_budget),
        },
        "freelance": {
            "gross": _rates_from_yearly(
                freelance_gross_yearly,
                hourly_override=freelance_gross_hourly_override,
            ),
            "net": _rates_from_yearly(freelance_net_yearly),
            "expenses_daily_budget": _round_money(freelance_net_yearly / YEARLY_DAYS),
        },
        "notes": [
            "Gross-to-net conversions depend on local tax setup and are therefore not inferred automatically.",
            "Use the same assumption window for payroll and freelance to keep comparisons fair.",
        ],
    }
    return result, None

