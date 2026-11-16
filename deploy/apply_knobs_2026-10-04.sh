#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "$0")"

set_knob() {
  local key="$1" value="$2"
  if grep -q "^${key}=" .env; then
    sed -i "s|^${key}=.*|${key}=${value}|" .env
  else
    printf '%s=%s\n' "$key" "$value" >> .env
  fi
}

set_knob RECYCLE_RESERVE_FRAC 0
set_knob EXIT_HURDLE 0
set_knob RECYCLE_ENABLED false
set_knob RISK_CARRY_CAP_FRAC 0.15
set_knob RISK_TIER_DEPLOYED 0.30,0.30,0.30,0.30
set_knob RISK_MAX_RACE_LOSS_FRAC 0.20
set_knob SWEEP_DUMP_OFFSETS_C 6,9,13
set_knob SWEEP_ROC_SIZES 6000,8000,10000
set_knob SWEEP_BUDGET_FRAC 0.80
set_knob SWEEP_CASH_MULTIPLE 1.3
set_knob EXIT_UNWIND_EXCHANGES carry
set_knob EXIT_UNWIND_AFTER 2026-10-04T15:00Z
set_knob EXIT_UNWIND_MAX_LOSS_C 4
sed -i '/^SWEEP_ROC_LEVELS=/d' .env

echo "knobs now:"
grep -E "^(RECYCLE_RESERVE_FRAC|EXIT_HURDLE|RECYCLE_ENABLED|RISK_CARRY_CAP_FRAC|RISK_TIER_DEPLOYED|RISK_MAX_RACE_LOSS_FRAC|SWEEP_DUMP_OFFSETS_C|SWEEP_ROC_SIZES|SWEEP_ROC_LEVELS|SWEEP_BUDGET_FRAC|SWEEP_CASH_MULTIPLE|EXIT_UNWIND_EXCHANGES|EXIT_UNWIND_AFTER|EXIT_UNWIND_MAX_LOSS_C|SWEEP_MARKETS|SWEEP_BAND_C)=" .env | sort
