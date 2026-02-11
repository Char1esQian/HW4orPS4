from __future__ import annotations


FREMONT_PLANT_CODE = "F"
AUSTIN_PLANT_CODE = "A"
FREMONT_HW4_MIN_SERIAL = 789500
AUSTIN_HW4_MIN_SERIAL = 131200


def normalize_vin(vin: str | None) -> str | None:
    if not vin:
        return None
    normalized = vin.strip().upper()
    return normalized or None


def is_hw4_likely_model_y(vin: str | None) -> tuple[bool, str]:
    normalized = normalize_vin(vin)
    if not normalized:
        return False, "VIN missing."
    if len(normalized) != 17:
        return False, "VIN invalid length; expected 17 characters."

    plant_code = normalized[10]
    serial_text = normalized[-6:]
    if not serial_text.isdigit():
        return False, "VIN serial is not numeric."

    serial = int(serial_text)
    if plant_code == FREMONT_PLANT_CODE:
        if serial >= FREMONT_HW4_MIN_SERIAL:
            return True, "Fremont serial meets HW4 threshold."
        return False, "Fremont serial below HW4 threshold."
    if plant_code == AUSTIN_PLANT_CODE:
        if serial >= AUSTIN_HW4_MIN_SERIAL:
            return True, "Austin serial meets HW4 threshold."
        return False, "Austin serial below HW4 threshold."
    return False, f"Unsupported plant code '{plant_code}' for this heuristic."

