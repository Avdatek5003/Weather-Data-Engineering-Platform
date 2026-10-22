def validate_row_counts(
    landing: int,
    silver: int,
    quarantine: int,
) -> None:
    """Validate consistency between pipeline layers."""

    counts = {
        "landing": landing,
        "silver": silver,
        "quarantine": quarantine,
    }

    for name, value in counts.items():
        if type(value) is not int or value < 0:
            raise ValueError(
                f"Invalid {name} count: {value}"
            )

    if landing <= 0:
        raise ValueError("Landing must not be empty")

    if silver <= 0:
        raise ValueError("Silver must not be empty")

    if landing != silver + quarantine:
        raise ValueError(
            "Row count mismatch: "
            f"landing={landing}, "
            f"silver={silver}, "
            f"quarantine={quarantine}"
        )