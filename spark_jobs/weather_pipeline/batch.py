from dataclasses import dataclass


@dataclass(frozen=True)
class BatchIdentity:
    batch_key: str
    mode: str
    start_date: str | None
    end_date: str | None


def build_batch_key(args) -> str:
    """
    Build a deterministic key for a logical processing request.
    """
    if args.start_date:
        return (
            f"historical_{args.mode}_"
            f"{args.start_date.replace('-', '')}_"
            f"{args.end_date.replace('-', '')}"
        )

    if args.mode == "sample":
        return "historical_sample_unbounded"

    return "historical_full_unbounded"