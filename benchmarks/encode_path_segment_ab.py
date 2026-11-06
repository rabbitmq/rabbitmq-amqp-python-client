"""O1 A/B: ``encode_path_segment`` legacy vs ``urllib.parse.quote``.

Measures the **public** function, so the feature-flag dispatch is included.
For each corpus:

* compares the outputs: if they differ on ASCII, exits with 1 (regression);
* reports the median and minimum of ``REPEATS`` runs of ``ITERATIONS`` iterations.

Usage, from the client root:

    python benchmarks/encode_path_segment_ab.py
    RABBITMQ_AMQP_CLIENT_ENCODER=quote python benchmarks/encode_path_segment_ab.py

The flag is not required: the script forces both branches with ``_set_encoder``
and restores the initial value at the end.
"""

from __future__ import annotations

import statistics
import sys
import timeit
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from rabbitmq_amqp_python_client import AddressHelper  # noqa: E402
from rabbitmq_amqp_python_client import address_helper as ah  # noqa: E402

ITERATIONS = 20_000
REPEATS = 7

CORPORA: list[tuple[str, str, bool]] = [
    ("short ASCII", "ordini-2026", True),
    ("long ASCII", "code con spazi e caratteri 2026-" + "x" * 40, True),
    ("64 characters", "q" * 64, True),
    ("hostile ASCII URI", "my_exchange/()?a=b#c", True),
    ("Unicode", "città/italia", False),
    ("emoji", "emoji-🚀-queue", False),
]


def _measure(value: str) -> tuple[float, float]:
    """Median and minimum in µs/op of the public function on the given value."""
    samples = timeit.repeat(
        "encode(value)",
        globals={"encode": ah.encode_path_segment, "value": value},
        repeat=REPEATS,
        number=ITERATIONS,
    )
    per_op = [sample / ITERATIONS * 1e6 for sample in samples]
    return statistics.median(per_op), min(per_op)


def main() -> int:
    print(f"Python {sys.version.split()[0]} — {ITERATIONS} iterations × {REPEATS} runs\n")
    print(f"{'corpus':16} {'legacy µs':>10} {'quote µs':>10} {'speedup':>9}  output")
    print("-" * 74)

    original = ah._ENCODER
    regressions: list[str] = []
    try:
        for label, value, ascii_only in CORPORA:
            ah._set_encoder(ah.ENCODER_LEGACY)
            legacy_median, legacy_min = _measure(value)
            legacy_output = ah.encode_path_segment(value)

            ah._set_encoder(ah.ENCODER_QUOTE)
            quote_median, quote_min = _measure(value)
            quote_output = ah.encode_path_segment(value)

            same = legacy_output == quote_output
            if ascii_only and not same:
                regressions.append(label)
            speedup = legacy_median / quote_median if quote_median else float("inf")
            status = "identical" if same else "different (Unicode)"
            print(
                f"{label:16} {legacy_median:10.3f} {quote_median:10.3f} "
                f"{speedup:8.2f}×  {status}"
            )
            print(
                f"{'':16} min {legacy_min:.3f} / {quote_min:.3f} | "
                f"legacy={legacy_output[:52]!r}"
            )
            if not same:
                print(f"{'':16} quote={quote_output[:52]!r}")

        # Real path: AddressHelper.queue_address includes the prefix.
        ah._set_encoder(ah.ENCODER_LEGACY)
        legacy_queue = _measure(AddressHelper.queue_address("ordini-2026"))
        ah._set_encoder(ah.ENCODER_QUOTE)
        quote_queue = _measure(AddressHelper.queue_address("ordini-2026"))
        print("-" * 74)
        print(
            "queue_address('ordini-2026'): "
            f"legacy {legacy_queue[0]:.3f} µs → quote {quote_queue[0]:.3f} µs "
            f"({legacy_queue[0] / quote_queue[0]:.2f}×)"
        )
    finally:
        ah._set_encoder(original)

    if regressions:
        print(f"\nREGRESSION: ASCII output differs for {regressions}")
        return 1

    print("\nNo ASCII output regressions.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
