"""Command-line boundary for read-only review and explicitly authorized approval."""

import argparse
from pathlib import Path
from uuid import UUID

from labgdd.publication_governance import approve_reviewed_step, review_publication_plan


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="labgdd-publications")
    subparsers = parser.add_subparsers(dest="operation", required=True)
    review = subparsers.add_parser("review")
    review.add_argument("--plan", required=True, type=Path)
    review.add_argument("--project-root", required=True, type=Path)
    review.add_argument("--data-root", required=True, type=Path)
    review.add_argument("--report", required=True, type=Path)
    approve = subparsers.add_parser("approve")
    approve.add_argument("--plan", required=True, type=Path)
    approve.add_argument("--project-root", required=True, type=Path)
    approve.add_argument("--data-root", required=True, type=Path)
    approve.add_argument("--report", required=True, type=Path)
    approve.add_argument("--step", required=True)
    approve.add_argument("--dataset-version", required=True)
    approve.add_argument("--identity", required=True, type=UUID)
    return parser


def main() -> int:
    args = _parser().parse_args()
    if args.operation == "review":
        review_publication_plan(args.plan, args.project_root, args.data_root, args.report)
    else:
        approve_reviewed_step(
            args.plan,
            args.project_root,
            args.data_root,
            args.report,
            step_name=args.step,
            dataset_version=args.dataset_version,
            identity=args.identity,
        )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
