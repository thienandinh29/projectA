"""Read collector session/request evidence without changing its status."""
import argparse
import json

from config import OUTBOX_PATH
from research.sessions import read_session_evidence, invalidate_session
from utils.outbox import DeliveryOutbox
from pathlib import Path


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--ledger', default=OUTBOX_PATH)
    parser.add_argument('--invalidate-run', help='Append an exclusion for an already terminated faulty collection run')
    parser.add_argument('--reason', help='Required concrete reason for the research exclusion')
    args = parser.parse_args()
    if bool(args.invalidate_run) != bool(args.reason):
        parser.error('--invalidate-run and --reason must be supplied together')
    if args.invalidate_run:
        if not Path(args.ledger).is_file():
            parser.error('Ledger is missing')
        invalidate_session(DeliveryOutbox(args.ledger), args.invalidate_run, args.reason)
    print(json.dumps(read_session_evidence(args.ledger), indent=2))


if __name__ == '__main__':
    main()
