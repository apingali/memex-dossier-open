'''Run all memex_dossier.fc tests.

.. This software is released under an MIT/X11 open source license.
   Copyright 2012-2014 Diffeo, Inc.

'''
from __future__ import absolute_import, division, print_function
import argparse
import os

try:
    import pytest
except ImportError:
    pytest = None

import memex_dossier.fc.tests.performance as performance_tests


def main():
    parser = argparse.ArgumentParser(
        description='Run all memex_dossier.fc tests.',
    )
    if pytest:
        parser.add_argument('--unit', action='store_true',
                            help='Run unit tests')
    parser.add_argument('--performance', '--perf', action='store_true',
                        help='Run performance tests')
    args = parser.parse_args()

    if not args.unit and not args.performance:
        args.unit = (pytest is not None)
        args.performance = True

    if args.unit:
        pytest.main(os.path.dirname(__file__))

    if args.performance:
        performance_tests.main()


if __name__ == '__main__':
    main()
