from argparse import ArgumentParser
from functools import reduce


def parse_spark_args(keys: list, argv: list):
    """parse_spark_args.
    Parse agruments passed to spark submit operator
    :param keys: The keys for parameters passed
    :type keys: list
    :param argv: The sys.argv instance containing the list of keys and values
    :type argv: list
    """
    parser = ArgumentParser()
    for key in keys:
        parser.add_argument(('--' + key))

    parser.add_argument(('--' + "app_name"))

    return parser.parse_args(argv[1:])


def build_spark_args(**kwargs):
    return reduce(list.__add__, [[f"--{key}", value] for key, value in kwargs.items() if value])
