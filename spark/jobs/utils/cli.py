from __future__ import annotations

import argparse


def base_parser(description: str) -> argparse.ArgumentParser:
    return argparse.ArgumentParser(description=description)
