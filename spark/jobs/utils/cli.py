import argparse
from skyfeed.config import settings

def base_parser(description: str) -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=description)
    parser.add_argument('--warehouse-uri', default=settings.warehouse_uri)
    parser.add_argument('--checkpoint-dir', default=settings.checkpoint_dir)
    return parser
