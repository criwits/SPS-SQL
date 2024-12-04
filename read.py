import argparse
import json

arg_parser = argparse.ArgumentParser()
arg_parser.add_argument("--input", dest="input", type=str, default="data/bird_minidev/MINIDEV/mini_dev_sqlite.json")
arg_parser.add_argument("--difficulty", dest="difficulty", type=str, required=False, default=None)

args = arg_parser.parse_args()

with open(args.input, "r") as f:
    data = json.load(f)

if args.difficulty is not None:
    data = [d for d in data if d["difficulty"] == args.difficulty]

    