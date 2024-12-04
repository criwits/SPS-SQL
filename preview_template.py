import argparse
import pickle
from template import SQLTemplate

parser = argparse.ArgumentParser()
parser.add_argument('--templates', type=str, required=True)
args = parser.parse_args()

with open(args.templates, 'rb') as f:
    templates = pickle.load(f)

for index, (template, occur) in enumerate(templates):
    print(f"No.{index}({occur}) {template.framework}")