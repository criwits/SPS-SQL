import json
import os
import argparse
import tempfile

parser = argparse.ArgumentParser(description='Validate generated SQLs')
parser.add_argument('--result-file', type=str, required=True, help='Path to the JSON file containing the generated SQLs', dest='result_file')
parser.add_argument('--db-dir', type=str, required=True, help='Path to the SQLite database file', dest='db_dir')
parser.add_argument('--start', type=int, required=False, default=0, help='Start index of the samples to validate', dest='start')
parser.add_argument('--end', type=int, required=False, default=-1, help='End index of the samples to validate', dest='end')
# parser.add_argument('--output-file', type=str, required=True, help='Path to the output JSON file', dest='output_file')
args = parser.parse_args()

with open(args.result_file, 'r') as file:
    data = json.load(file)

gold_sqls = [f"{sample['reference']}\t{sample['db_id']}" for sample in data[args.start:args.end]]
predicted_sqls = [f"{sample['generated']}" for sample in data[args.start:args.end]]

with tempfile.TemporaryDirectory() as work_dir:
    gold_full_path = f"{work_dir}/gold.db"
    predicted_full_path = f"{work_dir}/predicted.db"

    with open(gold_full_path, 'w') as file:
        file.write("\n".join(gold_sqls))
    with open(predicted_full_path, 'w') as file:
        file.write("\n".join(predicted_sqls))

    # 执行 python test-suit-sql-eval/evaluation.py --gold GOLD --pred PRED --db DB --etype exec
    os.system(f"python test-suite-sql-eval/evaluation.py --gold {gold_full_path} --pred {predicted_full_path} --db {args.db_dir} --etype exec")

