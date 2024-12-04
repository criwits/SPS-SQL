import argparse
import json
from jinja2 import Template
from tqdm import tqdm
import re

from llm.glm4 import GLM4
from schema import build_db_from_spider

parser = argparse.ArgumentParser()
parser.add_argument("--input", dest="input", type=str, required=True)
parser.add_argument("--begin", dest="begin", type=int, default=0)
parser.add_argument("--end", dest="end", type=int, default=-1)
parser.add_argument("--table", dest="table", type=str, required=True)
parser.add_argument("--base-model", dest="base_model", type=str, required=True)
parser.add_argument("--output-result", dest="output_result", type=str, required=True)
parser.add_argument("--save-interval", dest="save_interval", type=int, default=10)
args = parser.parse_args()

input_template_file =  "llm_templates/narrate_input.j2"

with open(input_template_file, "r") as f:
    input_template = Template(f.read())

# model = InternLM(args.base_model, "base-model")
# model = Llama(args.base_model, "base-model")
model = GLM4(args.base_model, "base-model")

# 读数据
with open(args.input, 'r') as f:
    data = json.load(f)

# 读取数据库 schema 信息
with open(args.table, "r") as f:
    table = json.load(f)

dbs = {}
for db in table:
    dbs[db["db_id"]] = build_db_from_spider(db)

output = []

with tqdm(data) as bar:
    bar.update(args.begin)
    for index in range(args.begin, len(data) if args.end == -1 else args.end):
        sample = data[index]
        db_id = sample["db_id"]
        sql = sample["sql"]

        ddls = [table.to_ddl() for table in dbs[db_id].tables.values()]

        input_text = input_template.render(
            ddls=ddls,
            sql=sql
        )

        model_output = model.infer(input_text, system_prompt=None, do_sample=False, top_k=None, top_p=None, temperature=None, max_new_tokens=512)
        answer = re.sub(r"\s+", " ", model_output).strip()
        answer = answer.strip()

        res = {
            "db_id": db_id,
            "sql": sql,
            "query": answer
        }

        output.append(res)
        bar.update(1)

        if index % args.save_interval == 0:
            with open(args.output_result, "w", encoding="utf-8") as f:
                json.dump(output, f, ensure_ascii=False, indent=4)

with open(args.output_result, "w", encoding="utf-8") as f:
    json.dump(output, f, ensure_ascii=False, indent=4)




    

