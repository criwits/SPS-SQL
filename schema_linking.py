import argparse
from transformers import AutoModelForCausalLM, AutoTokenizer, AutoModel, pipeline
import torch
import json
from jinja2 import Template
from tqdm import tqdm
import re

from llm.glm4 import GLM4
from llm.llama import Llama
from llm.llm import LLM
from llm.qwen import Qwen
from schema import Database, Table, build_db_from_spider

def schema_linking(question: str, db: Database, llm: LLM, template: Template) -> list[Table]:
    """
    使用 LLM 进行 schema linking
    """
    ddls = [table.to_ddl() for table in db.tables.values()]
    prompt = template.render(ddls=ddls, question=question)
    answer = llm.infer(prompt, max_new_tokens=256, do_sample=False, num_beams=5, top_p=None, top_k=None, temperature=None)
    answer = answer.strip()
    # 用 \n 分割，每行一个表名
    answer = answer.split("\n")
    # 去掉空行
    answer = [line for line in answer if line.strip() != ""]
    # 去掉表名后的空格
    answer = [re.sub(r"\s+", " ", line) for line in answer]
    # 去掉表名后的逗号
    answer = [re.sub(r",", "", line) for line in answer]
    answer = list(set(answer))

    real_answer = []
    for table_name in answer:
        for table in db.tables.values():
            if table_name.lower() == table.name.lower():
                real_answer.append(table)
                break

    return real_answer

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--sl-set", dest="sl_set", type=str, required=True)
    parser.add_argument("--table", dest="table", type=str, required=True)
    parser.add_argument("--base-model", dest="base_model", type=str, required=True)
    parser.add_argument("--output-result", dest="output_result", type=str, required=True)
    parser.add_argument("--save-interval", dest="save_interval", type=int, default=10)
    args = parser.parse_args()

    input_template_file =  "llm_templates/schema_linking_input.j2"

    with open(input_template_file, "r") as f:
        input_template = Template(f.read())

    # 加载 LLM
    llm = Qwen(args.base_model, "sl-model")

    # 读数据
    with open(args.sl_set, 'r') as f:
        data = json.load(f)

    # 读取数据库 schema 信息
    with open(args.table, "r") as f:
        table = json.load(f)

    dbs = {}
    for db in table:
        dbs[db["db_id"]] = build_db_from_spider(db)

    tables = {}
    ddls = {}
    fks = {}
    ref_datasets = {}

    for db in dbs.values():
        my_tables = []
        my_fks = []
        my_ddls = []

        for table in db.tables.values():
            my_tables.append(str(table))
            my_ddls.append(table.to_ddl())
            for fk in table.foreign_keys:
                my_fks.append(f"{table.name}.{fk[0]} references {fk[1].name}.{fk[2]}")

        tables[db.name] = my_tables
        fks[db.name] = my_fks
        ddls[db.name] = my_ddls


    output = []
    total = len(data)
    matched_cnt = 0
    ok_cnt = 0
    original_tables = 0
    linked_tables = 0

    with tqdm(data) as bar:
        for index, sample in enumerate(data):
            db = dbs[sample["db"]]
            ddls = [table.to_ddl() for table in db.tables.values()]
            question = sample["question"]
            reference_tables = sample["used_tables"]

            answer = schema_linking(question, db, llm, input_template)
            answer = [table.name for table in answer]

            lower_ref = [table.lower() for table in reference_tables]
            lower_ans = [table.lower() for table in answer]

            matched = set(lower_ref) == set(lower_ans)
            # 如果 reference_tables 是 answer 的子集，也算对
            ok = set(lower_ref).issubset(set(lower_ans))

            if matched:
                matched_cnt += 1
            if ok:
                ok_cnt += 1

            original_tables += len(db.tables)
            linked_tables += len(answer)

            output.append({
                "db": db.name,
                "table_cnt": len(db.tables),
                "question": question,
                "used_tables": reference_tables,
                "predicted_tables": answer,
                "match": matched,
                "ok": ok
            })

            bar.set_postfix_str(f"Matched: {matched_cnt / (index + 1):.2%}, OK: {ok_cnt / (index + 1):.2%}, Link Ratio: {linked_tables / original_tables:.2%}")

            bar.update(1)

            if index % args.save_interval == 0:
                with open(args.output_result, "w", encoding="utf-8") as f:
                    json.dump(output, f, ensure_ascii=False, indent=4)

    with open(args.output_result, "w", encoding="utf-8") as f:
        json.dump(output, f, ensure_ascii=False, indent=4)




        

