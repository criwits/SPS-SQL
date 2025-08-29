import argparse
import numpy as np
from transformers import AutoModelForCausalLM, AutoTokenizer, AutoModel, pipeline
import torch
from peft import PeftModel # type: ignore
import json
from jinja2 import Template
from tqdm import tqdm
import re
import random
import datasets

from llm.glm4 import GLM4
from llm.llama import Llama
from llm.llm import LLM, DumbLLM
from llm.qwen import Qwen
from schema import Database, build_db_from_spider_bird

def generate_prompt(
    db: Database,
    question: str,
    question_embeddings: np.ndarray,
    reference_shot: int,
    reference_datasets_dict: dict,
    sl: bool,
    link_result: list[dict],
    input_template: Template,
):
    # 找相似问题
    if reference_shot > 0:
        score, nearest_examples = reference_datasets_dict[db.name].get_nearest_examples('embeddings', question_embeddings, k=reference_shot)
        examples = zip(nearest_examples["question"], nearest_examples["sql"])
        examples = list(examples)
    else:
        examples = []

    # 找到所有的表
    ddls = []

    # Schema linking
    # if sl:
        # linked_tables = schema_linking(question, db, sl_model, sl_template)
    # else:
    linked_tables = db.tables.values()

    tables = [str(table) for table in linked_tables]
    ddls = [table.to_ddl() for table in linked_tables]
    fks = []
    for table in linked_tables:
        for fk in table.foreign_keys:
            fks.append(f"{table.name}({fk[0]}) REFERENCES {fk[1].name}({fk[2]})")

    prompt = input_template.render(question=question, tables=tables, ddls=ddls, fks=fks, examples=examples, have_fk=len(fks) > 0, have_examples=len(examples) > 0)

    return prompt

def extract_sql(raw_answer: str) -> str:
    raw_answer = raw_answer.strip()
    # 将所有 \s 全部替换成空格
    raw_answer = re.sub(r'\s+', ' ', raw_answer)

    # 第一种情况，生成的 SQL 被 ```sql``` 包裹
    sql_pattern = r'```sql(.*?)```'
    all_sqls = []
    for match in re.finditer(sql_pattern, raw_answer, re.DOTALL):
        all_sqls.append(match.group(1).strip())
    
    if all_sqls:
        # 如果有多个 SQL，返回最后一个，因为一般 LLM 会不断修改它之前回答的错误
        return all_sqls[-1]
    
    # 第二种情况，只能先找到第一个 `SELECT `，然后向后匹配到第一个 `;` 或匹配到结尾
    # Find all SELECT occurrences
    selects = [m.start() for m in re.finditer('SELECT', raw_answer, re.IGNORECASE)]
    
    # If no SELECT is found, return an empty string
    if not selects:
        return "ERROR"
    
    # Get the first SELECT position
    last_select_pos = selects[0]
    
    # Extract the substring from the last SELECT to the end of the string
    substring = raw_answer[last_select_pos:]
    
    # Find the first semicolon or the end of the string
    semicolon_pos = substring.find(';')
    if semicolon_pos != -1:
        return substring[:semicolon_pos+1]
    else:
        return substring

def flatten_sql(sql: str) -> str:
    # 去除所有的多余空格和换行
    sql = re.sub(r'\s+', ' ', sql)
    return sql.strip()

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--model-type", dest="model_type", type=str, required=True)
    parser.add_argument("--test-set", dest="test_set", type=str, required=True)
    parser.add_argument("--table", dest="table", type=str, required=True)
    parser.add_argument("--db-name", dest="db_name", type=str, required=False, nargs="*")
    parser.add_argument("--reference-datasets-prefix", dest="reference_datasets_prefix", type=str, required=True)
    parser.add_argument("--reference-shot", dest="reference_shot", type=int, required=True)
    parser.add_argument("--sl", dest="sl", action="store_true", default=False)
    parser.add_argument("--base-model", dest="base_model", type=str, required=True)
    parser.add_argument("--base-peft", dest="base_peft", type=str, required=False)
    parser.add_argument("--bge-model", dest="bge_model", type=str, required=True)
    parser.add_argument("--output-result", dest="output_result", type=str, required=True)
    parser.add_argument("--save-interval", dest="save_interval", type=int, default=10)
    parser.add_argument("--preview-prompt", dest="preview_prompt", action="store_true", default=False)
    parser.add_argument("--begin", dest="begin", type=int, required=False, default=0)
    parser.add_argument("--end", dest="end", type=int, required=False, default=-1)
    parser.add_argument("--template-file", dest="template_file", type=str, required=False, default="llm_templates/infer_input_used.j2")
    args = parser.parse_args()

    ####################################
    # 以下是针对不同模板、模型需要修改的部分

    ## 推理模板
    # input_template_file =  "llm_templates/infer_input_used.j2"
    # input_template_file =  "llm_templates/infer_nosharp_input.j2"
    # input_template_file =  "llm_templates/infer_ddl_input.j2"
    # input_template_file =  "llm_templates/infer_cr_ddl_input.j2"
    # input_template_file =  "llm_templates/infer_thinking_ddl_input.j2"
    # input_template_file =  "llm_templates/infer_cr_input.j2"
    input_template_file = args.template_file

    # 基础 LLM
    if args.model_type == "glm4":
        base_model = GLM4(args.base_model, "base-model")
    elif args.model_type == "llama":
        base_model = Llama(args.base_model, "base-model")
    elif args.model_type == "qwen":
        base_model = Qwen(args.base_model, "base-model")
    else:
        base_model = DumbLLM(args.base_model, "base-model")

    ####################################

    # 加载bge模型
    bge_tokenizer = AutoTokenizer.from_pretrained(args.bge_model)
    bge_model = AutoModel.from_pretrained(args.bge_model)
    bge_model.eval()

    # 读数据
    with open(args.test_set, 'r') as f:
        data = json.load(f)
    with open(input_template_file, "r") as f:
        input_template = Template(f.read())

    # 筛选出 db_name 对应的数据
    if args.db_name is not None:
        data = [x for x in data if x["db_id"] in args.db_name]

    data = data[args.begin:args.end]

    # 读取数据库 schema 信息
    with open(args.table, "r") as f:
        table = json.load(f)

    dbs = {}
    for db in table:
        dbs[db["db_id"]] = build_db_from_spider_bird(db)

    used_dbs = set()
    for sample in data:
        used_dbs.add(sample["db_id"])

    dbs = {k: v for k, v in dbs.items() if k in used_dbs}

    ref_datasets = {}

    for db in dbs.values():
        ref_path = args.reference_datasets_prefix + "_" + db.name
        ref_datasets[db.name] = datasets.Dataset.load_from_disk(ref_path)
        ref_datasets[db.name].add_faiss_index(column='embeddings')


    output = []

    with tqdm(data) as bar:
        for index, sample in enumerate(data):
            db = dbs[sample["db_id"]]
            question = sample["question"]
            reference_sql = sample["query"]
            # reference_sql = sample["sql"]
            # link_result = sample["link_result"]
            # link_result = [{"table": x[0], "column": x[1]} for x in sample["used_tables_and_columns"]]

            # 计算问题的 BGE embeddings
            with torch.no_grad():
                inputs = bge_tokenizer(question, return_tensors="pt", padding=True, truncation=True)
                outputs = bge_model(**inputs)
                question_embeddings = outputs[0][:, 0][0].cpu().numpy()

            prompt = generate_prompt(
                db=db,
                question=question,
                question_embeddings=question_embeddings,
                reference_shot=args.reference_shot,
                reference_datasets_dict=ref_datasets,
                sl=args.sl,
                link_result=None,
                input_template=input_template
            )

            if args.preview_prompt:
                print(prompt)
            
            # answer = base_model.infer(prompt, system_prompt=None, max_new_tokens=512, do_sample=False, top_p=None, top_k=None, temperature=None)
            answer = base_model.infer(prompt, system_prompt=None, max_new_tokens=512, do_sample=False, num_beams=5, top_p=None, top_k=None, temperature=None)
            # answer = base_model.infer(prompt, system_prompt=None, max_new_tokens=512, do_sample=False, top_p=None, top_k=None, temperature=None, num_beams=8)
            # generated_sql = re.sub(r'\s+', ' ', answer).strip()
            generated_sql = flatten_sql(extract_sql(answer))

            print(generated_sql)

            # if args.dataset_type == "bird":
            #     res = {
            #         "sample_id": "bird_" + str(sample["question_id"]), # For BIRD
            #         "difficulty": sample["difficulty"], # For BIRD
            #         "db_id": sample["db_id"],
            #         "question": question,
            #         "reference": reference_sql,
            #         "generated": generated_sql
            #     }
            # else:
            res = {
                "db_id": sample["db_id"],
                "question": question,
                "reference": reference_sql,
                "generated": generated_sql
            }

            output.append(res)
            bar.update(1)

            if index % args.save_interval == 0:
                with open(args.output_result, "w", encoding="utf-8") as f:
                    json.dump(output, f, ensure_ascii=False, indent=4)

    with open(args.output_result, "w", encoding="utf-8") as f:
        json.dump(output, f, ensure_ascii=False, indent=4)




        

