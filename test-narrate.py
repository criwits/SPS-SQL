import argparse
from transformers import AutoModelForCausalLM, AutoTokenizer, AutoModel
import torch
import json
from jinja2 import Template
from tqdm import tqdm
import re

from llm.glm4 import GLM4
from llm.internlm import InternLM
from llm.llama import Llama
from llm.qwen import Qwen
from schema import build_db_from_spider

parser = argparse.ArgumentParser()
parser.add_argument("--test-set", dest="test_set", type=str, required=True)
parser.add_argument("--table", dest="table", type=str, required=True)
parser.add_argument("--base-model", dest="base_model", type=str, required=True)
parser.add_argument("--bge-model", dest="bge_model", type=str, required=True)

parser.add_argument("--peft-dir", dest="peft_dir", type=str, required=False)
parser.add_argument("--output-result", dest="output_result", type=str, required=True)
parser.add_argument("--save-interval", dest="save_interval", type=int, default=10)
args = parser.parse_args()

input_template_file =  "llm_templates/narrate_input.j2"

with open(input_template_file, "r") as f:
    input_template = Template(f.read())

# model = InternLM(args.base_model, "base-model")
# model = Llama(args.base_model, "base-model")
# model = GLM4(args.base_model, "base-model", peft=args.peft_dir)
model = Qwen(args.base_model, "base-model")

# 加载bge模型
bge_tokenizer = AutoTokenizer.from_pretrained(args.bge_model)
bge_model = AutoModel.from_pretrained(args.bge_model)
bge_model.eval()

def encode(text):
    encoded_input = bge_tokenizer(text, padding=True, truncation=True, return_tensors='pt')
    with torch.no_grad():
        mo = bge_model(**encoded_input)
        embedding = mo[0][:, 0]
        embedding = torch.nn.functional.normalize(embedding, p=2, dim=1) # 归一化，这之后的embedding可以直接计算cosine相似度或点积
    return embedding

# 读数据
with open(args.test_set, 'r') as f:
    test_set = json.load(f)

# 读取数据库 schema 信息
with open(args.table, "r") as f:
    table = json.load(f)

dbs = {}
for db in table:
    dbs[db["db_id"]] = build_db_from_spider(db)

output = []
similarities = []
max = 0
min = 1

with tqdm(test_set) as bar:
    for index, sample in enumerate(test_set):
        sql = sample["query"]
        original = sample["question"]
        db = dbs[sample["db_id"]]


        ddls = [table.to_ddl() for table in db.tables.values()]

        input = input_template.render(
            sql=sql, ddls=ddls)
        
        print(input)

        mo = model.infer(input)

        answer = re.sub(r"\s+", " ", mo).strip()
        answer = answer.strip()

        oe = encode(original)
        ae = encode(answer)
        similarity = oe @ ae.T

        if similarity.item() > max:
            max = similarity.item()
        if similarity.item() < min:
            min = similarity.item()

        res = {
            "sql": sql,
            "original": original,
            "generated": answer,
            "similarity": similarity.item()
        }

        similarities.append(similarity.item())
        avg_similarity = sum(similarities) / len(similarities)

        bar.set_postfix_str(f"avg_similarity: {avg_similarity:.3f}, max_similarity: {max:.3f}, min_similarity: {min:.3f}")

        output.append(res)
        bar.update(1)

        if index % args.save_interval == 0:
            with open(args.output_result, "w", encoding="utf-8") as f:
                json.dump(output, f, ensure_ascii=False, indent=4)

with open(args.output_result, "w", encoding="utf-8") as f:
    json.dump(output, f, ensure_ascii=False, indent=4)




    

