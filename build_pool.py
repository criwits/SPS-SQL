from transformers import AutoTokenizer, AutoModel
import torch
import argparse
import json
from tqdm import tqdm
import datasets

parser = argparse.ArgumentParser(description='Validate Narrate')
parser.add_argument('--bge-model', type=str, default='../models/bge-large-en-v1.5', help='model name')
parser.add_argument("--result-files", type=str, help="result file", dest='result_files', nargs='+')
parser.add_argument("--output-dataset-prefix", type=str, help="output dataset prefix", dest='output_dataset_prefix')
args = parser.parse_args()

input = []

for result_file in args.result_files:
    with open(result_file, 'r') as f:
        input.extend(json.load(f))

tokenizer = AutoTokenizer.from_pretrained(args.bge_model)
model = AutoModel.from_pretrained(args.bge_model)
model.eval()

new_data = {}

for item in tqdm(input):
    db_id = item['db_id']
    sql = item['sql']
    question = item['query']

    with torch.no_grad():
        inputs = tokenizer(question, return_tensors="pt", padding=True, truncation=True)
        outputs = model(**inputs)
        embeddings = outputs[0][:, 0][0].cpu().numpy()

    if db_id not in new_data:
        new_data[db_id] = []

    new_data[db_id].append({
        "db_id": db_id,
        "sql": sql,
        "question": question,
        "embeddings": embeddings
    })

# 预览第一个数据
print(new_data[list(new_data.keys())[0]][0])
    

output_datasets = {}
for db_id, data in new_data.items():
    output_datasets[db_id] = datasets.Dataset.from_dict({
        "db_id": [x['db_id'] for x in data],
        "sql": [x['sql'] for x in data],
        "question": [x['question'] for x in data],
        "embeddings": [x['embeddings'] for x in data]
    })

    # output_datasets[db_id].add_faiss_index(column='embeddings')

    # # 测试一下
    # question = "Tell me all gas station ids in China."
    # with torch.no_grad():
    #     inputs = tokenizer(question, return_tensors="pt", padding=True, truncation=True)
    #     outputs = model(**inputs)
    #     embeddings = outputs[0][:, 0][0].cpu().numpy()
    
    # scores, results = output_datasets[db_id].get_nearest_examples('embeddings', embeddings, k=3)
    # print(results)

    output_datasets[db_id].save_to_disk(f"{args.output_dataset_prefix}_{db_id}")

print(" ".join(output_datasets.keys()))