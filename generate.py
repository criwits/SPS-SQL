from re import template
import time

from template import SQLTemplate
from schema import Database, Table, build_db_from_spider
import argparse
from tqdm import tqdm
from itertools import permutations, product
from functools import reduce
import sqlite3
import threading

conns = {}

def generate_sqls_with_timeout(db: Database, db_sqlite_file: str, template: SQLTemplate, timeout: int = 300) -> list[str]:
    finished_event = threading.Event()
    result = []
    def generate():
        nonlocal result
        try:
            result = generate_sqls(db, db_sqlite_file, template)
        except Exception as e:
            result = []
        finished_event.set()
    thread = threading.Thread(target=generate)
    thread.start()
    finished_event.wait(timeout)
    if not finished_event.is_set():
        raise TimeoutError

    return result


def generate_sqls(db: Database, db_sqlite_file: str, template: SQLTemplate,
                  max_literal_length: int = 32, # 最大字面量长度，用于防止诸如 Description 等字段被作为条件
                  no_id_in_literal: bool = True, # 是否在字面量中不包含 ID 及关联的外键，用于防止生成无意义的 SQL，检测 ID 为如下字符串：Id、ID、_id，不直接检测 id 是因为可能会误伤
                  # TODO: 加一些其他的约束
                  ) -> list[str]:
    """
    从 db 中按照 template 的模式和约束生成所有可行 SQL 语句
    """

    # Step 0: 加载 SQLite 数据库
    # conn = sqlite3.connect(db_sqlite_file)
    if db_sqlite_file not in conns:
        conns[db_sqlite_file] = sqlite3.connect(db_sqlite_file)
    conn = conns[db_sqlite_file]

    # Step 1: 按照 template.tables_count 取表
    tables = db.tables
    tables_count = template.tables_count
    if tables_count > len(tables):
        return []
    
    result = []
    
    # 把 tables: dict 转换成 list
    tables = list(tables.values())
    for table_combination in permutations(tables, tables_count):
        # print([x.name for x in table_combination])
        # 已选定表，寻找其中符合约束的列组合
        all_valid = True
        candidate_columns = []
        for table_index, table in enumerate(table_combination):
            # Step 2: 从 table 中取列
            columns = table.columns
            columns_count = len(template.columns[table_index])
            if columns_count > len(columns):
                all_valid = False
                break
            candidate_columns.append(list(permutations(columns, columns_count)))

        if not all_valid:
            continue

        # candidate_columns = [[(col1, col2), (col2, col1)], [(col1, col2, col3), ...], ...]
        new_candidate_columns = []
        # 现在要从 candidate_columns 中取列组合
        for table_index, table in enumerate(table_combination):
            # print(f"现在检查 table {table_index} 的列组合")
            table_candidate_columns = []
            candidates = candidate_columns[table_index]
            table_constraint = template.columns[table_index]
            # print(f"table_constraint: {table_constraint}")

            for columns in candidates:
                # print(f"检查列组合 {columns}")
                all_valid_columns = True

                for column_index, (column_name, column_type) in enumerate(columns):
                    column_constraint = table_constraint[column_index]
                    column_info = table.get_column_info(column_name)

                    if not (column_info[1] == column_constraint["column_type"] or (column_constraint["column_type"] == "number" and column_info[1] in ["integer", "real"])):
                        all_valid_columns = False
                        break

                    if column_info[2] != column_constraint["pk"]:
                        all_valid_columns = False

                    if column_constraint["fk"] and column_constraint["fk_info"] != (-1, -1):
                        if column_info[3] == False: # 没外键
                            all_valid_columns = False
                            break

                if all_valid_columns:
                    table_candidate_columns.append(columns)

            new_candidate_columns.append(table_candidate_columns)



        total_combinations = reduce(lambda x, y: x * y, [len(x) for x in new_candidate_columns], 1)
        for columns_combination in product(*new_candidate_columns):
            # columns_combination = [(col1, col2), (col1, col2, col3), ...]
            # print(columns_combination)
            # 现在逐表逐列检查是否符合约束
            all_valid_tables = True
            for table_index, table in enumerate(table_combination):
                columns = columns_combination[table_index]
                table_constraint = template.columns[table_index]
                all_valid_columns = True
                for column_index, (column_name, column_type) in enumerate(columns):
                    column_constraint = table_constraint[column_index]
                    column_info = table.get_column_info(column_name)

                    # if not (column_info[1] == column_constraint["column_type"] or (column_constraint["column_type"] == "number" and column_info[1] in ["integer", "real"])):
                    #     all_valid_columns = False
                    #     break

                    # if column_info[2] != column_constraint["pk"]:
                    #     all_valid_columns = False
                    #     break

                    if column_constraint["fk"] and column_constraint["fk_info"] != (-1, -1):
                        if column_info[3] == False: # 没外键
                            all_valid_columns = False
                            break
                        # 检查外键索引对不对
                        fk_table_name, fk_column_name = column_info[4]
                        fk_table_id, fk_column_id = column_constraint["fk_info"]
                        if fk_table_name != table_combination[fk_table_id].name or fk_column_name != columns_combination[fk_table_id][fk_column_id][0]:
                            all_valid_columns = False
                            break

                if not all_valid_columns:
                    all_valid_tables = False
                    break

            if all_valid_tables:
                # 生成 SQL
                # table_combinations: (Table, Table, ...)
                def add_quote(text: str):
                    if " " in text:
                        return f"`{text}`"
                    return text
                tables = [add_quote(table.name) for table in table_combination]
                # columns_combinations: ((col1, col2), (col1, col2, col3), ...), col = (name, type)
                columns = [
                    [add_quote(col[0]) for col in table_columns]
                    for table_columns in columns_combination
                ]

                def get_literal(table_name, column_name):
                    cursor = conn.cursor()
                    cursor.execute(f"SELECT {column_name} FROM {table_name} ORDER BY RANDOM() LIMIT 1")
                    result = cursor.fetchone()[0]
                    cursor.close()
                    if isinstance(result, str):
                        return f"'{result}'"
                    return str(result)
                
                def get_fks(table_name, column_name):
                    table = db.get_table(table_name)
                    column = table.get_column_info(column_name)
                    if column is None:
                        return []
                    if column[3] == False:
                        return []
                    else:
                        return [(column[4][0], column[4][1])] # type: ignore
                        
                    

                sql = template.render(tables, columns, get_literal, max_literal_length=max_literal_length, no_id_in_literal=no_id_in_literal, get_fks=get_fks)
                # print(sql)
                if sql is not None:
                    result.append(sql)

    return result

if __name__ == "__main__":
    import argparse
    import random
    import json
    from tqdm import tqdm
    import pickle

    parser = argparse.ArgumentParser()
    parser.add_argument("--table-json", dest="spider_table_json", type=str, default="./data/bird_minidev/MINIDEV/dev_tables.json")
    parser.add_argument("--db-names", dest="db_names", required=False, nargs="*")
    parser.add_argument("--db-dir", dest="db_dir", type=str)
    parser.add_argument("--templates", dest="templates", type=str)
    parser.add_argument("--template-index", dest="template_index", type=int, default=-1)
    parser.add_argument("--output", dest="output", required=False, type=str)
    parser.add_argument("--maximum-sqls-per-template", dest="maximum_sqls_per_template", type=int, default=-1)
    parser.add_argument("--template-limit", dest="template_limit", type=int, default=-1)
    parser.add_argument("--save-interval", dest="save_interval", type=int, default=10)
    args = parser.parse_args()

    with open(args.spider_table_json, "r") as f:
        db_data = json.load(f)

    dbs = []
    for item in tqdm(db_data):
        db = build_db_from_spider(item)
        dbs.append(db)

    used_dbs = []
    if args.db_names is None:
        used_dbs = dbs
    else:
        for db in dbs:
            if db.name in args.db_names:
                used_dbs.append(db)

    with open(args.templates, "rb") as f:
        templates = pickle.load(f)

    templates = [x[0] for x in templates]
    if args.template_limit != -1:
        templates = templates[:args.template_limit]

    generated_sqls = []

    start_time = time.time()
    total_cnt = 0
    with tqdm(total=len(used_dbs) * len(templates)) as pbar:
        for db in used_dbs:

            if args.template_index == -1:
                template_list = templates
            else:
                template_list = [templates[args.template_index]]

            db_path = f"{args.db_dir}/{db.name}/{db.name}.sqlite"

            for template in template_list:
                try:
                    sqls = generate_sqls_with_timeout(db,  db_path, template, 120)
                except Exception as e:
                    sqls = []
                if args.maximum_sqls_per_template != -1:
                    sqls = random.sample(sqls, min(args.maximum_sqls_per_template, len(sqls)))
                # 移除所有含有 <|-1,-1|> 和 (|-1,-1|) 的 SQL
                sqls = [sql for sql in sqls if "<|-1,-1|>" not in sql and "(|-1,-1|)" not in sql]
                for sql in sqls:
                    generated_sqls.append({
                        "db_id": db.name,
                        "template": template.framework,
                        "sql": sql
                    })
                    total_cnt += 1

                    if args.output and total_cnt % args.save_interval == 0:
                        with open(args.output, "w") as f:
                            json.dump(generated_sqls, f, indent=4, ensure_ascii=False)

                pbar.update(1)

    end_time = time.time()

    print(f"SQLs generated: {total_cnt}, Time: {end_time - start_time:.2f}s, Speed: {total_cnt / (end_time - start_time):.2f} SQL/s")

    if args.output:
        with open(args.output, "w") as f:
            json.dump(generated_sqls, f, indent=4, ensure_ascii=False)