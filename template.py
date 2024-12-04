import re
from typing import Callable
from tqdm import tqdm
from schema import Database, build_db_from_spider
from parser.parse import BaseConstraintExpr, ParsedSQL, parse_sql
import argparse
import random
import json
import pickle

def to_upper_snake_case(s: str) -> str:
    return "_".join(s.upper().split())

class SQLTemplate(object):
    def __init__(self, db: Database, sql: ParsedSQL):
        # 从 SQL 构建模板

        # Step 1: 处理列，在 SELECT、JOIN ON、WHERE、GROUP BY、ORDER BY 中使用的所有列都记录下来
        all_used_columns = set()
        ## SELECT
        for column in sql.result_columns:
            # result_columns: [(table_name, column_name), ...]
            all_used_columns.add((column[0], column[1]))

        ## JOIN ON
        def extract_columns_from_constraint(constraint: BaseConstraintExpr | tuple) -> set:
            # 如果是 tuple 是 (OPERATOR, BCE, BCE)，OPERATOR 是 AND 或者 OR
            def _extract_from_bce(bce: BaseConstraintExpr) -> set:
                answer = set()
                left = (bce.table_name, bce.column_name)
                answer.add(left)
                if isinstance(bce.value, tuple) and bce.operator not in ["BETWEEN", "NOT_BETWEEN"]: # 情况特殊，BETWEEN 的 value 也是 tuple，但不是列名
                    right = (bce.value[0], bce.value[1])
                    answer.add(right)
                return answer
            result = set()
            if isinstance(constraint, tuple):
                result |= extract_columns_from_constraint(constraint[1])
                result |= extract_columns_from_constraint(constraint[2])
            else:
                result |= _extract_from_bce(constraint)
            return result
        
        for join in sql.from_join_clauses:
            all_used_columns |= extract_columns_from_constraint(join)

        ## WHERE
        if sql.where_condition:
            all_used_columns |= extract_columns_from_constraint(sql.where_condition)

        ## GROUP BY
        for column in sql.group_by_columns:
            all_used_columns.add(column)

        ## HAVING
        if sql.having_condition:
            all_used_columns |= extract_columns_from_constraint(sql.having_condition)

        ## ORDER BY
        if sql.order_by_column:
            column = sql.order_by_column
            all_used_columns.add((column[0], column[1]))

        # print(all_used_columns)
        if ("*", "*") in all_used_columns:
            all_used_columns.remove(("*", "*"))
            
        # Step 2: 扩展列信息，包括列的类型、是否是主键、是否是外键
        # (table_name, column_name) -> ((table_name, column_name), column_type, pk?, fk?, (fk_table_name, fk_column_name))
        all_columns_info = set()
        for table_name, column_name in all_used_columns:
            table = db.get_table(table_name)
            column_info = table.get_column_info(column_name)
            if column_info is None:
                continue
            column_real_name, column_type, is_pk, is_fk, fk_info = column_info
            all_columns_info.add(((table.name, column_real_name), column_type, is_pk, is_fk, fk_info))

        # print(all_columns_info)

        # Step 3: 提取出所有的表的信息，并给表编号
        if len(all_used_columns) != 0:
            all_tables = set()
            for table_name, _, _, _, _ in all_columns_info:
                all_tables.add(table_name[0])
            all_tables = list(all_tables)
        else:
            # 一种特殊情况，SELECT * FROM table_name，没有显式地指明列，导致 all_used_columns 为空
            # 这时直接从 FROM 中提取表名
            all_tables = [db.get_table(table_name).name for table_name in sql.from_tables]

        def table_to_index(table_name: str) -> int:
            for i, table in enumerate(all_tables):
                if table.upper() == table_name.upper():
                    return i
            return -1 # 不会出现这种情况，调用这个函数的场景是可控的

        self.tables_count = len(all_tables)

        # Step 4: 抽象出列信息
        self.columns = [[] for _ in range(self.tables_count)]
        for table_index, table_name in enumerate(all_tables):
            cnt = 0
            for column_info in all_columns_info:
                if column_info[0][0] == table_name:
                    self.columns[table_index].append({
                        "table_id": table_index,
                        "column_id": cnt,
                        "reference_name": column_info[0], # 构建模板时参考的列名
                        "column_type": column_info[1],
                        "pk": column_info[2],
                        "fk": column_info[3],
                        "fk_info": column_info[4]
                    })
                    cnt += 1

        def ref_name_to_ids(ref_name: tuple) -> tuple:
            for table_index, table in enumerate(self.columns):
                for column_index, column in enumerate(table):
                    if column["reference_name"][0].upper() == ref_name[0].upper() and column["reference_name"][1].upper() == ref_name[1].upper():
                        return table_index, column_index

            return -1, -1

        # 还需要再把 fk_info 也对应地处理成 table_id 和 column_id
        for t in self.columns:
            for c in t:
                if c["fk"]:
                    c["fk_info"] = ref_name_to_ids(c["fk_info"])

        # print(self.columns)

        # Step 5: 生成 SELECT 部分模板
        def select_to_str(item: tuple) -> str:
            # print(item)
            if item[0] == "*":
                column_ids = "*"
            else:
                ids = ref_name_to_ids(item)
                if ids == (-1, -1):
                    print(f"Error: {item} not found in columns")
                    print(f"Columns: {self.columns}")
                    print(f"SQL: {sql.query}")
                column_ids = f"<|{ids[0]},{ids[1]}|>"

            if item[2] is None:
                return column_ids
            else:
                return f"{item[2].upper()}({column_ids})"
        self.select_template = ", ".join([select_to_str(item) for item in sql.result_columns])

        # Step 6: 生成 FROM 部分模板
        def constraint_to_str(constraint: BaseConstraintExpr | tuple) -> str:
            def _process_bce(bce: BaseConstraintExpr) -> str:
                if bce.column_name == "*":
                    left_ids = "*"
                else:
                    ids = ref_name_to_ids((bce.table_name, bce.column_name))
                    left_ids = f"<|{ids[0]},{ids[1]}|>"
                if bce.aggregate_func is not None:
                    left = f"{bce.aggregate_func.upper()}({left_ids})"
                else:
                    left = left_ids
                if isinstance(bce.value, tuple):
                    if bce.operator not in ["BETWEEN", "NOT_BETWEEN"]:
                        right = ref_name_to_ids((bce.value[0], bce.value[1]))
                        right = f"<|{right[0]},{right[1]}|>"
                    else:
                        right = f"(|{ids[0]},{ids[1]}|) AND (|{ids[0]},{ids[1]}|)_2" # TODO: 字面量处理
                else:
                    if bce.aggregate_func is None:
                        right = f"(|{ids[0]},{ids[1]}|)"
                    else:
                        right = f"{to_upper_snake_case(bce.aggregate_func)}((|{ids[0]},{ids[1]}|))"

                return f"{left} {bce.operator.replace('_', ' ')} {right}"
            if isinstance(constraint, tuple):
                left = constraint_to_str(constraint[1])
                right = constraint_to_str(constraint[2])
                return f"({left} {constraint[0]} {right})"
            else:
                return _process_bce(constraint)
            

        self.from_template = f"[|{table_to_index(sql.from_tables[0])}|]"
        if len(sql.from_tables) > 1:
            for idx, table in enumerate(sql.from_tables[1:]):
                self.from_template += f" JOIN [|{table_to_index(table)}|] ON {constraint_to_str(sql.from_join_clauses[idx])}"

        # print(self.from_template)

        # Step 7: 生成 WHERE 部分模板
        if sql.where_condition:
            self.where_template = constraint_to_str(sql.where_condition)
        else:
            self.where_template = ""

        # Step 8: 生成 GROUP BY 部分模板
        self.group_by_template = ", ".join([f"<|{ref_name_to_ids(item)[0]},{ref_name_to_ids(item)[1]}|>" for item in sql.group_by_columns])

        # Step 9: 生成 HAVING 部分模板
        if sql.having_condition:
            self.having_template = constraint_to_str(sql.having_condition)
        else:
            self.having_template = ""

        # Step 10: 生成 ORDER BY 部分模板
        if sql.order_by_column:
            order_by_column = sql.order_by_column
            if order_by_column[0] == "*":
                order_by_column_ids = "*"
            else:
                ids = ref_name_to_ids(order_by_column)
                order_by_column_ids = f"<|{ids[0]},{ids[1]}|>"

            if order_by_column[2] is None:
                self.order_by_template = order_by_column_ids
            else:
                self.order_by_template = f"{order_by_column[2].upper()}({order_by_column_ids})"

            self.order_by_template += f" {sql.ordering}"
        else:
            self.order_by_template = ""

        # Step 11: 生成 LIMIT 部分模板
        if sql.limit:
            self.limit_template = str(sql.limit)
        else:
            self.limit_template = ""

        # Step 12: 合成总模板
        self.template = f"SELECT {self.select_template}"
        self.template += f" FROM {self.from_template}"
        if self.where_template:
            self.template += f" WHERE {self.where_template}"
        if self.group_by_template:
            self.template += f" GROUP BY {self.group_by_template}"
        if self.having_template:
            self.template += f" HAVING {self.having_template}"
        if self.order_by_template:
            self.template += f" ORDER BY {self.order_by_template}"
        if self.limit_template:
            self.template += f" LIMIT {self.limit_template}"

        # print(self.template)

        # Step 13: 抽象化模板，用于进行比较
        self.framework = self.template
        for i in [
            (r"\[\|.*?\|\]", "[|TABLE|]"),
            (r"<\|.*?\|>", "<|COLUMN|>"),
            (r"\(\|.*?\|\)", "(|VALUE|)"),
        ]:
            self.framework = re.sub(i[0], i[1], self.framework)

        # Step 14: 把 columns 中的 reference_name 去掉，不再需要它了
        for table in self.columns:
            for column in table:
                column.pop("reference_name")

        # 模板生成完毕


    def __eq__(self, __value: "SQLTemplate") -> bool:
        return self.framework == __value.framework
    
    def __hash__(self) -> int:
        return hash(self.framework)
        
    def render(self, tables: list, columns: list[list], get_literal: Callable,
               max_literal_length: int = 32, # 最大字面量长度，用于防止诸如 Description 等字段被作为条件
               no_id_in_literal: bool = True, # 是否在字面量中不包含 ID 及关联的外键，用于防止生成无意义的 SQL，检测 ID 为如下字符串：Id、ID、_id，不直接检测 id 是因为可能会误伤
               get_fks: Callable | None = None # 用于获取外键信息
               ) -> str | None:
        """
        tables: ["Students", "Courses"]
        columns: [["Id", "name"], ["Id", "professor", "credit"]]
        对应着 [|0|] = Students, [|1|] = Courses
        <|0,0|> = Students.Id, <|1,1|> = Courses.professor, ...
        这样直接用 self.template 做文本替换就行了
        """

        result = self.template
        literals = {}
        for table_index, table_name in enumerate(tables):
            result = result.replace(f"[|{table_index}|]", table_name)
            for column_index, column_name in enumerate(columns[table_index]):
                if f"(|{table_index},{column_index}|)" in result:
                    # 处理字面量
                    if no_id_in_literal:
                        for id_str in ["Id", "ID", "_id"]:
                            if id_str in column_name:
                                return None
                            if get_fks is not None:
                                for fk in get_fks(table_name, column_name):
                                    if id_str in fk[2]:
                                        return None
                    literal = get_literal(table_name, column_name)
                    if len(literal) > max_literal_length:
                        return None
                    literals[f"(|{table_index},{column_index}|)"] = literal

                    result = result.replace(f"(|{table_index},{column_index}|)", literal)
                    
                result = result.replace(f"<|{table_index},{column_index}|>", f"{table_name}.{column_name}")
                # result = result.replace(f"(|{table_index},{column_index}|)", get_literal(table_name, column_name)) # 处理字面量
        return result


    
if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--sql-json", dest="spider_sql_json", type=str, nargs="+")
    parser.add_argument("--table-json", dest="spider_table_json", type=str, default="./data/spider/tables.json")
    parser.add_argument("--output", dest="output", type=str)
    parser.add_argument("--limit", dest="limit", type=int, default=100)
    args = parser.parse_args()

    sql_data = []
    for sql_json in args.spider_sql_json:
        with open(sql_json, "r") as f:
            sql_data.extend(json.load(f))

    with open(args.spider_table_json, "r") as f:
        db_data = json.load(f)

    dbs = dict()
    for item in tqdm(db_data):
        db = build_db_from_spider(item)
        dbs[db.name] = db

    templates = dict()
    with tqdm(total=len(sql_data)) as pbar:
        cnt = 0
        for item in sql_data:
            try:
                db = dbs[item["db_id"]]
                sql = parse_sql(item["query"])
                template = SQLTemplate(db, sql)

                templates.setdefault(hash(template), [template, 0])
                templates[hash(template)][1] += 1
            except:
                pass
            cnt += 1
            pbar.set_postfix_str(f"Templates: {len(templates)}, Processed: {cnt}")
            pbar.update(1)

    # 按照出现次数排序
    sorted_templates = sorted(templates.items(), key=lambda x: x[1][1], reverse=True)

    sorted_templates = [x[1] for x in sorted_templates[:args.limit]]
 
    with open(args.output, "wb") as f:
        pickle.dump(sorted_templates, f)

    
