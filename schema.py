class Database:
    def __init__(self, name: str):
        self.name = name
        self.tables = {}

    def __str__(self) -> str:
        return f"{self.name}({', '.join([str(table) for table in self.tables.values()])})"

    def add_table(self, table: "Table"):
        self.tables[table.name] = table
        table.database = self

    def get_table(self, name: str) -> "Table":
        name = name.upper()
        for table_name, table in self.tables.items():
            if table_name.upper() == name:
                return table
        raise ValueError(f"Table {name} not found in database {self.name}")
    
def build_db_from_spider(spider_db_schema: dict) -> Database:
    db = Database(spider_db_schema["db_id"])
    table_columns_indexes = {} # {table_name: [column_index, ...]}
    for index, table_name in enumerate(spider_db_schema["table_names_original"]):
        table = Table(table_name)

        for col_index, col in enumerate(spider_db_schema["column_names_original"]):
            if col[0] == index:
                # 记录下每张表在 Spider 那个 schema 中的列索引
                table_columns_indexes[table_name] = table_columns_indexes.get(table_name, []) + [col_index]
                table.add_column(col[1], spider_db_schema["column_types"][col_index], col_index in spider_db_schema["primary_keys"])
        
        db.add_table(table)

    for foreign_key_pair in spider_db_schema["foreign_keys"]:
        def col_index_to_name(col_index) -> tuple[str, str]: # (table_name, column_name)
            for table_name, indexes in table_columns_indexes.items():
                if col_index in indexes:
                    return table_name, spider_db_schema["column_names_original"][col_index][1]
            return ("", "") # 应该不会走到这里
        def add_foreign_key(a: int, b:int):
            ta, ca = col_index_to_name(a)
            tb, cb = col_index_to_name(b)
            db.get_table(ta).set_foreign_key(ca, db.get_table(tb), cb)

        a = foreign_key_pair[0]
        b = foreign_key_pair[1]

        add_foreign_key(a, b)

    return db



class Table:
    def __init__(self, name: str, database: "Database | None" = None):
        self.database = database
        self.name = name
        self.columns = [] # [(name, type), ...]
        self.primary_keys = [] # [column, ...]
        self.foreign_keys = [] # [(column, table, column), ...]

    def add_column(self, name: str, type: str, primary_key: bool = False):
        self.columns.append((name, type))
        if primary_key:
            self.primary_keys.append(name)

    def set_foreign_key(self, column: str, table: "Table", ref_column: str):
        self.foreign_keys.append((column, table, ref_column))

    def get_column_info(self, column_name: str) -> tuple[str, str, bool, bool, tuple[str, str] | None] | None: # (stdname, type, pk?, fk?, (fktn, fkcn))
        for col, col_type in self.columns:
            if col.upper() == column_name.upper():
                fk = None
                for fk_col, fk_table, fk_ref_col in self.foreign_keys:
                    if fk_col == col:
                        fk = (fk_table.name, fk_ref_col)
                        break
                return col, col_type, col in self.primary_keys, fk is not None, fk
            
        return None # 不存在这个列


    def __str__(self):
        column_strings = []
        fk_list = [x for x, _, _ in self.foreign_keys]
        for col, col_type in self.columns:
            column_strings.append(f"{col}")
        return f"{self.name}({', '.join(column_strings)})"
    
    def to_ddl(self) -> str:
        ddl = f"CREATE TABLE {self.name} (\n"
        for col, col_type in self.columns:
            ddl += f"    {col} {col_type},\n"
        ddl += "    PRIMARY KEY ("
        ddl += ", ".join(self.primary_keys)
        ddl += "),\n"
        for col, table, ref_col in self.foreign_keys:
            ddl += f"    FOREIGN KEY ({col}) REFERENCES {table.name}({ref_col}),\n"
        ddl = ddl[:-2] + "\n);"
        return ddl

if __name__ == "__main__":
    import argparse
    import json
    import random
    parser = argparse.ArgumentParser()
    parser.add_argument("--spider-json", type=str, default="./data/spider/tables.json")
    args = parser.parse_args()

    with open(args.spider_json, "r") as f:
        data = json.load(f)

    index = random.randint(0, len(data) - 1)

    db = build_db_from_spider(data[index])
    print(db)
