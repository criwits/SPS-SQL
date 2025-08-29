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
        for table_name, table in self.tables.items():
            if table_name.upper() == name.upper():
                return table
        raise ValueError(f"Table {name} not found in database {self.name}")
    
    def get_column(self, name: str, tables: list[str] | None = []) -> tuple["Table", str]:
        name = name.upper()
        if tables:
            searched_tables = [self.get_table(x) for x in tables]
        else:
            searched_tables = self.tables.values()
        for table in searched_tables:
            for col, col_type in table.columns:
                if col.upper() == name:
                    return table, col
        raise ValueError(f"Column {name} not found in database {self.name}")

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

    def to_string(self, filter: list[str] | None = None, max_column_count: int | None = None, max_column_name_length: int | None = None) -> str:
        column_strings = []
        col_cnt = 0
        fk_list = [x for x, _, _ in self.foreign_keys]
        for index, (col, col_type) in enumerate(self.columns):
            if (filter is None or col in filter) and (max_column_count is None or col_cnt < max_column_count) and (max_column_name_length is None or len(col) <= max_column_name_length):
                column_strings.append(f"{col}:{col_type}{' [PK]' if col in self.primary_keys else ''}{' [FK]' if col in fk_list else ''}")
                col_cnt += 1
                # column_strings.append(f"{col}")
        
        return f"{self.name}({', '.join(column_strings)}{', ...' if max_column_count is not None and len(self.columns) > max_column_count else ''})"
    
    def list_fks(self, filter: list[tuple] | None = None) -> list[str]:
        result = []

        def column_in(candidate: tuple[str, str], filter: list[tuple[str, str]]) -> bool:
            for x in filter:
                if x[0].upper() == candidate[0].upper() and x[1].upper() == candidate[1].upper():
                    return True
            return False

        for col, table, ref_col in self.foreign_keys:
            this_table_column = (self.name, col)
            that_table_column = (table.name, ref_col)
            if filter is None or (column_in(this_table_column, filter) and column_in(that_table_column, filter)):
                result.append(f"{self.name}.{col} -> {table.name}.{ref_col}")
        return result


    def to_ddl(self, column_filter: list[str] | None = None, values_dict: dict | None = None) -> str:
        def if_str_add_quotes(s) -> str:
            if isinstance(s, str):
                if len(s) > 48:
                    return f'"{s[:6]}...{s[-6:]}"'
                else:
                    return f'"{s}"'
            else:
                s = str(s)
                if len(s) > 64:
                    return f'{s[:12]}...{s[-12:]}'
                else:
                    return s

        ddl = f"CREATE TABLE {self.name} (\n"
        for col, col_type in self.columns:
            if column_filter is None or col in column_filter:
                ddl += f"    {col} {col_type},"
                if values_dict is not None and col in values_dict:
                    ddl += f" -- {', '.join([if_str_add_quotes(x) for x in values_dict[col]])}..."
                ddl += "\n"
        pks = [x for x in self.primary_keys if column_filter is None or x in column_filter]
        if pks:
            ddl += "    PRIMARY KEY ("
            ddl += ", ".join(pks)
            ddl += "),\n"

        for col, table, ref_col in self.foreign_keys:
            if column_filter is None or col in column_filter:
                ddl += f"    FOREIGN KEY ({col}) REFERENCES {table.name}({ref_col}),\n"
        ddl = ddl[:-2] + "\n);"
        return ddl


    def __str__(self):
        return self.to_string()
   
def build_db_from_spider_bird(spider_db_schema: dict) -> Database:
    db = Database(spider_db_schema["db_id"])
    table_columns_indexes = {} # {table_name: [column_index, ...]}
    for index, table_name in enumerate(spider_db_schema["table_names_original"]):
        table = Table(table_name)

        for col_index, col in enumerate(spider_db_schema["column_names_original"]):
            if col[0] == index:
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

