from antlr4 import CommonTokenStream, InputStream
from parser.utils import *
from parser.grammar.gen.SQLiteLexer import SQLiteLexer
from parser.grammar.gen.SQLiteParser import SQLiteParser

def extract_table_column(table_column: RuleContext) -> tuple[str | None, str]:
    table_name = find_children_with_type(table_column, SQLiteParser.Table_nameContext)
    if len(table_name) == 0:
        table_name = None
    else:
        table_name = table_name[0].getText().strip()
    column_name = find_children_with_type(table_column, SQLiteParser.Column_nameContext)[0].getText().strip()
    return table_name, column_name

def extract_function_table_column(function_table_column: RuleContext) -> tuple[str | None, str, str]:
    function_name = function_table_column.getChild(0).getText().strip()
    if "*" in function_table_column.getText():
        return ("*", "*", function_name)
    else:
        table_column = find_children_with_type(function_table_column, SQLiteParser.Table_columnContext)[0]
        table_name, column_name = extract_table_column(table_column)
        return (table_name, column_name, function_name)

def extract_table_or_subquery(table_or_subquery: RuleContext) -> tuple[str, str | None]:
    table_name_with_alias = find_children_with_type(table_or_subquery, SQLiteParser.Table_name_with_aliasContext)[0]
    table_name = table_name_with_alias.getChild(0).getText().strip()
    alias = find_children_with_type(table_name_with_alias, SQLiteParser.Table_aliasContext)
    if len(alias) == 0:
        alias = None
    else:
        alias = alias[0].getText().strip()
    return table_name, alias

class BaseConstraintExpr():
    def __init__(self, table_name: str | None, column_name: str, operator: str, value, aggregate_func: str | None = None):
        self.table_name = table_name
        self.column_name = column_name
        self.aggregate_func = aggregate_func
        self.operator = operator
        self.value = value

    def __str__(self):
        if self.aggregate_func is not None:
            return f"({self.aggregate_func}({self.table_name}.{self.column_name}) {self.operator} {self.value})"
        return f"({self.table_name}.{self.column_name} {self.operator} {self.value})"
    
def alter_text_by_table(alias, alt_table) -> str:
    for a, b in alt_table:
        if alias == a or (a is not None and alias is not None and alias.upper() == a.upper()):
            return b
    return alias

def extract_base_constraint_expr(constraint_expr: RuleContext, alt_table: list = []) -> BaseConstraintExpr:
    def alt_text(text):
        return alter_text_by_table(text, alt_table)
    first_item = constraint_expr.getChild(0)
    if isinstance(first_item, SQLiteParser.Table_columnContext):
        table_name, column_name = extract_table_column(first_item)
        aggregate_func = None
    else:
        table_name, column_name, aggregate_func = extract_function_table_column(first_item)
    condition = constraint_expr.getChild(1)
    if isinstance(condition, SQLiteParser.Compare_operatorContext):
        operator = condition.getText().strip()
        value = constraint_expr.getChild(2)
        # 注意最后有可能是 table_column 或者字面量
        if isinstance(value, SQLiteParser.Table_columnContext):
            value_table_name, value_column_name = extract_table_column(value)
            return BaseConstraintExpr(alt_text(table_name), column_name, operator, (alt_text(value_table_name), value_column_name), aggregate_func)
        else:
            # return table_name, column_name, operator, value.getText().strip()
            literal = value.getText().strip()
            # 如果是字符串，去掉引号，单引号或者双引号
            if literal[0] == "'" or literal[0] == '"':
                literal = literal[1:-1]
            return BaseConstraintExpr(alt_text(table_name), column_name, operator, literal, aggregate_func)
            # return BaseConstraintExpr(alt_text(table_name), column_name, operator, value.getText().strip())
    elif isinstance(condition, SQLiteParser.Is_null_operatorContext):
        if condition.getChildCount == 1:
            return BaseConstraintExpr(alt_text(table_name), column_name, "IS_NULL", None)
        else:
            # return table_name, column_name, "IS_NOT_NULL"
            return BaseConstraintExpr(alt_text(table_name), column_name, "IS_NOT_NULL", None)
    elif isinstance(condition, SQLiteParser.Between_operatorContext):
        if condition.getChildCount == 1:
            op = "BETWEEN"
        else:
            op = "NOT_BETWEEN"
        value1 = constraint_expr.getChild(2)
        value2 = constraint_expr.getChild(4) # 3 是 AND
        # return table_name, column_name, op, value1.getText().strip(), value2.getText().strip()
        return BaseConstraintExpr(alt_text(table_name), column_name, op, (value1.getText().strip(), value2.getText().strip()))
    elif isinstance(condition, SQLiteParser.Match_like_operatorContext):
    # else:
        if condition.getChildCount == 1:
            op = "LIKE"
        else:
            op = "NOT_LIKE"
        value = constraint_expr.getChild(2)
        # return table_name, column_name, op, value.getText().strip()
        return BaseConstraintExpr(alt_text(table_name), column_name, op, value.getText().strip())
    # elif isinstance(condition, SQLiteParser.In_operatorContext):
    else:
        if condition.getChildCount == 1:
            op = "IN"
        else:
            op = "NOT_IN"
        sub_query = constraint_expr.getChild(3) # 2 是 OPEN_PAREN
        return BaseConstraintExpr(alt_text(table_name), column_name, op, extract_select_stmt("", sub_query))
    

def extract_constraint_expr(constraint_expr: RuleContext, alt_table: list = []) -> BaseConstraintExpr | tuple:
    def _extract_constraint_expr(expr: RuleContext) -> BaseConstraintExpr | tuple:
        first_child = expr.getChild(0)
        if isinstance(first_child, SQLiteParser.Base_constraint_exprContext):
            return extract_base_constraint_expr(first_child, alt_table)
        if isinstance(first_child, SQLiteParser.Constraint_exprContext):
            first = _extract_constraint_expr(first_child)
            second = _extract_constraint_expr(expr.getChild(2))
            op = expr.getChild(1).getText().strip().upper()
            return op, first, second
        if first_child.getText().strip().upper() == "NOT":
            return "NOT", _extract_constraint_expr(expr.getChild(1))
        if first_child.getText().strip().upper() == "(":
            return _extract_constraint_expr(expr.getChild(1))
        return ()
    return _extract_constraint_expr(constraint_expr)

class ParsedSQL(object):
    def __init__(
            self,
            query: str,
            result_columns: list[tuple[str, str, str | None]],
            from_tables: list[str],
            from_join_clauses: list[tuple | BaseConstraintExpr],
            where_condition: tuple | BaseConstraintExpr | None,
            group_by_columns: list[tuple[str, str]],
            having_condition: tuple | BaseConstraintExpr | None,
            order_by_column: tuple[str, str, str | None] | None = None,
            ordering: str | None = None,
            limit: int | None = None
        ) -> None:
        self.query = query
        self.result_columns = result_columns
        self.from_tables = from_tables
        self.from_join_clauses = from_join_clauses
        self.where_condition = where_condition
        self.group_by_columns = group_by_columns
        self.having_condition = having_condition
        self.order_by_column = order_by_column
        self.ordering = ordering
        self.limit = limit


def extract_select_stmt(query: str, select_stmt: RuleContext) -> ParsedSQL:
    # select_stmt: common_table_stmt? select_core (compound_operator select_core)* order_by_stmt? limit_stmt?


    select_core_stmts = get_children_with_type(select_stmt, SQLiteParser.Select_coreContext)

    # 就先只处理第一个 select_core
    first_select_core = select_core_stmts[0]

    # 处理 FROM 部分
    from_tables = [] # [(table_name, alias), ...]
    from_join_clauses = [] # 所有的 JOIN ON 条件

    l_from_tables = get_children_with_type(first_select_core, SQLiteParser.From_tablesContext)[0] # 一定有，语法保证
    all_table_or_subqueries = find_children_with_type(l_from_tables, SQLiteParser.Table_or_subqueryContext) # 会把 JOIN 里面的也自动提取出来
    all_constraints = find_children_with_type(l_from_tables, SQLiteParser.Constraint_exprContext)
    for table_or_subquery in all_table_or_subqueries:
        table_name, alias = extract_table_or_subquery(table_or_subquery)
        from_tables.append((table_name, alias))

    # 因为有同一个表以不同别名出现的情况，在原名后面加上 #1, #2, ... 以区分，没有多个别名的表不处理
    # TODO

    alt_table = [(b, a) for a, b in from_tables]
    def alt_text(text):
        return alter_text_by_table(text, alt_table)
    
    from_tables = [a for a, _ in from_tables] # 只保留 table_name

    for constraint in all_constraints:
        from_join_clauses.append(extract_constraint_expr(constraint, alt_table))

    # 处理 SELECT 部分
    result_columns = [] # [(table_name, column_name, func), ...]
    l_result_columns = find_children_with_type(first_select_core, SQLiteParser.Result_columnContext)
    for column in l_result_columns:
        if column.getText().strip() == "*":
            result_columns.append(("*", "*", None))
            break # 如果有 *，那么就不需要再处理了，因为所有列都给它选了
        elif isinstance(column.getChild(0), SQLiteParser.Table_columnContext):
            table_column = column.getChild(0)
            table_name, column_name = extract_table_column(table_column)
            table_name = alt_text(table_name)
            result_columns.append((table_name, column_name, None))
        elif isinstance(column.getChild(0), SQLiteParser.Function_table_columnContext):
            function_table_column = column.getChild(0)
            table_name, column_name, function_name = extract_function_table_column(function_table_column)
            table_name = alt_text(table_name)
            result_columns.append((table_name, column_name, function_name))

    # 处理 WHERE 部分
    where_constraint_exprs = get_children_with_type(first_select_core, SQLiteParser.Where_exprContext)
    if len(where_constraint_exprs) == 0:
        where_condition = None
    else:
        constraint_expr = where_constraint_exprs[0].getChild(0)
        where_condition = extract_constraint_expr(constraint_expr, alt_table)

    # 处理 GROUP BY 部分
    group_by_columns = []
    l_group_by_columns = get_children_with_type(first_select_core, SQLiteParser.Group_by_columnsContext)
    if len(l_group_by_columns) != 0:
        columns = find_children_with_type(l_group_by_columns[0], SQLiteParser.Table_columnContext)
        for column in columns:
            table_name, column_name = extract_table_column(column)
            table_name = alt_text(table_name)
            group_by_columns.append((table_name, column_name))

    having = get_children_with_type(first_select_core, SQLiteParser.Having_exprContext)
    if len(having) == 0:
        having_expr = None
    else:
        constraint_expr = having[0].getChild(0)
        having_expr = extract_constraint_expr(constraint_expr, alt_table)


    # ORDER BY 部分
    order_by_stmts = get_children_with_type(select_stmt, SQLiteParser.Order_by_stmtContext)
    if len(order_by_stmts) == 0:
        order_by_column = None
        ordering = None
    else:
        order_by_stmt = order_by_stmts[0]
        ordering_term = order_by_stmt.getChild(2) # 0: ORDER 1: BY
        if isinstance(ordering_term.getChild(0), SQLiteParser.Table_columnContext):
            table_name, column_name = extract_table_column(ordering_term.getChild(0))
            table_name = alt_text(table_name)
            order_by_column = (table_name, column_name, None)
        else:
            function_table_column = ordering_term.getChild(0)
            table_name, column_name, function_name = extract_function_table_column(function_table_column)
            table_name = alt_text(table_name)
            order_by_column = (table_name, column_name, function_name)
        
        if ordering_term.getChildCount() == 2:
            # 有指定 ASC 或者 DESC
            ordering = ordering_term.getChild(1).getText().strip().upper()
        else:
            # 没有指定 ASC 或者 DESC，默认是 ASC
            ordering = "ASC"
            

    limit_stmts = get_children_with_type(select_stmt, SQLiteParser.Limit_stmtContext)
    if len(limit_stmts) == 0:
        limit = None
    else:
        limit_stmt = limit_stmts[0]
        limit = int(limit_stmt.getChild(1).getText().strip())


    return ParsedSQL(
        query=query,
        result_columns=result_columns,
        from_tables=from_tables,
        from_join_clauses=from_join_clauses,
        where_condition=where_condition,
        group_by_columns=group_by_columns,
        having_condition=having_expr,
        order_by_column=order_by_column,
        ordering = ordering,
        limit=limit
    )
    

def parse_sql(query: str) -> ParsedSQL:
    lexer = SQLiteLexer(InputStream(query))
    stream = CommonTokenStream(lexer)
    parser = SQLiteParser(stream)
    tree = parser.parse()
    tree = get_children_with_type(tree, SQLiteParser.Select_stmtContext)[0]
    return extract_select_stmt(query, tree)

if __name__ == "__main__":
    import argparse
    import json
    parser = argparse.ArgumentParser()
    parser.add_argument("--spider-json", type=str, default="../data/spider/train_spider.json")
    args = parser.parse_args()

    with open(args.spider_json, "r") as f:
        data = json.load(f)

    for item in data:
        question = item["question"]
        query = item["query"]
        
        try:
            result = parse_sql(query)
            print(question)
            print(query)
            print("Result columns:", result.result_columns)
            print("From Tables:", result.from_tables)
            print("Join clauses:", [str(x) for x in result.from_join_clauses])
            print("Where condition:", result.where_condition)
            print("Group by:", result.group_by_columns)
            print("Having:", result.having_condition)
            print("Order by:", result.order_by_column)
            print("Ordering:", result.ordering)
            print("Limit:", result.limit)

            print("")
        except Exception as e:
            print(question)
            print(query)
            print(e)
                
            print("")


