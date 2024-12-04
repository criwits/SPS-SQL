generate-antlr:
	antlr4 -Dlanguage=Python3 -o parser/grammar/gen -Xexact-output-dir parser/grammar/SQLiteLexer.g4
	antlr4 -Dlanguage=Python3 -o parser/grammar/gen -Xexact-output-dir parser/grammar/SQLiteParser.g4