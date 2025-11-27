#include "poached_extension.hpp"
#include "duckdb/parser/parser.hpp"
#include "duckdb/parser/keyword_helper.hpp"
#include "duckdb/parser/statement/select_statement.hpp"
#include "duckdb/parser/query_node/select_node.hpp"
#include "duckdb/parser/tableref/basetableref.hpp"
#include "duckdb/parser/tableref/joinref.hpp"
#include "duckdb/parser/tableref/subqueryref.hpp"
#include "duckdb/parser/tableref/table_function_ref.hpp"
#include "duckdb/parser/expression/function_expression.hpp"
#include "duckdb/parser/expression/comparison_expression.hpp"
#include "duckdb/parser/expression/conjunction_expression.hpp"
#include "duckdb/parser/expression/columnref_expression.hpp"
#include "duckdb/parser/expression/constant_expression.hpp"
#include "duckdb/function/table_function.hpp"
#include "duckdb/function/scalar_function.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb/main/client_context.hpp"
#include "duckdb/main/database.hpp"
#include "duckdb/main/connection.hpp"
#include "duckdb/planner/planner.hpp"
#include "duckdb/planner/bound_result_modifier.hpp"

#include <cstring>
#include <sstream>

namespace duckdb {

// ============================================================================
// tokenize_sql(query) - returns tokens with byte positions and categories
// ============================================================================

struct TokenizeSqlBindData : public TableFunctionData {
	string query;
	vector<SimplifiedToken> tokens;
};

struct TokenizeSqlState : public GlobalTableFunctionState {
	idx_t current_idx = 0;
};

static unique_ptr<FunctionData> TokenizeSqlBind(ClientContext &context, TableFunctionBindInput &input,
                                                 vector<LogicalType> &return_types, vector<string> &names) {
	auto result = make_uniq<TokenizeSqlBindData>();
	result->query = input.inputs[0].GetValue<string>();
	result->tokens = Parser::Tokenize(result->query);

	return_types.push_back(LogicalType::BIGINT);
	names.push_back("byte_position");
	return_types.push_back(LogicalType::VARCHAR);
	names.push_back("category");

	return std::move(result);
}

static unique_ptr<GlobalTableFunctionState> TokenizeSqlInit(ClientContext &context, TableFunctionInitInput &input) {
	return make_uniq<TokenizeSqlState>();
}

static string TokenTypeToString(SimplifiedTokenType type) {
	switch (type) {
	case SimplifiedTokenType::SIMPLIFIED_TOKEN_IDENTIFIER: return "IDENTIFIER";
	case SimplifiedTokenType::SIMPLIFIED_TOKEN_NUMERIC_CONSTANT: return "NUMERIC_CONSTANT";
	case SimplifiedTokenType::SIMPLIFIED_TOKEN_STRING_CONSTANT: return "STRING_CONSTANT";
	case SimplifiedTokenType::SIMPLIFIED_TOKEN_OPERATOR: return "OPERATOR";
	case SimplifiedTokenType::SIMPLIFIED_TOKEN_KEYWORD: return "KEYWORD";
	case SimplifiedTokenType::SIMPLIFIED_TOKEN_COMMENT: return "COMMENT";
	default: return "ERROR";
	}
}

static void TokenizeSqlFunc(ClientContext &context, TableFunctionInput &data_p, DataChunk &output) {
	auto &bind_data = data_p.bind_data->Cast<TokenizeSqlBindData>();
	auto &state = data_p.global_state->Cast<TokenizeSqlState>();

	idx_t count = 0;
	while (state.current_idx < bind_data.tokens.size() && count < STANDARD_VECTOR_SIZE) {
		auto &token = bind_data.tokens[state.current_idx];
		output.data[0].SetValue(count, Value::BIGINT(token.start));
		output.data[1].SetValue(count, Value(TokenTypeToString(token.type)));
		count++;
		state.current_idx++;
	}
	output.SetCardinality(count);
}

// ============================================================================
// Scalar functions
// ============================================================================

static void IsValidSqlFunc(DataChunk &args, ExpressionState &state, Vector &result) {
	UnaryExecutor::Execute<string_t, bool>(args.data[0], result, args.size(), [&](string_t query) {
		try {
			Parser parser;
			parser.ParseQuery(query.GetString());
			return true;
		} catch (...) {
			return false;
		}
	});
}

static void SqlErrorMessageFunc(DataChunk &args, ExpressionState &state, Vector &result) {
	auto &input = args.data[0];
	auto count = args.size();

	UnifiedVectorFormat vdata;
	input.ToUnifiedFormat(count, vdata);
	auto input_data = UnifiedVectorFormat::GetData<string_t>(vdata);

	result.SetVectorType(VectorType::FLAT_VECTOR);
	auto result_data = FlatVector::GetData<string_t>(result);
	auto &result_validity = FlatVector::Validity(result);

	for (idx_t i = 0; i < count; i++) {
		auto idx = vdata.sel->get_index(i);
		if (!vdata.validity.RowIsValid(idx)) {
			result_validity.SetInvalid(i);
			continue;
		}

		auto query_str = input_data[idx].GetString();
		try {
			Parser parser;
			parser.ParseQuery(query_str);
			// Valid SQL - return NULL
			result_validity.SetInvalid(i);
		} catch (const Exception &e) {
			result_data[i] = StringVector::AddString(result, e.what());
		} catch (const std::exception &e) {
			result_data[i] = StringVector::AddString(result, e.what());
		}
	}
}

static void NumStatementsFunc(DataChunk &args, ExpressionState &state, Vector &result) {
	UnaryExecutor::Execute<string_t, int64_t>(args.data[0], result, args.size(), [&](string_t query) {
		try {
			Parser parser;
			parser.ParseQuery(query.GetString());
			return static_cast<int64_t>(parser.statements.size());
		} catch (...) {
			return static_cast<int64_t>(0);
		}
	});
}

static void IsKeywordFunc(DataChunk &args, ExpressionState &state, Vector &result) {
	UnaryExecutor::Execute<string_t, bool>(args.data[0], result, args.size(), [&](string_t str) {
		return KeywordHelper::IsKeyword(str.GetString());
	});
}

// ============================================================================
// sql_keywords() - list all SQL keywords
// ============================================================================

struct SqlKeywordsBindData : public TableFunctionData {
	vector<string> keywords;
};

struct SqlKeywordsState : public GlobalTableFunctionState {
	idx_t current_idx = 0;
};

static unique_ptr<FunctionData> SqlKeywordsBind(ClientContext &context, TableFunctionBindInput &input,
                                                 vector<LogicalType> &return_types, vector<string> &names) {
	auto result = make_uniq<SqlKeywordsBindData>();
	auto keywords = Parser::KeywordList();
	for (auto &kw : keywords) {
		result->keywords.push_back(kw.name);
	}

	return_types.push_back(LogicalType::VARCHAR);
	names.push_back("keyword");

	return std::move(result);
}

static unique_ptr<GlobalTableFunctionState> SqlKeywordsInit(ClientContext &context, TableFunctionInitInput &input) {
	return make_uniq<SqlKeywordsState>();
}

static void SqlKeywordsFunc(ClientContext &context, TableFunctionInput &data_p, DataChunk &output) {
	auto &bind_data = data_p.bind_data->Cast<SqlKeywordsBindData>();
	auto &state = data_p.global_state->Cast<SqlKeywordsState>();

	idx_t count = 0;
	while (state.current_idx < bind_data.keywords.size() && count < STANDARD_VECTOR_SIZE) {
		output.data[0].SetValue(count, Value(bind_data.keywords[state.current_idx]));
		count++;
		state.current_idx++;
	}
	output.SetCardinality(count);
}

// ============================================================================
// parse_statements(query) - parse multi-statement SQL
// ============================================================================

struct ParseStatementsBindData : public TableFunctionData {
	string query;
	vector<unique_ptr<SQLStatement>> statements;
	string error;
};

struct ParseStatementsState : public GlobalTableFunctionState {
	idx_t current_idx = 0;
};

static unique_ptr<FunctionData> ParseStatementsBind(ClientContext &context, TableFunctionBindInput &input,
                                                     vector<LogicalType> &return_types, vector<string> &names) {
	auto result = make_uniq<ParseStatementsBindData>();
	result->query = input.inputs[0].GetValue<string>();

	try {
		Parser parser;
		parser.ParseQuery(result->query);
		result->statements = std::move(parser.statements);
	} catch (const Exception &e) {
		result->error = e.what();
	}

	return_types.push_back(LogicalType::BIGINT);
	names.push_back("stmt_index");
	return_types.push_back(LogicalType::VARCHAR);
	names.push_back("stmt_type");
	return_types.push_back(LogicalType::VARCHAR);
	names.push_back("error");
	return_types.push_back(LogicalType::BIGINT);
	names.push_back("param_count");

	return std::move(result);
}

static unique_ptr<GlobalTableFunctionState> ParseStatementsInit(ClientContext &context, TableFunctionInitInput &input) {
	return make_uniq<ParseStatementsState>();
}

static void ParseStatementsFunc(ClientContext &context, TableFunctionInput &data_p, DataChunk &output) {
	auto &bind_data = data_p.bind_data->Cast<ParseStatementsBindData>();
	auto &state = data_p.global_state->Cast<ParseStatementsState>();

	idx_t count = 0;

	if (!bind_data.error.empty() && state.current_idx == 0) {
		output.data[0].SetValue(count, Value::BIGINT(0));
		output.data[1].SetValue(count, Value());
		output.data[2].SetValue(count, Value(bind_data.error));
		output.data[3].SetValue(count, Value::BIGINT(0));
		count++;
		state.current_idx++;
		output.SetCardinality(count);
		return;
	}

	while (state.current_idx < bind_data.statements.size() && count < STANDARD_VECTOR_SIZE) {
		auto &stmt = bind_data.statements[state.current_idx];
		output.data[0].SetValue(count, Value::BIGINT(state.current_idx));
		output.data[1].SetValue(count, Value(StatementTypeToString(stmt->type)));
		output.data[2].SetValue(count, Value());
		output.data[3].SetValue(count, Value::BIGINT(0));
		count++;
		state.current_idx++;
	}
	output.SetCardinality(count);
}

// ============================================================================
// Helper: Extract table references from a statement
// ============================================================================

struct ExtractedTable {
	string schema;
	string table;
	string context; // "FROM", "JOIN", "INSERT", etc.
};

static void ExtractTablesFromTableRef(duckdb::TableRef *tref, vector<ExtractedTable> &tables, const string &ctx);

static void ExtractTablesFromQueryNode(QueryNode *node, vector<ExtractedTable> &tables) {
	if (!node) return;

	if (node->type == QueryNodeType::SELECT_NODE) {
		auto &select = node->Cast<SelectNode>();
		if (select.from_table) {
			ExtractTablesFromTableRef(select.from_table.get(), tables, "FROM");
		}
	}

	// Handle CTEs
	for (auto &cte : node->cte_map.map) {
		if (cte.second && cte.second->query) {
			ExtractTablesFromQueryNode(cte.second->query->node.get(), tables);
		}
	}
}

static void ExtractTablesFromTableRef(duckdb::TableRef *tref, vector<ExtractedTable> &tables, const string &ctx) {
	if (!tref) return;

	switch (tref->type) {
	case TableReferenceType::BASE_TABLE: {
		auto &base = tref->Cast<BaseTableRef>();
		ExtractedTable t;
		t.schema = base.schema_name;
		t.table = base.table_name;
		t.context = ctx;
		tables.push_back(t);
		break;
	}
	case TableReferenceType::JOIN: {
		auto &join = tref->Cast<JoinRef>();
		ExtractTablesFromTableRef(join.left.get(), tables, ctx);
		ExtractTablesFromTableRef(join.right.get(), tables, "JOIN");
		break;
	}
	case TableReferenceType::SUBQUERY: {
		auto &subq = tref->Cast<SubqueryRef>();
		if (subq.subquery && subq.subquery->node) {
			ExtractTablesFromQueryNode(subq.subquery->node.get(), tables);
		}
		break;
	}
	case TableReferenceType::TABLE_FUNCTION: {
		auto &func = tref->Cast<TableFunctionRef>();
		if (func.function) {
			auto &fn = func.function->Cast<FunctionExpression>();
			ExtractedTable t;
			t.schema = fn.schema;
			t.table = fn.function_name;
			t.context = "TABLE_FUNCTION";
			tables.push_back(t);
		}
		break;
	}
	default:
		break;
	}
}

static void ExtractTablesFromStatement(SQLStatement *stmt, vector<ExtractedTable> &tables) {
	if (!stmt) return;

	if (stmt->type == StatementType::SELECT_STATEMENT) {
		auto &select = stmt->Cast<SelectStatement>();
		if (select.node) {
			ExtractTablesFromQueryNode(select.node.get(), tables);
		}
	}
}

// ============================================================================
// parse_tables(query) - Extract table references
// ============================================================================

struct ParseTablesBindData : public TableFunctionData {
	vector<ExtractedTable> tables;
	string error;
};

struct ParseTablesState : public GlobalTableFunctionState {
	idx_t current_idx = 0;
};

static unique_ptr<FunctionData> ParseTablesBind(ClientContext &context, TableFunctionBindInput &input,
                                                 vector<LogicalType> &return_types, vector<string> &names) {
	auto result = make_uniq<ParseTablesBindData>();
	string query = input.inputs[0].GetValue<string>();

	try {
		Parser parser;
		parser.ParseQuery(query);
		for (auto &stmt : parser.statements) {
			ExtractTablesFromStatement(stmt.get(), result->tables);
		}
	} catch (const Exception &e) {
		result->error = e.what();
	}

	return_types.push_back(LogicalType::VARCHAR);
	names.push_back("schema_name");
	return_types.push_back(LogicalType::VARCHAR);
	names.push_back("table_name");
	return_types.push_back(LogicalType::VARCHAR);
	names.push_back("context");

	return std::move(result);
}

static unique_ptr<GlobalTableFunctionState> ParseTablesInit(ClientContext &context, TableFunctionInitInput &input) {
	return make_uniq<ParseTablesState>();
}

static void ParseTablesFunc(ClientContext &context, TableFunctionInput &data_p, DataChunk &output) {
	auto &bind_data = data_p.bind_data->Cast<ParseTablesBindData>();
	auto &state = data_p.global_state->Cast<ParseTablesState>();

	idx_t count = 0;
	while (state.current_idx < bind_data.tables.size() && count < STANDARD_VECTOR_SIZE) {
		auto &t = bind_data.tables[state.current_idx];
		output.data[0].SetValue(count, t.schema.empty() ? Value() : Value(t.schema));
		output.data[1].SetValue(count, Value(t.table));
		output.data[2].SetValue(count, Value(t.context));
		count++;
		state.current_idx++;
	}
	output.SetCardinality(count);
}

// ============================================================================
// parse_table_names(query) - Returns table names as array
// ============================================================================

static void ParseTableNamesFunc(DataChunk &args, ExpressionState &state, Vector &result) {
	UnaryExecutor::Execute<string_t, list_entry_t>(args.data[0], result, args.size(), [&](string_t query) {
		vector<ExtractedTable> tables;
		try {
			Parser parser;
			parser.ParseQuery(query.GetString());
			for (auto &stmt : parser.statements) {
				ExtractTablesFromStatement(stmt.get(), tables);
			}
		} catch (...) {
		}

		auto &child = ListVector::GetEntry(result);
		auto current_size = ListVector::GetListSize(result);

		for (auto &t : tables) {
			child.SetValue(current_size++, Value(t.table));
		}
		ListVector::SetListSize(result, current_size);

		return list_entry_t{current_size - tables.size(), tables.size()};
	});
}

// ============================================================================
// Helper: Extract function calls from expressions
// ============================================================================

struct FunctionRef {
	string name;
	string type; // "scalar" or "aggregate"
};

static void ExtractFunctionsFromExpression(ParsedExpression *expr, vector<FunctionRef> &functions) {
	if (!expr) return;

	if (expr->type == ExpressionType::FUNCTION) {
		auto &fn = expr->Cast<FunctionExpression>();
		FunctionRef f;
		f.name = fn.function_name;
		f.type = fn.is_operator ? "operator" : (fn.distinct ? "aggregate" : "scalar");
		functions.push_back(f);
		// Recurse into function arguments
		for (auto &child : fn.children) {
			ExtractFunctionsFromExpression(child.get(), functions);
		}
	}
}

static void ExtractFunctionsFromQueryNode(QueryNode *node, vector<FunctionRef> &functions) {
	if (!node) return;

	if (node->type == QueryNodeType::SELECT_NODE) {
		auto &select = node->Cast<SelectNode>();
		for (auto &expr : select.select_list) {
			ExtractFunctionsFromExpression(expr.get(), functions);
		}
		if (select.where_clause) {
			ExtractFunctionsFromExpression(select.where_clause.get(), functions);
		}
		for (auto &expr : select.groups.group_expressions) {
			ExtractFunctionsFromExpression(expr.get(), functions);
		}
	}
}

static void ExtractFunctionsFromStatement(SQLStatement *stmt, vector<FunctionRef> &functions) {
	if (!stmt) return;

	if (stmt->type == StatementType::SELECT_STATEMENT) {
		auto &select = stmt->Cast<SelectStatement>();
		if (select.node) {
			ExtractFunctionsFromQueryNode(select.node.get(), functions);
		}
	}
}

// ============================================================================
// parse_functions(query) - Extract function calls
// ============================================================================

struct ParseFunctionsBindData : public TableFunctionData {
	vector<FunctionRef> functions;
	string error;
};

struct ParseFunctionsState : public GlobalTableFunctionState {
	idx_t current_idx = 0;
};

static unique_ptr<FunctionData> ParseFunctionsBind(ClientContext &context, TableFunctionBindInput &input,
                                                    vector<LogicalType> &return_types, vector<string> &names) {
	auto result = make_uniq<ParseFunctionsBindData>();
	string query = input.inputs[0].GetValue<string>();

	try {
		Parser parser;
		parser.ParseQuery(query);
		for (auto &stmt : parser.statements) {
			ExtractFunctionsFromStatement(stmt.get(), result->functions);
		}
	} catch (const Exception &e) {
		result->error = e.what();
	}

	return_types.push_back(LogicalType::VARCHAR);
	names.push_back("function_name");
	return_types.push_back(LogicalType::VARCHAR);
	names.push_back("function_type");

	return std::move(result);
}

static unique_ptr<GlobalTableFunctionState> ParseFunctionsInit(ClientContext &context, TableFunctionInitInput &input) {
	return make_uniq<ParseFunctionsState>();
}

static void ParseFunctionsFunc(ClientContext &context, TableFunctionInput &data_p, DataChunk &output) {
	auto &bind_data = data_p.bind_data->Cast<ParseFunctionsBindData>();
	auto &state = data_p.global_state->Cast<ParseFunctionsState>();

	idx_t count = 0;
	while (state.current_idx < bind_data.functions.size() && count < STANDARD_VECTOR_SIZE) {
		auto &f = bind_data.functions[state.current_idx];
		output.data[0].SetValue(count, Value(f.name));
		output.data[1].SetValue(count, Value(f.type));
		count++;
		state.current_idx++;
	}
	output.SetCardinality(count);
}

// ============================================================================
// parse_function_names(query) - Returns function names as array
// ============================================================================

static void ParseFunctionNamesFunc(DataChunk &args, ExpressionState &state, Vector &result) {
	UnaryExecutor::Execute<string_t, list_entry_t>(args.data[0], result, args.size(), [&](string_t query) {
		vector<FunctionRef> functions;
		try {
			Parser parser;
			parser.ParseQuery(query.GetString());
			for (auto &stmt : parser.statements) {
				ExtractFunctionsFromStatement(stmt.get(), functions);
			}
		} catch (...) {
		}

		auto &child = ListVector::GetEntry(result);
		auto current_size = ListVector::GetListSize(result);

		for (auto &f : functions) {
			child.SetValue(current_size++, Value(f.name));
		}
		ListVector::SetListSize(result, current_size);

		return list_entry_t{current_size - functions.size(), functions.size()};
	});
}

// ============================================================================
// Helper: Extract WHERE conditions
// ============================================================================

struct WhereCondition {
	string column_name;
	string op;
	string value;
};

static string ComparisonTypeToOperator(ExpressionType type) {
	switch (type) {
	case ExpressionType::COMPARE_EQUAL: return "=";
	case ExpressionType::COMPARE_NOTEQUAL: return "!=";
	case ExpressionType::COMPARE_LESSTHAN: return "<";
	case ExpressionType::COMPARE_GREATERTHAN: return ">";
	case ExpressionType::COMPARE_LESSTHANOREQUALTO: return "<=";
	case ExpressionType::COMPARE_GREATERTHANOREQUALTO: return ">=";
	case ExpressionType::COMPARE_IN: return "IN";
	case ExpressionType::COMPARE_NOT_IN: return "NOT IN";
	default: return "?";
	}
}

static void ExtractWhereConditions(ParsedExpression *expr, vector<WhereCondition> &conditions) {
	if (!expr) return;

	if (expr->type == ExpressionType::CONJUNCTION_AND || expr->type == ExpressionType::CONJUNCTION_OR) {
		auto &conj = expr->Cast<ConjunctionExpression>();
		for (auto &child : conj.children) {
			ExtractWhereConditions(child.get(), conditions);
		}
		return;
	}

	if (expr->GetExpressionClass() == ExpressionClass::COMPARISON) {
		auto &cmp = expr->Cast<ComparisonExpression>();
		WhereCondition cond;
		cond.op = ComparisonTypeToOperator(cmp.type);

		// Try to get column name from left side
		if (cmp.left && cmp.left->type == ExpressionType::COLUMN_REF) {
			auto &col = cmp.left->Cast<ColumnRefExpression>();
			cond.column_name = col.GetColumnName();
		} else if (cmp.left) {
			cond.column_name = cmp.left->ToString();
		}

		// Try to get value from right side
		if (cmp.right && cmp.right->type == ExpressionType::VALUE_CONSTANT) {
			auto &val = cmp.right->Cast<ConstantExpression>();
			cond.value = val.value.ToString();
		} else if (cmp.right) {
			cond.value = cmp.right->ToString();
		}

		conditions.push_back(cond);
	}
}

// ============================================================================
// parse_where(query) - Extract WHERE clause conditions
// ============================================================================

struct ParseWhereBindData : public TableFunctionData {
	vector<WhereCondition> conditions;
	string error;
};

struct ParseWhereState : public GlobalTableFunctionState {
	idx_t current_idx = 0;
};

static unique_ptr<FunctionData> ParseWhereBind(ClientContext &context, TableFunctionBindInput &input,
                                                vector<LogicalType> &return_types, vector<string> &names) {
	auto result = make_uniq<ParseWhereBindData>();
	string query = input.inputs[0].GetValue<string>();

	try {
		Parser parser;
		parser.ParseQuery(query);
		for (auto &stmt : parser.statements) {
			if (stmt->type == StatementType::SELECT_STATEMENT) {
				auto &select = stmt->Cast<SelectStatement>();
				if (select.node && select.node->type == QueryNodeType::SELECT_NODE) {
					auto &snode = select.node->Cast<SelectNode>();
					if (snode.where_clause) {
						ExtractWhereConditions(snode.where_clause.get(), result->conditions);
					}
				}
			}
		}
	} catch (const Exception &e) {
		result->error = e.what();
	}

	return_types.push_back(LogicalType::VARCHAR);
	names.push_back("column_name");
	return_types.push_back(LogicalType::VARCHAR);
	names.push_back("operator");
	return_types.push_back(LogicalType::VARCHAR);
	names.push_back("value");

	return std::move(result);
}

static unique_ptr<GlobalTableFunctionState> ParseWhereInit(ClientContext &context, TableFunctionInitInput &input) {
	return make_uniq<ParseWhereState>();
}

static void ParseWhereFunc(ClientContext &context, TableFunctionInput &data_p, DataChunk &output) {
	auto &bind_data = data_p.bind_data->Cast<ParseWhereBindData>();
	auto &state = data_p.global_state->Cast<ParseWhereState>();

	idx_t count = 0;
	while (state.current_idx < bind_data.conditions.size() && count < STANDARD_VECTOR_SIZE) {
		auto &c = bind_data.conditions[state.current_idx];
		output.data[0].SetValue(count, Value(c.column_name));
		output.data[1].SetValue(count, Value(c.op));
		output.data[2].SetValue(count, Value(c.value));
		count++;
		state.current_idx++;
	}
	output.SetCardinality(count);
}

// ============================================================================
// sql_strip_comments(query) - Remove comments from SQL
// ============================================================================

static void SqlStripCommentsFunc(DataChunk &args, ExpressionState &state, Vector &result) {
	UnaryExecutor::Execute<string_t, string_t>(args.data[0], result, args.size(), [&](string_t query) {
		string input = query.GetString();
		string output;
		output.reserve(input.size());

		bool in_line_comment = false;
		bool in_block_comment = false;
		bool in_string = false;
		char string_char = 0;

		for (size_t i = 0; i < input.size(); i++) {
			char c = input[i];
			char next = (i + 1 < input.size()) ? input[i + 1] : 0;

			if (in_line_comment) {
				if (c == '\n') {
					in_line_comment = false;
					output += c;
				}
				continue;
			}

			if (in_block_comment) {
				if (c == '*' && next == '/') {
					in_block_comment = false;
					i++; // skip the /
				}
				continue;
			}

			if (in_string) {
				output += c;
				if (c == string_char && (i + 1 >= input.size() || input[i + 1] != string_char)) {
					in_string = false;
				} else if (c == string_char) {
					output += input[++i]; // escaped quote
				}
				continue;
			}

			// Check for comment start
			if (c == '-' && next == '-') {
				in_line_comment = true;
				i++; // skip second -
				continue;
			}

			if (c == '/' && next == '*') {
				in_block_comment = true;
				i++; // skip *
				continue;
			}

			// Check for string start
			if (c == '\'' || c == '"') {
				in_string = true;
				string_char = c;
			}

			output += c;
		}

		return StringVector::AddString(result, output);
	});
}

// ============================================================================
// parse_columns(query, stmt_index) - Get SELECT column names from AST
// ============================================================================

struct ParseColumnsBindData : public TableFunctionData {
	vector<string> col_names;
	string error;
};

struct ParseColumnsState : public GlobalTableFunctionState {
	idx_t current_idx = 0;
};

static void ExtractColumnNames(ParsedExpression *expr, idx_t index, vector<string> &names) {
	if (!expr) return;

	// Check for alias first
	if (!expr->alias.empty()) {
		names.push_back(expr->alias);
		return;
	}

	// For column references, use the column name
	if (expr->type == ExpressionType::COLUMN_REF) {
		auto &col = expr->Cast<ColumnRefExpression>();
		names.push_back(col.GetColumnName());
		return;
	}

	// For functions, use function name or generate col_N
	if (expr->type == ExpressionType::FUNCTION) {
		auto &fn = expr->Cast<FunctionExpression>();
		names.push_back(fn.function_name);
		return;
	}

	// Default to col_N
	names.push_back("col" + std::to_string(index));
}

static unique_ptr<FunctionData> ParseColumnsBind(ClientContext &context, TableFunctionBindInput &input,
                                                  vector<LogicalType> &return_types, vector<string> &names) {
	auto result = make_uniq<ParseColumnsBindData>();
	string query = input.inputs[0].GetValue<string>();
	idx_t stmt_index = input.inputs[1].GetValue<int64_t>();

	try {
		Parser parser;
		parser.ParseQuery(query);

		if (stmt_index < parser.statements.size()) {
			auto &stmt = parser.statements[stmt_index];

			if (stmt->type == StatementType::SELECT_STATEMENT) {
				auto &select = stmt->Cast<SelectStatement>();
				if (select.node && select.node->type == QueryNodeType::SELECT_NODE) {
					auto &snode = select.node->Cast<SelectNode>();
					for (idx_t i = 0; i < snode.select_list.size(); i++) {
						ExtractColumnNames(snode.select_list[i].get(), i, result->col_names);
					}
				}
			}
		}
	} catch (const Exception &e) {
		result->error = e.what();
	}

	return_types.push_back(LogicalType::BIGINT);
	names.push_back("col_index");
	return_types.push_back(LogicalType::VARCHAR);
	names.push_back("col_name");

	return std::move(result);
}

static unique_ptr<GlobalTableFunctionState> ParseColumnsInit(ClientContext &context, TableFunctionInitInput &input) {
	return make_uniq<ParseColumnsState>();
}

static void ParseColumnsFunc(ClientContext &context, TableFunctionInput &data_p, DataChunk &output) {
	auto &bind_data = data_p.bind_data->Cast<ParseColumnsBindData>();
	auto &state = data_p.global_state->Cast<ParseColumnsState>();

	idx_t count = 0;
	while (state.current_idx < bind_data.col_names.size() && count < STANDARD_VECTOR_SIZE) {
		output.data[0].SetValue(count, Value::BIGINT(state.current_idx));
		output.data[1].SetValue(count, Value(bind_data.col_names[state.current_idx]));
		count++;
		state.current_idx++;
	}
	output.SetCardinality(count);
}

// ============================================================================
// sql_parse_json(query) - Get parse tree as JSON
// ============================================================================

static void SqlParseJsonFunc(DataChunk &args, ExpressionState &state, Vector &result) {
	UnaryExecutor::Execute<string_t, string_t>(args.data[0], result, args.size(), [&](string_t query) {
		try {
			Parser parser;
			parser.ParseQuery(query.GetString());

			std::ostringstream json;
			json << "{\"error\":false,\"statements\":[";

			bool first_stmt = true;
			for (auto &stmt : parser.statements) {
				if (!first_stmt) json << ",";
				first_stmt = false;

				json << "{\"type\":\"" << StatementTypeToString(stmt->type) << "\"";
				json << ",\"query\":\"" << stmt->ToString() << "\"";
				json << "}";
			}

			json << "]}";
			return StringVector::AddString(result, json.str());
		} catch (const Exception &e) {
			std::ostringstream json;
			json << "{\"error\":true,\"message\":\"" << e.what() << "\"}";
			return StringVector::AddString(result, json.str());
		}
	});
}

// ============================================================================
// Registration
// ============================================================================

void RegisterParserFunctions(ExtensionLoader &loader) {
	// Table functions
	TableFunction tokenize_sql("tokenize_sql", {LogicalType::VARCHAR}, TokenizeSqlFunc, TokenizeSqlBind, TokenizeSqlInit);
	loader.RegisterFunction(tokenize_sql);

	TableFunction sql_keywords("sql_keywords", {}, SqlKeywordsFunc, SqlKeywordsBind, SqlKeywordsInit);
	loader.RegisterFunction(sql_keywords);

	TableFunction parse_statements("parse_statements", {LogicalType::VARCHAR}, ParseStatementsFunc, ParseStatementsBind, ParseStatementsInit);
	loader.RegisterFunction(parse_statements);

	TableFunction parse_tables("parse_tables", {LogicalType::VARCHAR}, ParseTablesFunc, ParseTablesBind, ParseTablesInit);
	loader.RegisterFunction(parse_tables);

	TableFunction parse_functions("parse_functions", {LogicalType::VARCHAR}, ParseFunctionsFunc, ParseFunctionsBind, ParseFunctionsInit);
	loader.RegisterFunction(parse_functions);

	TableFunction parse_where("parse_where", {LogicalType::VARCHAR}, ParseWhereFunc, ParseWhereBind, ParseWhereInit);
	loader.RegisterFunction(parse_where);

	TableFunction parse_columns("parse_columns", {LogicalType::VARCHAR, LogicalType::BIGINT}, ParseColumnsFunc, ParseColumnsBind, ParseColumnsInit);
	loader.RegisterFunction(parse_columns);

	// Scalar functions
	ScalarFunction is_valid_sql("is_valid_sql", {LogicalType::VARCHAR}, LogicalType::BOOLEAN, IsValidSqlFunc);
	loader.RegisterFunction(is_valid_sql);

	ScalarFunction sql_error_message("sql_error_message", {LogicalType::VARCHAR}, LogicalType::VARCHAR, SqlErrorMessageFunc);
	sql_error_message.null_handling = FunctionNullHandling::SPECIAL_HANDLING;
	loader.RegisterFunction(sql_error_message);

	ScalarFunction num_statements("num_statements", {LogicalType::VARCHAR}, LogicalType::BIGINT, NumStatementsFunc);
	loader.RegisterFunction(num_statements);

	ScalarFunction is_keyword("is_keyword", {LogicalType::VARCHAR}, LogicalType::BOOLEAN, IsKeywordFunc);
	loader.RegisterFunction(is_keyword);

	ScalarFunction sql_strip_comments("sql_strip_comments", {LogicalType::VARCHAR}, LogicalType::VARCHAR, SqlStripCommentsFunc);
	loader.RegisterFunction(sql_strip_comments);

	ScalarFunction sql_parse_json("sql_parse_json", {LogicalType::VARCHAR}, LogicalType::VARCHAR, SqlParseJsonFunc);
	loader.RegisterFunction(sql_parse_json);

	ScalarFunction parse_table_names("parse_table_names", {LogicalType::VARCHAR}, LogicalType::LIST(LogicalType::VARCHAR), ParseTableNamesFunc);
	loader.RegisterFunction(parse_table_names);

	ScalarFunction parse_function_names("parse_function_names", {LogicalType::VARCHAR}, LogicalType::LIST(LogicalType::VARCHAR), ParseFunctionNamesFunc);
	loader.RegisterFunction(parse_function_names);
}

} // namespace duckdb
