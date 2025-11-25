#include "duckdb/parser/parser.hpp"
#include "duckdb/parser/simplified_token.hpp"

extern "C" {

// Token types matching SimplifiedTokenType
typedef enum {
    TOKEN_IDENTIFIER = 0,
    TOKEN_NUMERIC_CONSTANT = 1,
    TOKEN_STRING_CONSTANT = 2,
    TOKEN_OPERATOR = 3,
    TOKEN_KEYWORD = 4,
    TOKEN_COMMENT = 5,
    TOKEN_ERROR = 6
} TokenType;

typedef struct {
    TokenType type;
    uint64_t start;
} Token;

typedef struct {
    Token *tokens;
    uint64_t count;
} TokenizeResult;

TokenizeResult tokenize_sql_impl(const char *query) {
    TokenizeResult result;
    result.tokens = nullptr;
    result.count = 0;

    if (!query) {
        return result;
    }

    try {
        auto tokens = duckdb::Parser::Tokenize(std::string(query));
        result.count = tokens.size();
        if (result.count > 0) {
            result.tokens = (Token *)malloc(sizeof(Token) * result.count);
            for (size_t i = 0; i < result.count; i++) {
                result.tokens[i].type = static_cast<TokenType>(static_cast<uint8_t>(tokens[i].type));
                result.tokens[i].start = tokens[i].start;
            }
        }
    } catch (...) {
        result.tokens = nullptr;
        result.count = 0;
    }

    return result;
}

void free_tokenize_result(TokenizeResult *result) {
    if (result && result->tokens) {
        free(result->tokens);
        result->tokens = nullptr;
        result->count = 0;
    }
}

const char *token_type_name(TokenType type) {
    switch (type) {
        case TOKEN_IDENTIFIER: return "IDENTIFIER";
        case TOKEN_NUMERIC_CONSTANT: return "NUMERIC_CONSTANT";
        case TOKEN_STRING_CONSTANT: return "STRING_CONSTANT";
        case TOKEN_OPERATOR: return "OPERATOR";
        case TOKEN_KEYWORD: return "KEYWORD";
        case TOKEN_COMMENT: return "COMMENT";
        case TOKEN_ERROR: return "ERROR";
        default: return "UNKNOWN";
    }
}

} // extern "C"
