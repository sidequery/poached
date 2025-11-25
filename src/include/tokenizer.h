#pragma once

#include <stdint.h>

#ifdef __cplusplus
extern "C" {
#endif

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

TokenizeResult tokenize_sql_impl(const char *query);
void free_tokenize_result(TokenizeResult *result);
const char *token_type_name(TokenType type);

#ifdef __cplusplus
}
#endif
