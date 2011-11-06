package com.alexholmes.json.parser;

/**
 * A very loosey-goosey lexer that doesn't enforce any JSON structural rules
 */
public class JsonLexer {
    private JsonLexerState state;

    public JsonLexer() {
        this(JsonLexerState.NULL);
    }

    public JsonLexer(JsonLexerState initState) {
        state = initState;
    }

    public JsonLexerState getState() {
        return state;
    }

    public void setState(JsonLexerState state) {
        this.state = state;
    }

    public static enum JsonLexerState {
        NULL,
        DONT_CARE,
        BEGIN_OBJECT,
        END_OBJECT,
        BEGIN_STRING,
        END_STRING,
        INSIDE_STRING,
        STRING_ESCAPE,
        VALUE_SEPARATOR,
        NAME_SEPARATOR,
        BEGIN_ARRAY,
        END_ARRAY,
        WHITESPACE
    }

    public void lex(char c) {
        switch (state) {
            case NULL:
            case BEGIN_OBJECT:
            case END_OBJECT:
            case BEGIN_ARRAY:
            case END_ARRAY:
            case END_STRING:
            case VALUE_SEPARATOR:
            case NAME_SEPARATOR:
            case DONT_CARE:
            case WHITESPACE: {
                if (Character.isWhitespace(c)) {
                    state = JsonLexerState.WHITESPACE;
                    break;
                }
                switch (c) {
                    // value-separator (comma)
                    case ',':
                        state = JsonLexerState.VALUE_SEPARATOR;
                        break;
                    // name-separator (colon)
                    case ':':
                        state = JsonLexerState.NAME_SEPARATOR;
                        break;
                    // string
                    //
                    case '"':
                        state = JsonLexerState.BEGIN_STRING;
                        break;
                    // start-object
                    //
                    case '{':
                        state = JsonLexerState.BEGIN_OBJECT;
                        break;
                    // end-object
                    //
                    case '}':
                        state = JsonLexerState.END_OBJECT;
                        break;
                    // begin-array
                    //
                    case '[':
                        state = JsonLexerState.BEGIN_ARRAY;
                        break;
                    // end-array
                    //
                    case ']':
                        state = JsonLexerState.END_ARRAY;
                        break;
                    default:
                        state = JsonLexerState.DONT_CARE;
                }
                break;
            }
            case BEGIN_STRING: {
                state = JsonLexerState.INSIDE_STRING;
                // we will now enter the STRING state below
            }
            case INSIDE_STRING: {
                switch (c) {
                    // end-string
                    //
                    case '"':
                        state = JsonLexerState.END_STRING;
                        break;
                    // escape
                    //
                    case '\\':
                        state = JsonLexerState.STRING_ESCAPE;
                }
                break;
            }
            case STRING_ESCAPE: {
                state = JsonLexerState.INSIDE_STRING;
                break;
            }
        }
    }

}
