package io.jhtap.query;

import java.util.ArrayList;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class Lexer {
    public enum TokenType {
        SELECT, FROM, WHERE, AS, OF, SUM, KEY,
        INSERT, INTO, VALUES,
        ASTERISK, LPAREN, RPAREN, EQUALS, COMMA,
        IDENTIFIER, STRING, NUMBER, EOF
    }

    public record Token(TokenType type, String value) {}

    private static final Pattern TOKEN_PATTERN = Pattern.compile(
            "\\s*(?:(SELECT|FROM|WHERE|AS|OF|SUM|KEY|INSERT|INTO|VALUES)|(\\*)|(\\()|(\\))|(=)|(,)|([a-zA-Z_][a-zA-Z0-9_]*)|'([^']*)'|(\\d+))",
            Pattern.CASE_INSENSITIVE
    );

    private final String input;
    private int pos = 0;

    public Lexer(String input) {
        this.input = input;
    }

    public List<Token> tokenize() {
        List<Token> tokens = new ArrayList<>();
        Matcher matcher = TOKEN_PATTERN.matcher(input);
        while (matcher.find(pos)) {
            if (matcher.group(1) != null) tokens.add(new Token(TokenType.valueOf(matcher.group(1).toUpperCase()), matcher.group(1)));
            else if (matcher.group(2) != null) tokens.add(new Token(TokenType.ASTERISK, "*"));
            else if (matcher.group(3) != null) tokens.add(new Token(TokenType.LPAREN, "("));
            else if (matcher.group(4) != null) tokens.add(new Token(TokenType.RPAREN, ")"));
            else if (matcher.group(5) != null) tokens.add(new Token(TokenType.EQUALS, "="));
            else if (matcher.group(6) != null) tokens.add(new Token(TokenType.COMMA, ","));
            else if (matcher.group(7) != null) tokens.add(new Token(TokenType.IDENTIFIER, matcher.group(7)));
            else if (matcher.group(8) != null) tokens.add(new Token(TokenType.STRING, matcher.group(8)));
            else if (matcher.group(9) != null) tokens.add(new Token(TokenType.NUMBER, matcher.group(9)));
            pos = matcher.end();
        }
        tokens.add(new Token(TokenType.EOF, ""));
        return tokens;
    }
}
