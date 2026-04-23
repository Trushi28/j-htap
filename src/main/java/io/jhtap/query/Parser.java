package io.jhtap.query;

import java.util.List;

public class Parser {
    private final List<Lexer.Token> tokens;
    private int pos = 0;

    public Parser(List<Lexer.Token> tokens) {
        this.tokens = tokens;
    }

    public record SelectQuery(
            boolean selectAll,
            Integer sumColIdx,
            String table,
            String whereKey,
            Long asOfTimestamp
    ) {}

    public SelectQuery parse() {
        expect(Lexer.TokenType.SELECT);
        boolean selectAll = false;
        Integer sumColIdx = null;

        if (peek().type() == Lexer.TokenType.ASTERISK) {
            consume();
            selectAll = true;
        } else if (peek().type() == Lexer.TokenType.SUM) {
            consume();
            expect(Lexer.TokenType.LPAREN);
            String colName = expect(Lexer.TokenType.IDENTIFIER).value();
            // col0 -> 0
            sumColIdx = Integer.parseInt(colName.replace("col", ""));
            expect(Lexer.TokenType.RPAREN);
        }

        expect(Lexer.TokenType.FROM);
        String table = expect(Lexer.TokenType.IDENTIFIER).value();

        String whereKey = null;
        if (peek().type() == Lexer.TokenType.WHERE) {
            consume();
            expect(Lexer.TokenType.KEY);
            expect(Lexer.TokenType.EQUALS);
            whereKey = expect(Lexer.TokenType.STRING).value();
        }

        Long asOf = null;
        if (peek().type() == Lexer.TokenType.AS) {
            consume();
            expect(Lexer.TokenType.OF);
            asOf = Long.parseLong(expect(Lexer.TokenType.NUMBER).value());
        }

        return new SelectQuery(selectAll, sumColIdx, table, whereKey, asOf);
    }

    private Lexer.Token peek() { return tokens.get(pos); }
    private Lexer.Token consume() { return tokens.get(pos++); }
    private Lexer.Token expect(Lexer.TokenType type) {
        Lexer.Token t = consume();
        if (t.type() != type) throw new RuntimeException("Expected " + type + " but got " + t.type());
        return t;
    }
}
