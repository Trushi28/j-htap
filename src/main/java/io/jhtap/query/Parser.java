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

    public record InsertQuery(
            String table,
            String key,
            List<Long> values
    ) {}

    public Object parse() {
        if (peek().type() == Lexer.TokenType.SELECT) {
            return parseSelect();
        } else if (peek().type() == Lexer.TokenType.INSERT) {
            return parseInsert();
        }
        throw new RuntimeException("Unexpected token: " + peek().type());
    }

    private SelectQuery parseSelect() {
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

    private InsertQuery parseInsert() {
        expect(Lexer.TokenType.INSERT);
        expect(Lexer.TokenType.INTO);
        String table = expect(Lexer.TokenType.IDENTIFIER).value();
        expect(Lexer.TokenType.VALUES);
        expect(Lexer.TokenType.LPAREN);
        
        String key = expect(Lexer.TokenType.STRING).value();
        java.util.List<Long> values = new java.util.ArrayList<>();
        
        while (peek().type() == Lexer.TokenType.COMMA) {
            consume();
            values.add(Long.parseLong(expect(Lexer.TokenType.NUMBER).value()));
        }
        
        expect(Lexer.TokenType.RPAREN);
        return new InsertQuery(table, key, values);
    }

    private Lexer.Token peek() { return tokens.get(pos); }
    private Lexer.Token consume() { return tokens.get(pos++); }
    private Lexer.Token expect(Lexer.TokenType type) {
        Lexer.Token t = consume();
        if (t.type() != type) throw new RuntimeException("Expected " + type + " but got " + t.type());
        return t;
    }
}
