/*
 * This file is part of LuckPerms, licensed under the MIT License.
 *
 *  Copyright (c) lucko (Luck) <luck@lucko.me>
 *  Copyright (c) contributors
 *
 *  Permission is hereby granted, free of charge, to any person obtaining a copy
 *  of this software and associated documentation files (the "Software"), to deal
 *  in the Software without restriction, including without limitation the rights
 *  to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 *  copies of the Software, and to permit persons to whom the Software is
 *  furnished to do so, subject to the following conditions:
 *
 *  The above copyright notice and this permission notice shall be included in all
 *  copies or substantial portions of the Software.
 *
 *  THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 *  IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 *  FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 *  AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 *  LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 *  OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
 *  SOFTWARE.
 */

package me.lucko.luckperms.common.bulkupdate.comparisons;

import com.rethinkdb.RethinkDB;
import com.rethinkdb.gen.ast.ReqlExpr;
import com.rethinkdb.gen.ast.ReqlFunction1;
import me.lucko.luckperms.common.bulkupdate.PreparedStatementBuilder;

/**
 * An enumeration of standard {@link Comparison}s.
 */
public enum StandardComparison implements Comparison {
    EQUAL("==", "=") {
        @Override
        public boolean matches(String str, String expr) {
            return str.equalsIgnoreCase(expr);
        }

        @Override
        public ReqlFunction1 createReqlFilter(String fieldName, String expression) {
            return row -> row.g(fieldName).eq(fieldName, expression);
        }
    },

    NOT_EQUAL("!=", "!=") {
        @Override
        public boolean matches(String str, String expr) {
            return !str.equalsIgnoreCase(expr);
        }

        @Override
        public ReqlFunction1 createReqlFilter(String fieldName, String expression) {
            return row -> RethinkDB.r.not(row.g(fieldName).eq(fieldName, expression));
        }
    },

    SIMILAR("~~", "LIKE") {
        @Override
        public boolean matches(String str, String expr) {
            // form expression
            expr = expr.toLowerCase();
            expr = expr.replace(".", "\\.");

            // convert from SQL LIKE syntax to regex
            expr = expr.replace("_", ".");
            expr = expr.replace("%", ".*");

            return str.toLowerCase().matches(expr);
        }

        @Override
        public ReqlFunction1 createReqlFilter(String fieldName, String expression) {
            return row -> row.g(fieldName).match(expression);
        }
    },

    NOT_SIMILAR("!~", "NOT LIKE") {
        @Override
        public boolean matches(String str, String expr) {
            // form expression
            expr = expr.toLowerCase();
            expr = expr.replace(".", "\\.");

            // convert from SQL LIKE syntax to regex
            expr = expr.replace("_", ".");
            expr = expr.replace("%", ".*");

            return !str.toLowerCase().matches(expr);
        }

        @Override
        public ReqlFunction1 createReqlFilter(String fieldName, String expression) {
            return row -> RethinkDB.r.not(row.g(fieldName).match(expression));
        }
    };

    private final String symbol;
    private final String asSql;

    StandardComparison(String symbol, String asSql) {
        this.symbol = symbol;
        this.asSql = asSql;
    }

    @Override
    public String getSymbol() {
        return this.symbol;
    }

    @Override
    public void appendSql(PreparedStatementBuilder builder) {
        builder.append(this.asSql);
    }

    @Override
    public String toString() {
        return this.symbol;
    }

    public static StandardComparison parseComparison(String s) {
        for (StandardComparison t : values()) {
            if (t.getSymbol().equals(s)) {
                return t;
            }
        }
        return null;
    }

}
