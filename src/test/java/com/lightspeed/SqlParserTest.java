package com.lightspeed;

import com.lightspeed.model.Join;
import com.lightspeed.model.Query;
import com.lightspeed.model.Source;
import com.lightspeed.model.WhereClause;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

class SqlParserTest {

    private SqlParser parser;

    @BeforeEach
    void setUp() {
        parser = new SqlParser();
    }

    @Test
    void givenSelectFromJoinSql_whenParseCalled_thenColumnAndTableAndJoinParsed() {
        // TODO: implement parentheses processing in WHERE clauses
        // given
        String sql =
                """
                SELECT a.name
                FROM author a
                LEFT JOIN book b ON (a.id = b.author_id)
                WHERE a.name LIKE ('%A%')
                  AND b.price > 1000
                   OR b.pages > 300
                """;

        // when
        Query actual = parser.parse(sql);

        // then
        assertNotNull(actual.columns());
        assertTrue(actual.columns().contains("a.name"));
        assertNotNull(actual.tables());
        assertTrue(actual.tables().contains(new Source("author", "a")));
        assertNotNull(actual.joins());
        Join expectedJoin = new Join("LEFT", "book", "b", "(a.id = b.author_id)");
        assertEquals(expectedJoin, actual.joins().getFirst());
        List<WhereClause> expectedWhereClauses = List.of(
                new WhereClause("WHERE", "(a.name LIKE ('%A%')"),
                new WhereClause("AND", "b.price > 1000)"),
                new WhereClause("OR", "b.pages > 300"));
        assertEquals(expectedWhereClauses, actual.whereClauses());
    }
}