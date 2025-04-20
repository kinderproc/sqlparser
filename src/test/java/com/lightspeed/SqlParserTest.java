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
                SELECT a.name, count(book.id), sum(book.cost)
                FROM author a
                LEFT JOIN book b ON (a.id = b.author_id)
                WHERE a.name LIKE ('%A%')
                  AND b.cost > 1000
                   OR b.pages > 300
                GROUP BY a.name
                """;

        // when
        Query actual = parser.parse(sql);

        // then
        assertNotNull(actual.columns());
        List<String> expectedColumns = List.of(
                "a.name",
                "count(book.id)",
                "sum(book.cost)");
        assertEquals(expectedColumns, actual.columns());
        assertTrue(actual.tables().contains(new Source("author", "a")));
        Join expectedJoin = new Join("LEFT", "book", "b", "(a.id = b.author_id)");
        assertEquals(expectedJoin, actual.joins().getFirst());
        List<WhereClause> expectedWhereClauses = List.of(
                new WhereClause("WHERE", "a.name LIKE ('%A%')"),
                new WhereClause("AND", "b.cost > 1000"),
                new WhereClause("OR", "b.pages > 300"));
        assertEquals(expectedWhereClauses, actual.whereClauses());
        List<String> expectedGroupByColumns = List.of("a.name");
        assertEquals(expectedGroupByColumns, actual.groupByColumns());
    }
}