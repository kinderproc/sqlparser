package com.lightspeed;

import com.lightspeed.model.HavingClause;
import com.lightspeed.model.Join;
import com.lightspeed.model.Query;
import com.lightspeed.model.Sort;
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
        // given
        String sql =
                """
                SELECT a.name, count(book.id), sum(book.cost)
                LEFT JOIN book b ON (a.id = b.author_id)
                WHERE a.name LIKE ('%A%')
                  AND b.cost > 1000
                   OR b.pages > 300
                GROUP BY a.name
                HAVING COUNT(*) > 1
                   AND SUM(book.cost) > 500
                ORDER BY a.name ASC
                LIMIT 100
                OFFSET 50
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
                new WhereClause("OR", "b.pages > 300"),
                new WhereClause("AND", "SUM(book.cost) > 500"));
        assertEquals(expectedWhereClauses, actual.whereClauses());
        List<String> expectedGroupByColumns = List.of("a.name");
        assertEquals(expectedGroupByColumns, actual.groupByColumns());
        List<HavingClause> expectedHavingClause = List.of(
                new HavingClause("HAVING", "COUNT(*) > 1"),
                new HavingClause("AND", "SUM(book.cost) > 500"));
        assertEquals(expectedHavingClause, actual.havingClauses());
        List<Sort> expectedOrderByColumns = List.of(new Sort("a.name", "ASC"));
        assertEquals(expectedOrderByColumns, actual.orderByColumns());
        assertEquals(100, actual.limit());
        assertEquals(50, actual.offset());
    }

    @Test
    void givenSelectWithAsteriskAndImplicitJoin_whenParseCalled_thenColumnsAndTablesAndWhereParsed() {
        // given
        String sql =
                """
                SELECT *
                FROM author a, book b
                WHERE a.id = b.author_id
                  AND a.name LIKE ('%A%')
                  AND b.cost > 1000
                   OR b.pages > 300
                """;

        // when
        Query actual = parser.parse(sql);

        // then
        assertNotNull(actual.columns());
        List<String> expectedColumns = List.of("*");
        assertEquals(expectedColumns, actual.columns());
        List<Source> expectedTables = List.of(
                new Source("author", "a"),
                new Source("book", "b"));
        assertEquals(expectedTables, actual.tables());

        List<WhereClause> expectedWhereClauses = List.of(
                new WhereClause("WHERE", "a.id = b.author_id"),
                new WhereClause("AND", "a.name LIKE ('%A%')"),
                new WhereClause("AND", "b.cost > 1000"),
                new WhereClause("OR", "b.pages > 300"));
        assertEquals(expectedWhereClauses, actual.whereClauses());
    }
}