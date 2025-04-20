package com.lightspeed;

import com.lightspeed.model.Join;
import com.lightspeed.model.Query;
import com.lightspeed.model.Source;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

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
    // TODO: add parameterized test
    void givenSelectFromJoinSql_whenParseCalled_thenColumnAndTableAndJoinParsed() {
        // given
        String sql =
                """
                SELECT a.name
                FROM author a
                LEFT JOIN book b ON (a.id = b.author_id)
                """;

        // when
        Query actual = parser.parse(sql);

        // then
        assertNotNull(actual.columns());
        assertTrue(actual.columns().contains("a.name"));
        assertNotNull(actual.tables());
        assertTrue(actual.tables().contains(new Source("author", "a")));
        assertNotNull(actual.joins());
        Join expected = new Join("LEFT", "book", "b", "(a.id = b.author_id)");
        assertEquals(expected, actual.joins().getFirst());
    }
}