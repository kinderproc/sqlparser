package com.lightspeed;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

class SqlParserTest {

    private SqlParser parser;

    @BeforeEach
    void setUp() {
        parser = new SqlParser();
    }

    @Test
    void givenSelectFromSql_whenParseCalled_thenColumnAndTableParsed() {
        // given
        String sql =
                """
                SELECT author.name
                FROM author
                """;

        // when
        Query actual = parser.parse(sql);

        // then
        assertNotNull(actual.columns());
        assertTrue(actual.columns().contains("author.name"));
        assertNotNull(actual.tables());
        assertTrue(actual.tables().contains(new Source("author", null)));
    }
}