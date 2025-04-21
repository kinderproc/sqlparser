package com.lightspeed;

import com.lightspeed.model.HavingClause;
import com.lightspeed.model.Join;
import com.lightspeed.model.Query;
import com.lightspeed.model.Sort;
import com.lightspeed.model.Source;
import com.lightspeed.model.WhereClause;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class SqlParser {

    public Query parse(String sql) {
        List<String> parsingErrors = new ArrayList<>();
        List<String> columns = new ArrayList<>();
        List<Source> sources = new ArrayList<>();
        List<Join> joins = new ArrayList<>();
        List<WhereClause> whereClauses = new ArrayList<>();
        List<String> groupByColumns = new ArrayList<>();
        List<HavingClause> havingClauses = new ArrayList<>();
        List<Sort> orderByColumns = new ArrayList<>();
        Integer limit = 0;
        Integer offset = 0;

        try {
            columns = parseSelect(sql);
        } catch (IllegalArgumentException e) {
            parsingErrors.add(e.getMessage());
        }

        try {
            sources = parseFrom(sql);
        } catch (IllegalArgumentException e) {
            parsingErrors.add(e.getMessage());
        }

        try {
            joins = parseJoins(sql);
        } catch (IllegalArgumentException e) {
            parsingErrors.add(e.getMessage());
        }

        try {
            whereClauses = parseWhere(sql);
        } catch (IllegalArgumentException e) {
            parsingErrors.add(e.getMessage());
        }

        try {
            groupByColumns = parseGroupByColumns(sql);
        } catch (IllegalArgumentException e) {
            parsingErrors.add(e.getMessage());
        }

        try {
            havingClauses = parseHaving(sql);
        } catch (IllegalArgumentException e) {
            parsingErrors.add(e.getMessage());
        }

        try {
            orderByColumns = parseOrderBy(sql);
        } catch (IllegalArgumentException e) {
            parsingErrors.add(e.getMessage());
        }

        try {
            limit = parseLimit(sql);
        } catch (IllegalArgumentException e) {
            parsingErrors.add(e.getMessage());
        }

        try {
            offset = parseOffset(sql);
        } catch (IllegalArgumentException e) {
            parsingErrors.add(e.getMessage());
        }

        return new Query(columns, sources, joins, whereClauses, groupByColumns, havingClauses, orderByColumns, limit, offset);
    }

    private Integer parseLimit(String sql) {
        Pattern pattern = Pattern.compile(
                "(?i)LIMIT\\s+(\\d+)",
                Pattern.DOTALL);
        Matcher matcher = pattern.matcher(sql);

        if (matcher.find()) {
            try {
                return Integer.parseInt(matcher.group(1));
            } catch (NumberFormatException e) {
                return 0;
            }
        }

        return 0;
    }

    private Integer parseOffset(String sql) {
        Pattern pattern = Pattern.compile(
                "(?i)OFFSET\\s+(\\d+)",
                Pattern.DOTALL
        );
        Matcher matcher = pattern.matcher(sql);

        if (matcher.find()) {
            try {
                return Integer.parseInt(matcher.group(1));
            } catch (NumberFormatException e) {
                return 0;
            }
        }

        return 0;
    }

    private List<Sort> parseOrderBy(String sql) {
        Pattern pattern = Pattern.compile(
                "(?i)(?:^|\\s)ORDER BY\\s+(.+?)(:?LIMIT|OFFSET|$|\n)",
                Pattern.DOTALL);
        Matcher matcher = pattern.matcher(sql);

        if (matcher.find()) {
          String orderByPart = matcher.group(1);
          return Arrays.stream(orderByPart.split(","))
                  .map(str -> {
                      String[] parts = str.split(" ");
                      String column = parts[0];
                      String direction = parts.length > 1 ? parts[1] : null;

                      return new Sort(column, direction);
                  })
                  .toList();

        }

        return List.of();
    }

    private List<HavingClause> parseHaving(String sql) {
        // TODO: add OR processing for HAVING clauses
        Pattern pattern = Pattern.compile(
                "(?i)HAVING\\s+(.+?)(?=\\s*(?:ORDER BY|LIMIT|OFFSET|$))",
                Pattern.DOTALL
        );
        Matcher matcher = pattern.matcher(sql);

        if (!matcher.find()) {
            return List.of();
        }

        String havingBlock = matcher.group(1).trim();
        List<HavingClause> result = new ArrayList<>();

        String[] parts = havingBlock.split("((?i)\\s+(AND|OR)\\s+)");

        if (parts.length > 0) {
            result.add(new HavingClause("HAVING", parts[0].trim()));
        }

        for (int i = 1; i < parts.length; i ++) {
            result.add(new HavingClause("AND", parts[i].trim()));
        }

        return result;
    }

    private List<String> parseGroupByColumns(String sql) {
        Pattern pattern = Pattern.compile(
                "(?i)GROUP BY\\s(.+?)(?=\\s+(?:HAVING|ORDER|LIMIT|OFFSET|$))",
                Pattern.DOTALL);
        Matcher matcher = pattern.matcher(sql);

        if (matcher.find()) {
            return Arrays.stream(matcher.group(1).split(","))
                    .toList();
        }

        return List.of();
    }

    private List<WhereClause> parseWhere(String sql) {
        // TODO: implement parentheses processing in WHERE clauses
        Pattern pattern = Pattern.compile(
                "(?i)(?:^|\\s)(WHERE|AND|OR)\\s+(.+?)(?=\\s+(?:AND|OR|GROUP BY|HAVING|ORDER|LIMIT|OFFSET|$))",
                Pattern.DOTALL);
        Matcher matcher = pattern.matcher(sql);
        List<WhereClause> result = new ArrayList<>();

        while (matcher.find()) {
            String type = matcher.group(1).trim();
            String condition = matcher.group(2).trim();
            result.add(new WhereClause(type, condition));
        }

        return result;
    }

    private List<Join> parseJoins(String sql) {
        Pattern pattern = Pattern.compile(
                "(?i)(INNER|LEFT|RIGHT|FULL)\\s+JOIN(.+?)\\s(.+?)ON(.+?)(:?JOIN\\b|\\sWHERE\\b|$)",
                Pattern.DOTALL);
        Matcher matcher = pattern.matcher(sql);
        List<Join> result = new ArrayList<>();

        while (matcher.find()) {
            String type = matcher.group(1).trim();
            String table = matcher.group(2).trim();
            String alias = matcher.group(3).trim();
            String condition = matcher.group(4).trim();
            result.add(new Join(type, table, alias, condition));
        }

        return result;
    }

    private List<Source> parseFrom(String sql) throws IllegalArgumentException {
        Pattern pattern = Pattern.compile(
                "(?i)FROM(.+?)(?:\\s+INNER JOIN\\b|\\s+LEFT JOIN\\b|\\s+RIGHT JOIN\\b|\\s+FULL JOIN\\b|\\s+WHERE\\b|$)",
                Pattern.DOTALL);
        Matcher matcher = pattern.matcher(sql);

        if (matcher.find()) {
            String fromPart = matcher.group(1).trim();
            return Arrays.stream(fromPart.split(","))
                    .map(str -> {
                        String[] parts = str.split("\\s");
                        String tableName = parts[0];
                        String alias = parts.length > 1 ? parts[1] : null;

                        return new Source(tableName, alias);
                    }).toList();
        }

        return List.of();
    }

    private List<String> parseSelect(String sql) throws IllegalArgumentException {
        Pattern pattern = Pattern.compile("(?i)SELECT(.+?)FROM", Pattern.DOTALL);
        Matcher matcher = pattern.matcher(sql);

        if (matcher.find()) {
            String selectPart = matcher.group(1).trim();
            return Arrays.stream(selectPart.split(","))
                    .map(String::trim)
                    .toList();
        }

        throw new IllegalArgumentException("Can't parse columns from SELECT.");
    }
}