package com.lightspeed;

import com.lightspeed.model.Join;
import com.lightspeed.model.Query;
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

        return new Query(columns, sources, joins, whereClauses, groupByColumns);
    }

    private List<String> parseGroupByColumns(String sql) {
        Pattern pattern = Pattern.compile("GROUP BY+\\s(.+?)(?:ORDER|LIMIT|OFFSET|$)");
        Matcher matcher = pattern.matcher(sql);

        if (matcher.find()) {
            return Arrays.stream(matcher.group(1).split(","))
                    .toList();
        }

        return List.of();
    }

    private List<WhereClause> parseWhere(String sql) {
        Pattern pattern = Pattern.compile(
                "(?i)(?:^|\\s)(WHERE|AND|OR)\\s+(.+?)(?=\\s+(?:AND|OR|GROUP BY|$))",
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