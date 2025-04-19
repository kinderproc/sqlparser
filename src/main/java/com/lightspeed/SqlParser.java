package com.lightspeed;

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

        return new Query(columns, sources);
    }

    private List<Source> parseFrom(String sql) throws IllegalArgumentException {
        Pattern pattern = Pattern.compile("FROM(.+?)(?:\\s+WHERE\\b|$)", Pattern.CASE_INSENSITIVE | Pattern.DOTALL);
        Matcher matcher = pattern.matcher(sql);

        if (matcher.find()) {
            String fromPart = matcher.group(1).trim();
            return Arrays.stream(fromPart.split(","))
                    .map(str -> {
                        String[] parts = str.split("\\s");
                        String tableName = parts[0];
                        String alias = parts.length > 1 ? parts[1] : null;
                        return new Source(tableName, alias);
                    })
                    .toList();
        }

        return List.of();
    }

    private List<String> parseSelect(String sql) throws IllegalArgumentException {
        Pattern pattern = Pattern.compile("SELECT(.+?)FROM", Pattern.CASE_INSENSITIVE | Pattern.DOTALL);
        Matcher matcher = pattern.matcher(sql);

        if (matcher.find()) {
            String selectPart = matcher.group(1).trim();
            return Arrays.stream(selectPart.split(",")).toList();
        }

        throw new IllegalArgumentException("Can't parse columns from SELECT.");
    }
}