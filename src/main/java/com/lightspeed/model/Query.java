package com.lightspeed.model;

import java.util.List;

public record Query(
        List<String> columns,
        List<Source> tables,
        List<Join> joins,
        List<WhereClause> whereClauses,
        List<String> groupByColumns,
        List<HavingClause> havingClauses,
        List<Sort> orderByColumns,
        Integer limit,
        Integer offset) {

}
