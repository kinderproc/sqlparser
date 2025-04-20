package com.lightspeed.model;

import java.util.List;

public record Query(List<String> columns, List<Source> tables, List<Join> joins) {

}
