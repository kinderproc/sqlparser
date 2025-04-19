package com.lightspeed;

import java.util.List;

public record Query(List<String> columns, List<Source> tables) {

}
