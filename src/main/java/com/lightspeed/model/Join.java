package com.lightspeed.model;

public record Join(String type, String table, String alias, String condition) {
}
