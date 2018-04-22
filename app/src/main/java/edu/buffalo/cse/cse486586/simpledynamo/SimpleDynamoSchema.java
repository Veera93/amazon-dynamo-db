package edu.buffalo.cse.cse486586.simpledynamo;

import android.provider.BaseColumns;

/**
 * Created by veera on 4/22/18.
 */

/*
 * Defines the schema of the Table
 */

//From: developers.android.com
public final class SimpleDynamoSchema {
    // To prevent someone from accidentally instantiating the contract class,
    // make the constructor private.
    private SimpleDynamoSchema() {}

    /* Inner class that defines the table contents */
    public static class SimpleDynamoDataEntry implements BaseColumns {
        public static final String TABLE_NAME = "dynamo";
        public static final String COLUMN_NAME_KEY = "key";
        public static final String COLUMN_NAME_VALUE = "value";
    }
}
