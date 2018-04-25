package edu.buffalo.cse.cse486586.simpledynamo;

import android.content.Context;
import android.database.sqlite.SQLiteDatabase;
import android.database.sqlite.SQLiteOpenHelper;

import edu.buffalo.cse.cse486586.simpledynamo.SimpleDynamoSchema.SimpleDynamoDataEntry;

/**
 * Created by veera on 4/22/18.
 */

/*
 * Helper class that manages table creation and upgrades table if needed
 */

// From developers.android.com
public class SimpleDynamoDbHelper extends SQLiteOpenHelper {

    public static final int DATABASE_VERSION = 1;
    public static final String DATABASE_NAME = "Dynamo.db";

    private static final String SQL_CREATE_ENTRIES =
            "CREATE TABLE " + SimpleDynamoSchema.SimpleDynamoDataEntry.TABLE_NAME + " (" +
                    SimpleDynamoDataEntry.COLUMN_NAME_KEY + " TEXT PRIMARY KEY," +
                    SimpleDynamoDataEntry.COLUMN_NAME_VALUE + " TEXT)";

    private static final String SQL_DELETE_ENTRIES =
            "DROP TABLE IF EXISTS " + SimpleDynamoDataEntry.TABLE_NAME;
    public SimpleDynamoDbHelper(Context context) {
        super(context, DATABASE_NAME, null, DATABASE_VERSION);
    }

    @Override
    public void onCreate(SQLiteDatabase db) {
        db.execSQL(SQL_CREATE_ENTRIES);
    }

    @Override
    public void onUpgrade(SQLiteDatabase db, int oldVersion, int newVersion) {
        db.execSQL(SQL_DELETE_ENTRIES);
        onCreate(db);
    }
}
