package com.instaclustr.cassandra.backup.real;

public class Constants {

    // This is number of rows we inserted into Cassandra DB in total
    // we backed up first 6 rows. For the last two rows, they are stored in commit logs.
    // The backup procedure backs up also commit logs including remaining 2 rows
    // so commitlog restoration procedure restores them too
    public static int NUMBER_OF_INSERTED_ROWS = 8;

    // This is number of rows we expect to see in database after commitlog restoration
    // We omitted one row here, on purpose, to demonstrate point in time restoration
    public static int NUMBER_OF_ROWS_AFTER_RESTORATION = 7;
}
