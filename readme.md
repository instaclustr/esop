# Cassandra backup util
The Cassandra backup util is a project that allows you to take Cassandra snapshots, upload them to a blob store or folder of your choice and also restore a Cassandra node from a previously created snapshot. Cassandra backup util will also manage the jmx operation of taking the snapshot itself.

The Cassandra backup util can take full snapshots, incremental snapshots, per table and per keyspace snapshots. Token metadata is also maintained in relation to the snapshot taken, this makes restoring vnode enabled clusters simple and easy.

Cassandra backup util supports upload throttling based on throughput and expected time taken. Uploads to blob stores are done as multi-part uploads with automatic cleanup of failed uploads. 

The Cassandra backup util will also shortly support archiving commitlogs and point in time recovery.

Cassandra backup util also supports Cassandra running on Windows environments. 

## Use as a library
The Cassandra backup util can also be imported as a library for any JVM based project. It is currently used by the Instaclustr [cassandra operator](https://github.com/instaclustr/cassandra-operator) and within Instaclustrs [managed services](https://www.instaclustr.com). Commercial support for the backup util is available from [Instaclustr](https://www.instaclustr.com/services/cassandra-support/)

## TODO
* Documentation
* Testing for blob stores
* Commitlog archiving


Please see https://www.instaclustr.com/support/documentation/announcements/instaclustr-open-source-project-status/ for Instaclustr support status of this project
