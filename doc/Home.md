Welcome to the cassandra-backup wiki!

The documentation will help you to use the solution and to build procedures for backup, recovery and cleanup. 

Find the usage description for the utility below.

Subsequent pages describe: functionality description and procedures to handle cassandra-backup using this utility.

```
# Synopsis

java -jar backup-1.0-SNAPSHOT-final.jar [arguments] 

# Description

Take a snapshot of this nodes Cassandra data and upload it to remote storage.

Defaults to a snapshot of all keyspaces and their column families, but may be restricted to specific keyspaces or a single column-family.

# Usage

cassandra-backup [keyspace ...] [-t (--tag) snapshot-tag] [--cf (--column-family) name] [--drain] [-s (--speed) [slow | fast | ludicrous | plaid]] [-d (--duration) time] [-b (--bandwidth) data-rate] [-j (--jmx) jmx-url] [--ju jmx-user] [--jp jmx-password] [--bucket (--backup-bucket) bucket_name] [--id (--backup-id) cassandra-2] [--offline false] [--concurrent-connections count] [--wait] [--help] [--bs (--blob-storage) [AWS_S3 | AZURE_BLOB | GCP_BLOB | FILE]] 


-c (--cluster) cluster ID [--dd (--data-directory) /cassandra] [--fl (--filebackup-location) /backups] [--cd (--config-directory) /cassandra] [-p (--shared-path) /]

-t (--tag) snapshot-tag		: Snapshot tag name. Default is equiv. to 'autosnap-`date+%s`'

--cf (--column-family) name		: The column family to snapshot/upload. Requires a keyspace to be specified.

 --drain				: Optionally drain Cassandra following snapshot.

 -s (--speed) [slow | fast | ludicrous | plaid]: Speed to upload the com.instaclustr.backup.

 -d (--duration) time			: Calculate upload throughput based on total file size ÷ duration.

 -b (--bandwidth) data-rate		: Maximum upload throughput.

 -j (--jmx) jmx-url			: JMX service url for Cassandra

 --ju jmx-user				: JMX service user for Cassandra

 --jp jmx-password			: JMX service password for Cassandra

 --bucket (--backup-bucket) bucket_name : Container or bucket to store backups

 --id (--backup-id) cassandra-2	        : Cassandra backup id

--offline false				: Cassandra is not running (won't use JMX to snapshot, no token lists 
                                          uploaded)
 --concurrent-connections count         : Number of files (or file parts) to upload or download      
                                          concurrently. Higher values will increase throughput. Default is 10.

 --wait 				: Wait to acquire the global transfer lock (which prevents more than one 
                                          com.instaclustr.backup or restore from running).

 --help					: Show this message.

 --bs (--blob-storage) [AWS_S3 | AZURE_BLOB | GCP_BLOB | FILE]	: Blob storage provider (AWS, AZURE, GCP, FILE) 

 -c (--cluster) cluster ID		: Parent cluster of node to restore from.

 --dd (--data-directory) /cassandra	: Base directory that contains the Cassandra data, cache and commitlog                                                                                     
                                          directories

 --fl (--filebackup-location) /backups  : Base directory destination for filesystem based backups

 --cd (--config-directory) /cassandra   : Base directory that contains the Cassandra data, cache and commitlog directories

 -p (--shared-path) /			: Shared Container path for pod


If neither --speed or --bandwidth is specified, then a default speed of 'fast' is used.

If --duration is specified then the upload throughput is calculated as (total commitlog size ÷ duration), with a minimum speed of 500KB/s.
Specifying --bandwidth in addition to --duration will cap the upload bandwidth (i.e. min(bandwidth, calculated bandwidth)).

Times and data rates are specified by a numerical value and unit suffix (optionally shortened and space separated).
e.g. '1h', '1 day', '2m', '3000 kbps'

Valid time units are: [nanoseconds, microseconds, milliseconds, seconds, minutes, hours, days]
Valid data rate units are: [bps, kbps, mbps, gbps]

The following pre-defined speeds may be used to specify combined bandwidth and concurrent connection limits:
slow               1.00 MB/s, 1 concurrent connection
fast               10.00 MB/s, 1 concurrent connection
ludicrous          10.00 MB/s, 10 concurrent connections
plaid              unlimited, 100 concurrent connections
```
