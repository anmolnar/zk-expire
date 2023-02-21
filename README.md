# zk-expire

Delete expired znodes in ZooKeeper database based on Snapshot file.

## Build

```
mvn clean install
mvn dependency:copy-dependencies -DoutputDirectory=lib
```

## Run

```
java -cp "./lib/*:target/zk-expire-1.0-SNAPSHOT.jar" SnapshotExpiry -h 
```

```
usage: SnapshotExpiry [-c] --expiry-days <arg> [-h] [-n] --server <arg>
       --snapshot-file <arg> --znode <arg>
 -c,--ctime                 Use ctime to calculate expiration. (default:
                            mtime)
    --expiry-days <arg>     Znode expiry in days. (required)
 -h,--help                  Print help message
 -n,--dry-run               Don't delete the znodes, just list them.
    --server <arg>          ZooKeeper server to connect. (required)
    --snapshot-file <arg>   Snapshot file location. (required)
    --znode <arg>           Root znode to scan for expired children.
                            (required)
```

### Example

```
java -cp "./lib/*:target/zk-expire-1.0-SNAPSHOT.jar" SnapshotExpiry --expiry-days 7 --server localhost --znode /ranger/tokenroot --snapshot-file /tmp/zookeeper/version-2/snapshot.9
```

### Additional options

You can pass additional parameters to the ZooKeeper client with Java properties in the command line, like `-Dzookeeper.client.secure=true` for SSL connection.
