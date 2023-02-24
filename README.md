# zk-expire

The problem with deleting large number of znodes from a ZooKeeper database is usually
related to the getChildren() call. The results cannot be paged, so if a znode has lots 
of children, the response cannot be sent over the network. Increasing jute.maxbuffer could
resolve the problem, but it's only a temporary solution and the side effects could be a 
serious problem and unacceptable in a production system.

This tool loads a snapshot file to make a list of child znodes and connects to a ZooKeeper
instance to delete them one by one. It requires the most up-to-date snapshot for reading
locally, but don't need to run on each ZooKeeper node, since it deletes znodes via standard
ZooKeeper Api.

No need for shutting down the ZooKeeper ensemble or a node, but the latest snapshot file
could be slightly outdated, since this tool doesn't read transaction logs to build the Data Tree.
This is okay as long as we use it for "expiring nodes": 6-7 days old znodes are probably 
already snapshotted, but still there's a possibility for working with an outdated snapshot.
The tool will silently skip non-existent znodes.

There's one limitation: the tool declines to work on the root "/" znode, because it could 
potentially  destroy the entire ZK database, so it's disallowed. Please choose a specific 
znode.

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
