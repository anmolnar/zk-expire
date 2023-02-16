import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.jute.BinaryInputArchive;
import org.apache.jute.InputArchive;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.server.DataNode;
import org.apache.zookeeper.server.DataTree;
import org.apache.zookeeper.server.persistence.FileSnap;

import java.io.BufferedInputStream;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.zip.Adler32;
import java.util.zip.CheckedInputStream;

public class SnapshotExpiry implements AutoCloseable, Watcher {

  private final String snapshotFileName;
  private final String znode;
  private final boolean ctime;
  private final long expiryDays;
  private final String server;

  private ZooKeeper zkclient;
  private final CountDownLatch connectLatch = new CountDownLatch(1);

  public SnapshotExpiry(String snapshotFile, String znode, boolean ctime, long expiryDays, String server) {
    this.snapshotFileName = snapshotFile;
    this.znode = znode;
    this.ctime = ctime;
    this.expiryDays = expiryDays;
    this.server = server;
  }

  /**
   * USAGE: SnapshotExpiry snapshot_file
   */
  public static void main(String[] args) throws Exception {
    try (final SnapshotExpiry se = parseCommandLine(args)) {
      assert se != null;

      if (se.ctime) {
        System.out.println("INFO: Using ctime for expiration");
      }

      se.run();
      System.exit(0);
    }
  }

  public void run() throws IOException, InterruptedException {
    zkclient = new ZooKeeper(server, 30000, this);
    if (!connectLatch.await(30L, TimeUnit.SECONDS)) {
      throw new IOException("Unable to connect to ZooKeeper");
    }
    System.out.println("Connected to ZooKeeper instance");
    InputStream is = new CheckedInputStream(
        new BufferedInputStream(Files.newInputStream(Paths.get(snapshotFileName))),
        new Adler32());
    InputArchive ia = BinaryInputArchive.getArchive(is);
    FileSnap fileSnap = new FileSnap(null);
    DataTree dataTree = new DataTree();
    Map<Long, Integer> sessions = new HashMap<Long, Integer>();
    fileSnap.deserialize(dataTree, sessions, ia);
    expireZnode(dataTree, znode);
  }

  private static SnapshotExpiry parseCommandLine(String[] args) {
    CommandLineParser parser = new DefaultParser();
    Options options = new Options();

    options.addRequiredOption(null, "snapshot-file", true, "Snapshot file location. (required)");
    options.addRequiredOption(null, "expiry-days", true, "Znode expiry in days. (required)");
    options.addRequiredOption(null, "server", true, "ZooKeeper server to connect. (required)");
    options.addRequiredOption(null, "znode", true, "Root znode to scan for expired children. (required)");

    options.addOption("h", "help", false, "Print help message");
    options.addOption("c", "ctime", false, "Use ctime to calculate expiration. (default: mtime)");
    options.addOption("n", "dry-run", false, "Don't delete the znodes, just list them.");

    try {
      CommandLine cli = parser.parse(options, args);
      if (cli.hasOption("help")) {
        printHelpAndExit(0, options);
      }

      File f = new File(cli.getOptionValue("snapshot-file"));
      if (!f.exists() || !f.isFile() || !f.canRead()) {
        throw new ParseException("Unable to open file for reading: " + cli.getOptionValue("snapshot-file"));
      }

      return new SnapshotExpiry(cli.getOptionValue("snapshot-file"), cli.getOptionValue("znode"),
          options.hasOption("ctime"), Long.parseLong(cli.getOptionValue("expiry-days")), cli.getOptionValue("server"));
    } catch (ParseException e) {
      System.out.printf("ERROR: %s\n\n", e.getMessage());
      printHelpAndExit(1, options);
      return null;
    }
  }

  private static void printHelpAndExit(int exitCode, Options options) {
    HelpFormatter help = new HelpFormatter();
    help.printHelp("SnapshotExpiry", options, true);
    System.exit(exitCode);
  }

  @Override
  public void close() throws Exception {
    if (zkclient != null) {
      zkclient.close();
      zkclient = null;
    }
  }

  @Override
  public void process(WatchedEvent watchedEvent) {
    if (watchedEvent.getState() == Event.KeeperState.SyncConnected) {
      connectLatch.countDown();
    }
  }

  private void expireZnode(DataTree dataTree, String name) throws InterruptedException {
    DataNode dn = dataTree.getNode(name);
    Set<String> children = dn.getChildren();
    for (String child : children) {
      String fullPath = name + (name.equals("/") ? "" : "/") + child;
      DataNode cn = dataTree.getNode(fullPath);
      Date now = new Date();
      Date znodeDate = new Date(ctime ? cn.stat.getCtime() : cn.stat.getMtime());
      long age = TimeUnit.DAYS.convert(now.getTime() - znodeDate.getTime(), TimeUnit.MILLISECONDS);
      if (age > expiryDays) {
        deleteRecursively(dataTree, fullPath);
        System.out.printf("Deleted: %s - %d days\n", fullPath, age);
      }
    }
  }

  private void deleteRecursively(DataTree dataTree, String node) throws InterruptedException {
    DataNode dn = dataTree.getNode(node);
    Set<String> children = dn.getChildren();
    for (String child : children) {
      deleteRecursively(dataTree, node + (node.equals("/") ? "" : "/") + child);
    }
    try {
      zkclient.delete(node, dn.stat.getVersion());
    } catch (KeeperException e) {
      if (e.code() != KeeperException.Code.NONODE) {
        System.out.println("Unable to delete: " + node + " - " + e.getMessage());
      }
    }
  }
}
