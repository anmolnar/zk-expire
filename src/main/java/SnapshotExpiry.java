import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.jute.BinaryInputArchive;
import org.apache.jute.InputArchive;
import org.apache.zookeeper.server.DataNode;
import org.apache.zookeeper.server.DataTree;
import org.apache.zookeeper.server.persistence.FileSnap;

import java.io.BufferedInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.zip.Adler32;
import java.util.zip.CheckedInputStream;

public class SnapshotExpiry implements AutoCloseable {

  private final String snapshotFileName;
  private final String znode;
  private final boolean ctime;
  private final long expiryDays;

  public SnapshotExpiry(String snapshotFile, String znode, boolean ctime, long expiryDays) {
    this.snapshotFileName = snapshotFile;
    this.znode = znode;
    this.ctime = ctime;
    this.expiryDays = expiryDays;
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

  public void run() throws IOException {
    InputStream is = new CheckedInputStream(
        new BufferedInputStream(Files.newInputStream(Paths.get(snapshotFileName))),
        new Adler32());
    InputArchive ia = BinaryInputArchive.getArchive(is);
    FileSnap fileSnap = new FileSnap(null);
    DataTree dataTree = new DataTree();
    Map<Long, Integer> sessions = new HashMap<Long, Integer>();
    fileSnap.deserialize(dataTree, sessions, ia);
    printZnode(dataTree, znode);
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

      return new SnapshotExpiry(cli.getOptionValue("snapshot-file"), cli.getOptionValue("znode"),
          options.hasOption("ctime"), Long.parseLong(cli.getOptionValue("expiry-days")));
    } catch (ParseException e) {
      System.out.printf("%s\n\n", e.getMessage());
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
    // empty
  }

  private void printZnode(DataTree dataTree, String name) {
    DataNode n = dataTree.getNode(name);
    Set<String> children;
    Date now = new Date();
    Date znodeDate = new Date(ctime ? n.stat.getCtime() : n.stat.getMtime());
    long age = TimeUnit.DAYS.convert(now.getTime() - znodeDate.getTime(), TimeUnit.MILLISECONDS);
    if (age > expiryDays) {
      System.out.printf("Delete: %s - %d days\n", name, age);
      deleteRecursively(dataTree, name);
    } else {
      children = n.getChildren();
      for (String child : children) {
        printZnode(dataTree, name + (name.equals("/") ? "" : "/") + child);
      }
    }
  }

  private void deleteRecursively(DataTree dataTree, String node) {
    DataNode dn = dataTree.getNode(node);
    Set<String> children = dn.getChildren();
    for (String child : children) {
      deleteRecursively(dataTree, node + (node.equals("/") ? "" : "/") + child);
    }
    System.out.println("Deleted " + node);  // delete here
  }

}
