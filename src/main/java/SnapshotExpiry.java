import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.jute.BinaryInputArchive;
import org.apache.jute.InputArchive;
import org.apache.zookeeper.server.DataTree;
import org.apache.zookeeper.server.persistence.FileSnap;

import java.io.BufferedInputStream;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.Map;
import java.util.zip.Adler32;
import java.util.zip.CheckedInputStream;

public class SnapshotExpiry implements AutoCloseable {

  private final String snapshotFileName;

  public SnapshotExpiry(String snapshotFile) {
    this.snapshotFileName = snapshotFile;
  }

  /**
   * USAGE: SnapshotExpiry snapshot_file
   */
  public static void main(String[] args) throws Exception {
    try (final SnapshotExpiry se = parseCommandLine(args)) {
      assert se != null;
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
    System.out.println(dataTree.getNodeCount());
  }

  private static SnapshotExpiry parseCommandLine(String[] args) {
    CommandLineParser parser = new DefaultParser();
    Options options = new Options();

    options.addRequiredOption(null, "snapshot-file", true, "Snapshot file location. (required)");
    options.addRequiredOption(null, "expiry-days", true, "Znode expiry in days. (required)");
    options.addRequiredOption(null, "server", true, "ZooKeeper server to connect. (required)");

    options.addOption("h", "help", false, "Print help message");
    options.addOption("c", "ctime", false, "Use ctime to calculate expiration. (default: mtime)");
    options.addOption("n", "dry-run", false, "Don't delete the znodes, just list them.");

    try {
      CommandLine cli = parser.parse(options, args);
      if (cli.hasOption("help")) {
        printHelpAndExit(0, options);
      }

      return new SnapshotExpiry(cli.getOptionValue("snapshot-file"));
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
}
