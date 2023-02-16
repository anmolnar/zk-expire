import java.io.BufferedInputStream;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.HashMap;
import java.util.Map;
import java.util.zip.Adler32;
import java.util.zip.CheckedInputStream;

public class SnapshotExpirer {

  /**
   * USAGE: SnapshotFormatter snapshot_file
   */
  public static void main(String[] args) throws Exception {
    if (args.length != 1) {
      System.err.println("USAGE: SnapshotFormatter snapshot_file");
      System.exit(2);
    }

    new SnapshotExpirer().run(args[0]);
  }

  public void run(String snapshotFileName) throws IOException {
    InputStream is = new CheckedInputStream(
        new BufferedInputStream(new FileInputStream(snapshotFileName)),
        new Adler32());
    InputArchive ia = BinaryInputArchive.getArchive(is);

    FileSnap fileSnap = new FileSnap(null);

    DataTree dataTree = new DataTree();
    Map<Long, Integer> sessions = new HashMap<Long, Integer>();

    fileSnap.deserialize(dataTree, sessions, ia);

    printDetails(dataTree, sessions);
  }

}
