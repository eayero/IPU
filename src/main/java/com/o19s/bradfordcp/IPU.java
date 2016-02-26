package com.o19s.bradfordcp;

import com.google.common.collect.ImmutableMap;
import com.google.common.io.PatternFilenameFilter;
import org.apache.cassandra.config.*;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.Directories;
import org.apache.cassandra.db.Keyspace;
import org.apache.cassandra.db.compaction.CompactionManager;
import org.apache.cassandra.db.compaction.Upgrader;
import org.apache.cassandra.io.sstable.*;
import org.apache.cassandra.locator.AbstractReplicationStrategy;
import org.apache.cassandra.thrift.TFramedTransportFactory;
import org.apache.cassandra.tools.BulkLoader;
import org.apache.cassandra.utils.JVMStabilityInspector;
import org.apache.cassandra.utils.OutputHandler;

import java.io.File;
import java.net.InetAddress;
import java.util.*;
import java.util.concurrent.TimeUnit;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class IPU
{
  private static void cleanupSnapshotFilenames(String directoryPath) {
    File directory = new File(directoryPath);
    Pattern snapshotPattern = Pattern.compile("[0-9]{10}-(.*)");
    PatternFilenameFilter pff = new PatternFilenameFilter(snapshotPattern);

    File[] filesToClean = directory.listFiles(pff);
    for (File sourceFile : filesToClean) {
      Matcher m = snapshotPattern.matcher(sourceFile.getName());
      if (m.find()) {
        File destFile = new File(directory.getAbsolutePath() + File.separator + m.group(1));
        sourceFile.renameTo(destFile);
      }
    }
  }

  private static void upgrade(String keyspaceName, String columnFamilyName) {
    upgrade(keyspaceName, columnFamilyName, false, false);
  }

  private static void upgrade(String keyspaceName, String columnFamilyName, boolean keepSource, boolean debug) {
    // Set the addresses to pull schema CFMetaData from
    HashSet<InetAddress> addresses = new HashSet<InetAddress>();
    try {
      addresses.add(InetAddress.getLoopbackAddress());
    } catch (Exception ex) {
      System.err.println("Couldn't find localhost!");

      if (debug)
        ex.printStackTrace(System.err);
      System.exit(1);
    }

    // Inform C* classes that we're operating as a client (after first loading a config so certain variables are set)
    try {
      DatabaseDescriptor.getFileCacheSizeInMB();
      Config.setClientMode(true);
    } catch (Exception ex) {
      System.err.println(ex);

      if (debug)
        ex.printStackTrace(System.err);
      System.exit(1);
    }

    // Create a client to pull CFMetadata from another cluster (optionally we could load a CQL file containing a CREATE TABLE statement and use that).
    SSTableLoader.Client client = new BulkLoader.ExternalClient(addresses,
        9160,
        "",
        "",
        new TFramedTransportFactory(),
        7000,
        7001,
        new EncryptionOptions.ServerEncryptionOptions());
    client.init(keyspaceName);

    // Retrieve the CFMetaData from our target cluster
    CFMetaData cfmd = client.getCFMetaData(keyspaceName, columnFamilyName);

    // Stop the client, we don't need it anymore
    client.stop();

    // Setup for upgrading
    OutputHandler handler = new OutputHandler.SystemOutput(false, debug);
    Collection<SSTableReader> readers = new ArrayList<SSTableReader>();

    try {
      // Build Keyspace Metadata
      KSMetaData ksmd = KSMetaData.newKeyspace(cfmd.ksName,
          AbstractReplicationStrategy.getClass("org.apache.cassandra.locator.SimpleStrategy"),
          ImmutableMap.of("replication_factor", "1"),
          true,
          Collections.singleton(cfmd));

      // Load the remote ColumnFamily MetaData and built Keyspace MetaData
      Schema.instance.load(ksmd);
      Keyspace keyspace = Keyspace.openWithoutSSTables("est");
      ColumnFamilyStore cfs = keyspace.getColumnFamilyStore(columnFamilyName);

      Directories.SSTableLister lister = cfs.directories.sstableLister();
      for (Map.Entry<Descriptor, Set<Component>> entry : lister.list().entrySet()) {
        Set<Component> components = entry.getValue();
        if (!components.contains(Component.DATA) || !components.contains(Component.PRIMARY_INDEX))
          continue;

        try
        {
          SSTableReader sstable = SSTableReader.openNoValidation(entry.getKey(), components, cfs.metadata);
          if (sstable.descriptor.version.equals(Descriptor.Version.CURRENT))
            continue;
          readers.add(sstable);
        }
        catch (Exception e)
        {
          JVMStabilityInspector.inspectThrowable(e);
          System.err.println(String.format("Error Loading %s: %s", entry.getKey(), e.getMessage()));
          if (debug)
            e.printStackTrace(System.err);

          continue;
        }
      }

      int numSSTables = readers.size();
      handler.output("Found " + numSSTables + " sstables that need upgrading.");

      for (SSTableReader sstable : readers)
      {
        try
        {
          Upgrader upgrader = new Upgrader(cfs, sstable, handler);
          upgrader.upgrade();

          if (!keepSource)
          {
            // Remove the sstable (it's been copied by upgrade)
            System.out.format("Deleting table %s.%n", sstable.descriptor.baseFilename());
            sstable.markObsolete(null);
            sstable.selfRef().release();
          }
        }
        catch (Exception e)
        {
          System.err.println(String.format("Error upgrading %s: %s", sstable, e.getMessage()));
          if (debug)
            e.printStackTrace(System.err);
        }
      }
      CompactionManager.instance.finishCompactionsAndShutdown(5, TimeUnit.MINUTES);
      SSTableDeletingTask.waitForDeletions();
      System.exit(0);
    } catch (Exception ex) {
      System.err.println(ex);
      ex.printStackTrace(System.err);
      System.exit(1);
    }
  }

  public static void main(String[] args)
  {
    String keyspace = "keyspace_name";
    String column_family = "table_name";

    // Cleanup filename generated by the snapshot
    cleanupSnapshotFilenames("./data/" + keyspace + File.separator + column_family);

    // Perform the upgrade
    upgrade(keyspace, column_family);
  }
}
