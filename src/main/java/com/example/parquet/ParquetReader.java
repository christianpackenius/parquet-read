package com.example.parquet;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.column.page.PageReadStore;
import org.apache.parquet.example.data.Group;
import org.apache.parquet.example.data.simple.convert.GroupRecordConverter;
import org.apache.parquet.hadoop.ParquetFileReader;
import org.apache.parquet.hadoop.util.HadoopInputFile;
import org.apache.parquet.io.ColumnIOFactory;
import org.apache.parquet.io.MessageColumnIO;
import org.apache.parquet.io.RecordReader;
import org.apache.parquet.schema.MessageType;

import java.io.*;
import java.util.zip.Deflater;
import java.util.zip.ZipEntry;
import java.util.zip.ZipOutputStream;

public class ParquetReader {
  public static void readParquetFile(String filePath) throws IOException {
    HadoopInputFile hadoopInputFile = getHadoopInputFileFromPath(filePath);
    String zipOutput = java.nio.file.Path.of(filePath).toAbsolutePath() + "-context.zip";
    String entryName = java.nio.file.Path.of(filePath).toAbsolutePath().getFileName() + "-content.txt";
    try (ZipOutputStream zos = new ZipOutputStream(new BufferedOutputStream(new FileOutputStream(zipOutput), 1024 * 1024 * 16))) {
      zos.setLevel(Deflater.BEST_COMPRESSION);
      zos.putNextEntry(new ZipEntry(entryName));
      try (ParquetFileReader reader = ParquetFileReader.open(hadoopInputFile);
           PrintStream out = new PrintStream(new BufferedOutputStream(zos, 1024 * 1024 * 16))) {
        printParquetContent(reader, out);
      }
    }
  }

  private static void printParquetContent(ParquetFileReader reader, PrintStream out) throws IOException {
    MessageType schema = reader.getFooter().getFileMetaData().getSchema();

    out.println("=== Parquet File Schema ===");
    out.println();
    out.println(schema);
    out.println();

    out.println("=== Parquet File Contents ===");
    out.println();

    PageReadStore pages;
    long recordCount = 0;

    while ((pages = reader.readNextRowGroup()) != null) {
      long rows = pages.getRowCount();
      MessageColumnIO columnIO = new ColumnIOFactory().getColumnIO(schema);
      RecordReader<Group> recordReader = columnIO.getRecordReader(pages, new GroupRecordConverter(schema));

      for (int i = 0; i < rows; i++) {
        Group group = recordReader.read();
        recordCount++;
        out.println("*** Record " + recordCount + " ***");
        out.println(group);
      }
    }

    out.println();
    out.println("Total records read: " + recordCount);
  }

  private static HadoopInputFile getHadoopInputFileFromPath(String filePath) throws IOException {
    File file = getFileFromPath(filePath);

    // Set system properties before creating Hadoop configuration
    System.setProperty("hadoop.home.dir", "/");

    // Explicitly use local file system
    Configuration conf = new Configuration();
    conf.set("fs.file.impl", org.apache.hadoop.fs.LocalFileSystem.class.getName());
    conf.set("fs.hdfs.impl", org.apache.hadoop.hdfs.DistributedFileSystem.class.getName());

    // Convert to absolute path and create Hadoop Path with proper URI format
    // This ensures correct file positioning when reading Parquet files
    Path path = new Path(file.getAbsoluteFile().toURI().toString());

    return HadoopInputFile.fromPath(path, conf);
  }

  private static File getFileFromPath(String filePath) {
    if (filePath == null || filePath.trim().isEmpty()) {
      throw new IllegalArgumentException("File path cannot be null or empty");
    }

    File file = new File(filePath);
    if (!file.exists()) {
      throw new IllegalArgumentException("File does not exist: " + filePath);
    }

    if (!file.isFile()) {
      throw new IllegalArgumentException("Path is not a file: " + filePath);
    }
    return file;
  }

  public static void main(String[] args) throws IOException {
    readParquetFile("train-00000-of-00026.parquet");
    readParquetFile("train-00001-of-00026.parquet");
    readParquetFile("train-00002-of-00026.parquet");
    readParquetFile("train-00003-of-00026.parquet");
    readParquetFile("train-00004-of-00026.parquet");
    readParquetFile("train-00005-of-00026.parquet");
    readParquetFile("train-00006-of-00026.parquet");
    readParquetFile("train-00007-of-00026.parquet");
    readParquetFile("train-00008-of-00026.parquet");
    readParquetFile("train-00009-of-00026.parquet");
  }
}
