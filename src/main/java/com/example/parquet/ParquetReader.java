package com.example.parquet;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.hadoop.ParquetFileReader;
import org.apache.parquet.hadoop.util.HadoopInputFile;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.column.page.PageReadStore;
import org.apache.parquet.example.data.Group;
import org.apache.parquet.example.data.simple.SimpleGroup;
import org.apache.parquet.example.data.simple.convert.GroupRecordConverter;
import org.apache.parquet.io.ColumnIOFactory;
import org.apache.parquet.io.MessageColumnIO;
import org.apache.parquet.io.RecordReader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;

/**
 * A simple Parquet file reader that reads and displays the contents of a Parquet file.
 */
public class ParquetReader {
    
    private static final Logger logger = LoggerFactory.getLogger(ParquetReader.class);

    /**
     * Reads and displays the contents of a Parquet file.
     *
     * @param filePath the path to the Parquet file
     * @throws IOException if an error occurs while reading the file
     * @throws IllegalArgumentException if the file path is invalid
     */
    public static void readParquetFile(String filePath) throws IOException {
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
        
        // Set system properties before creating Hadoop configuration
        System.setProperty("hadoop.home.dir", "/");
        
        Configuration conf = new Configuration();
        // Explicitly use local file system
        conf.set("fs.file.impl", org.apache.hadoop.fs.LocalFileSystem.class.getName());
        conf.set("fs.hdfs.impl", org.apache.hadoop.hdfs.DistributedFileSystem.class.getName());
        
        // Convert to absolute path and create Hadoop Path with proper URI format
        // This ensures correct file positioning when reading Parquet files
        Path path = new Path(file.getAbsoluteFile().toURI().toString());

        try (ParquetFileReader reader = ParquetFileReader.open(HadoopInputFile.fromPath(path, conf))) {
            MessageType schema = reader.getFooter().getFileMetaData().getSchema();
            
            System.out.println("=== Parquet File Schema ===");
            System.out.println(schema);
            System.out.println();
            
            System.out.println("=== Parquet File Contents ===");
            
            PageReadStore pages;
            long recordCount = 0;
            
            while ((pages = reader.readNextRowGroup()) != null) {
                long rows = pages.getRowCount();
                MessageColumnIO columnIO = new ColumnIOFactory().getColumnIO(schema);
                RecordReader<Group> recordReader = columnIO.getRecordReader(pages, new GroupRecordConverter(schema));
                
                for (int i = 0; i < rows; i++) {
                    Group group = recordReader.read();
                    recordCount++;
                    System.out.println("Record " + recordCount + ": " + group);
                }
            }
            
            System.out.println();
            System.out.println("Total records read: " + recordCount);
        }
    }

    /**
     * Main method to run the Parquet reader.
     *
     * @param args command line arguments - expects the path to a Parquet file
     */
    public static void main(String[] args) {
        if (args.length == 0) {
            System.err.println("Usage: java -jar parquet-read.jar <path-to-parquet-file>");
            System.exit(1);
        }
        
        String filePath = args[0];
        
        try {
            System.out.println("Reading Parquet file: " + filePath);
            System.out.println();
            readParquetFile(filePath);
        } catch (IllegalArgumentException e) {
            System.err.println("Error: " + e.getMessage());
            logger.error("Invalid file path: {}", filePath, e);
            System.exit(1);
        } catch (IOException e) {
            System.err.println("Error reading Parquet file: " + e.getMessage());
            logger.error("Failed to read parquet file: {}", filePath, e);
            System.exit(1);
        }
    }
}
