package com.parquetread;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.hadoop.ParquetFileReader;
import org.apache.parquet.hadoop.metadata.ParquetMetadata;
import org.apache.parquet.hadoop.util.HadoopInputFile;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.example.data.Group;
import org.apache.parquet.hadoop.ParquetReader;
import org.apache.parquet.hadoop.example.GroupReadSupport;

import java.io.IOException;

/**
 * Main - A Java program to read and display Parquet files
 * 
 * Usage: java -jar parquet-read.jar <path-to-parquet-file>
 */
public class Main {

    public static void main(String[] args) {
        if (args.length < 1) {
            System.out.println("Usage: java -jar parquet-read.jar <path-to-parquet-file>");
            System.out.println("Example: java -jar parquet-read.jar /path/to/file.parquet");
            System.exit(1);
        }

        String parquetFilePath = args[0];
        
        try {
            readAndDisplayParquetFile(parquetFilePath);
        } catch (IOException e) {
            System.err.println("Error reading Parquet file: " + e.getMessage());
            e.printStackTrace();
            System.exit(1);
        }
    }

    /**
     * Reads and displays the content of a Parquet file
     * 
     * @param filePath Path to the Parquet file
     * @throws IOException if there's an error reading the file
     */
    public static void readAndDisplayParquetFile(String filePath) throws IOException {
        Configuration conf = new Configuration();
        Path path = new Path(filePath);
        
        // Display file metadata
        System.out.println("=================================================");
        System.out.println("Reading Parquet File: " + filePath);
        System.out.println("=================================================");
        
        try (ParquetFileReader fileReader = ParquetFileReader.open(HadoopInputFile.fromPath(path, conf))) {
            ParquetMetadata metadata = fileReader.getFooter();
            MessageType schema = metadata.getFileMetaData().getSchema();
            
            System.out.println("\nSchema:");
            System.out.println(schema);
            System.out.println("\nNumber of blocks: " + metadata.getBlocks().size());
            System.out.println("Number of rows: " + metadata.getBlocks().stream()
                    .mapToLong(block -> block.getRowCount())
                    .sum());
        }
        
        // Read and display data
        System.out.println("\n=================================================");
        System.out.println("Data:");
        System.out.println("=================================================");
        
        try (ParquetReader<Group> reader = ParquetReader.builder(new GroupReadSupport(), path)
                .withConf(conf)
                .build()) {
            
            Group record;
            int rowCount = 0;
            
            while ((record = reader.read()) != null) {
                rowCount++;
                System.out.println("Row " + rowCount + ":");
                System.out.println(record);
                System.out.println();
            }
            
            System.out.println("=================================================");
            System.out.println("Total rows displayed: " + rowCount);
            System.out.println("=================================================");
        }
    }
}
