package com.parquetread;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.example.data.Group;
import org.apache.parquet.example.data.simple.SimpleGroupFactory;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.hadoop.example.GroupWriteSupport;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.MessageTypeParser;

import java.io.IOException;

/**
 * Helper class to create a sample Parquet file for testing
 */
public class SampleParquetCreator {

    public static void createSampleFile(String outputPath) throws IOException {
        // Define schema
        String schemaString = "message sample {\n" +
                "  required binary name (UTF8);\n" +
                "  required int32 age;\n" +
                "  required binary city (UTF8);\n" +
                "}";
        
        MessageType schema = MessageTypeParser.parseMessageType(schemaString);
        SimpleGroupFactory groupFactory = new SimpleGroupFactory(schema);
        
        Configuration conf = new Configuration();
        Path path = new Path(outputPath);
        
        GroupWriteSupport.setSchema(schema, conf);
        
        try (ParquetWriter<Group> writer = new ParquetWriter<>(
                path,
                new GroupWriteSupport(),
                org.apache.parquet.hadoop.metadata.CompressionCodecName.SNAPPY,
                1024 * 1024,
                1024,
                512,
                true,
                false,
                org.apache.parquet.column.ParquetProperties.WriterVersion.PARQUET_1_0,
                conf)) {
            
            // Write sample data
            Group group1 = groupFactory.newGroup()
                    .append("name", "Alice")
                    .append("age", 30)
                    .append("city", "New York");
            writer.write(group1);
            
            Group group2 = groupFactory.newGroup()
                    .append("name", "Bob")
                    .append("age", 25)
                    .append("city", "San Francisco");
            writer.write(group2);
            
            Group group3 = groupFactory.newGroup()
                    .append("name", "Charlie")
                    .append("age", 35)
                    .append("city", "Seattle");
            writer.write(group3);
            
            Group group4 = groupFactory.newGroup()
                    .append("name", "Diana")
                    .append("age", 28)
                    .append("city", "Boston");
            writer.write(group4);
        }
        
        System.out.println("Sample Parquet file created at: " + outputPath);
    }
    
    public static void main(String[] args) throws IOException {
        String outputPath = args.length > 0 ? args[0] : "/tmp/sample.parquet";
        createSampleFile(outputPath);
    }
}
