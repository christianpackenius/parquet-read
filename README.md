# parquet-read

A Java application for reading and displaying Apache Parquet files.

## Description

This project provides a simple command-line tool to read and display the
contents of Parquet files. It uses Apache Parquet and Hadoop libraries to parse
Parquet files and print their schema and records to the console.

## Prerequisites

- Java 17 or higher
- Apache Maven 3.6 or higher

## Building the Project

To build the project, run:

```bash
mvn clean package
```

This will create a JAR file in the `target` directory.

## Usage

To read a Parquet file, run:

```bash
java -jar target/parquet-read-1.0-SNAPSHOT.jar <path-to-parquet-file>
```

Example:
```bash
java -jar target/parquet-read-1.0-SNAPSHOT.jar /path/to/your/file.parquet
```

The application will:
1. Display the Parquet file schema
2. Print all records in the file
3. Show the total number of records read

## Dependencies

- Apache Parquet 1.13.1
- Apache Hadoop 3.3.6
- SLF4J 2.0.9 (for logging)

## Java Compatibility

This application is compatible with Java 17-25. If you encounter issues with newer Java versions, the application includes compatibility fixes for:

- `UnsupportedOperationException: getSubject is not supported` (Java 18+)
- Hadoop filesystem initialization on Windows
- SLF4J logging conflicts

### Recommended Java Versions
- **Recommended**: Java 17 LTS or Java 21 LTS
- **Supported**: Java 17-25
- **Minimum**: Java 17

## License

This project is provided as-is for educational and demonstration purposes.

