# parquet-read

A Java program for reading and displaying Parquet files.

## Requirements

- Java 25
- Maven 3.6+

## Build

To build the project, run:

```bash
mvn clean package
```

This will create a shaded JAR file in the `target/` directory with all dependencies included.

## Usage

To read and display a Parquet file:

```bash
java -jar target/parquet-read-1.0-SNAPSHOT.jar <path-to-parquet-file>
```

Example:
```bash
java -jar target/parquet-read-1.0-SNAPSHOT.jar /path/to/data.parquet
```

## What it does

The program will:
1. Display the Parquet file schema
2. Show metadata (number of blocks, number of rows)
3. Display all data rows with their content

## Development

### Project Structure

```
parquet-read/
├── pom.xml                           # Maven configuration
├── src/
│   └── main/
│       └── java/
│           └── com/
│               └── parquetread/
│                   ├── Main.java                   # Main program
│                   └── SampleParquetCreator.java  # Test helper
└── README.md
```

### Dependencies

- Apache Parquet 1.13.1
- Apache Hadoop 3.3.6
- SLF4J Simple for logging