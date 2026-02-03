# Creating Test Parquet Files

This document explains how to create test Parquet files to use with the parquet-read application.

## Using Python and PyArrow

If you have Python installed, you can easily create test Parquet files using the PyArrow library.

### Installation

```bash
pip install pyarrow
```

### Create a Test File

Create a Python script (e.g., `create_test.py`):

```python
import pyarrow as pa
import pyarrow.parquet as pq

# Create sample data
data = {
    'id': [1, 2, 3],
    'name': ['Alice', 'Bob', 'Charlie'],
    'age': [30, 25, 35],
    'email': ['alice@example.com', 'bob@example.com', 'charlie@example.com']
}

# Create a PyArrow table
table = pa.table(data)

# Write to parquet file
pq.write_table(table, 'test.parquet')

print("Created test.parquet successfully!")
```

Run the script:

```bash
python create_test.py
```

### Read the Test File

Now you can read the generated Parquet file:

```bash
java -jar target/parquet-read-1.0-SNAPSHOT.jar test.parquet
```

## Using Other Tools

You can also create Parquet files using:
- Apache Spark
- Pandas (Python): `df.to_parquet('file.parquet')`
- Apache Drill
- Any data processing tool that supports Parquet output format

## Example Output

When reading a test file, you should see output similar to:

```
Reading Parquet file: test.parquet

=== Parquet File Schema ===
message schema {
  optional int64 id;
  optional binary name (STRING);
  optional int64 age;
  optional binary email (STRING);
}

=== Parquet File Contents ===
Record 1: id: 1
name: Alice
age: 30
email: alice@example.com

Record 2: id: 2
name: Bob
age: 25
email: bob@example.com

Record 3: id: 3
name: Charlie
age: 35
email: charlie@example.com

Total records read: 3
```
