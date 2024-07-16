# MeshJoin_ETL - README

## Project Overview

This project involves building and analyzing a data warehouse prototype for a metro system using a star schema. The goal is to analyze business information from transaction data by implementing a near-real-time data warehouse. The project includes creating a data warehouse with 10,000 transaction records and 100 products, joining incomplete transaction data with master data using the Mesh Join algorithm.

## Project Details

- **Project Title**: Building and Analyzing Data Warehouse Prototype for Metro Using Mesh Join Algorithm
- **Author**: Abdullah Shakeel

## Step 1: Database Setup

### Import Star Schema

1. Navigate to the `DDL` folder.
2. Locate the `warehouse.sql` file.
3. Import the `warehouse.sql` file into your MySQL Workbench.
4. Run the script to create a new schema with 6 different tables:
   - 5 Dimension tables (Dates, Store, Supplier, Customer, Products)
   - 1 Fact table (Sale)

### Star Schema Design

The star schema includes the following dimensions and a fact table:
- **Dimensions**: Dates, Store, Supplier, Customer, Products
- **Fact Table**: Sale

## Step 2: Implement Mesh Join Algorithm

### Load Java Files

1. Navigate to the `src->warehouse` folder.
2. Locate the following Java files:
   - `Connection.java`
   - `Main.java`
3. Load both files into your Eclipse IDE.
4. Run the `Main.java` file. The program will prompt you for your MySQL username and password.
5. Enter your credentials. If the information is correct, the program will connect to the database.
6. The program will load the data (transaction and master) and write to the database according to the mesh join algorithm.

### Mesh Join Algorithm

The Mesh Join algorithm, introduced by Polyzotis in 2008, is used to perform join operations during the transformation phase of ETL. The main components include:
- **Disk Buffer**: An array used to load partitions of master data (MD) into memory.
- **Hash Table**: Stores customer transactions (tuples) based on product ID.
- **Queue**: Keeps track of customer transactions in memory based on arrival times.

#### Steps to Implement Mesh Join

1. Read 50 tuples from the `TRANSACTIONS` table into the hash table.
2. Load the next MD partition into the disk buffer.
3. Compare the MD partition with the hash table.
4. If a match is found, calculate the required attributes (e.g., total sale) and load the transaction tuple into the data warehouse (DW), adding information to the dimension tables as necessary.
5. Remove the oldest partition of transactions from the hash table after each iteration.
6. Repeat until all data from the `TRANSACTIONS` table is loaded into the DW.

#### Pseudocode

```plaintext
While (true):
    Read transaction_data (50 tuples)
    Add to hash table
    Add join attribute to queue
    Load master data partition
    Compare master data partition with hash table
    If match:
        Aggregate and add to DW
    If queue.is full():
        Dequeue()
        Remove corresponding records from hash table
    If transaction limit reaches 10,000 and queue is empty:
        Break
```

## Step 3: Perform SQL Queries

### Run Analysis Queries

1. Navigate to the `Queries` folder.
2. Locate the `Queries.sql` file.
3. Import the `Queries.sql` file into your MySQL Workbench.
4. Uncomment each query in the file and run them individually.
5. Review the analysis results displayed.
