# Erasmus Expenses ETL Pipeline

A professional-grade Data Engineering project built in Java that implements a complete Extract, Transform, and Load (ETL) pipeline. This application processes raw student expense data from a CSV file, applies business logic for currency conversion and data sanitization, and loads the structured data into a PostgreSQL database.

## 🛠 Tech Stack & Tools
* **Language:** Java 17+
* **Database:** PostgreSQL 18
* **Database Management:** DBeaver Community Edition
* **Driver:** PostgreSQL JDBC Driver (pgjdbc)
* **IDE:** IntelliJ IDEA
* **Version Control:** Git & GitHub

## ⚙ Core Pipeline Architecture

1. **Extract (E):**
   Reads raw expense data from `expenses.csv` using efficient `BufferedReader` streams, gracefully handling file I/O operations and skipping header rows.

2. **Transform (T):**
    * **Data Sanitization:** Detects missing descriptions and assigns default values (`Unknown Expense`).
    * **Defensive Parsing:** Identifies corrupted numeric fields (e.g., text strings instead of numbers) and safely discards the row without crashing the pipeline.
    * **Business Logic:** Automatically identifies Polish Zloty (`PLN`) transactions and converts them to Euros (`EUR`) using a fixed exchange rate.

3. **Load (L):**
    * **JDBC Batching:** Groups processed rows using `addBatch()` and executes them simultaneously via `executeBatch()` for optimal database performance.
    * **ACID Transactions:** Manages database commits manually (`setAutoCommit(false)`), ensuring that either all valid data is written to the `clean_expenses` table, or nothing is, maintaining data integrity.

## 🗄 Database Schema

The pipeline injects the transformed data into the following PostgreSQL table structure:

```sql
CREATE TABLE clean_expenses (
    id SERIAL PRIMARY KEY,
    original_id VARCHAR(50),
    concept VARCHAR(255),
    amount NUMERIC(10, 2),
    currency VARCHAR(10),
    expense_date DATE
);