import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Date;

/**
 * ErasmusETL Pipeline
 *
 * A professional-grade ETL (Extract, Transform, Load) pipeline designed to process
 * student expenses from a CSV file, apply currency transformations, sanitize data,
 * and load the finalized records into a PostgreSQL database.
 *
 * Core Features:
 * - Resource management via Try-With-Resources.
 * - JDBC Batch Processing for optimized database inserts.
 * - ACID Transaction management (AutoCommit disabled) for data integrity.
 * - Defensive programming against malformed data.
 *
 * @author Alvaro (adlerauge)
 * @version 1.0
 */
public class ErasmusETL {

    // Pipeline Configuration Constants
    private static final String INPUT_FILE = "expenses.csv";
    private static final double PLN_TO_EUR_RATE = 0.23;

    // Database Connection Constants
    private static final String DB_URL = "jdbc:postgresql://localhost:5432/postgres";
    private static final String DB_USER = "postgres";
    private static final String DB_PASSWORD = "admin";

    /**
     * Main execution entry point for the ETL pipeline.
     * @param args Command line arguments (unused).
     */
    public static void main(String[] args) {
        System.out.println("=== STARTING ENTERPRISE ETL PIPELINE ===");
        executePipeline(INPUT_FILE);
        System.out.println("=== ETL PIPELINE COMPLETED SUCCESSFULLY ===");
    }

    /**
     * Coordinates the Extract, Transform, and Load phases.
     * Utilizes batch processing and manual transaction control to ensure data integrity.
     *
     * @param filePath The relative path to the source CSV data file.
     */
    private static void executePipeline(String filePath) {
        String insertSQL = "INSERT INTO clean_expenses (original_id, concept, amount, currency, expense_date) VALUES (?, ?, ?, ?, ?)";

        // Try-with-resources safely handles the closing of streams and connections
        try (BufferedReader reader = new BufferedReader(new FileReader(filePath));
             Connection conn = DriverManager.getConnection(DB_URL, DB_USER, DB_PASSWORD);
             PreparedStatement pstmt = conn.prepareStatement(insertSQL)) {

            System.out.println("[SYSTEM] Connected to PostgreSQL successfully.");

            // 1. Transaction Control: Disable auto-commit to manage the transaction manually
            conn.setAutoCommit(false);

            // Skip the CSV header row
            reader.readLine();

            String line;
            int batchSize = 0;

            // EXTRACT Phase
            while ((line = reader.readLine()) != null) {
                // TRANSFORM & LOAD PREPARATION Phase
                if (processAndBatchRow(line, pstmt)) {
                    batchSize++;
                }
            }

            // LOAD Phase (Execution)
            if (batchSize > 0) {
                System.out.println("[SYSTEM] Executing batch insert of " + batchSize + " records...");
                pstmt.executeBatch();

                // 2. Transaction Control: Commit all changes atomically
                conn.commit();
                System.out.println("[SUCCESS] All records committed to the database.");
            }

        } catch (IOException e) {
            System.err.println("[ERROR] IO Exception during extraction: " + e.getMessage());
        } catch (SQLException e) {
            System.err.println("[ERROR] Database Exception during load phase: " + e.getMessage());
        }
    }

    /**
     * Transforms a single raw CSV row into sanitized data and adds it to the SQL execution batch.
     *
     * @param row   The raw comma-separated data string representing a single expense.
     * @param pstmt The PreparedStatement holding the parameterized insert query.
     * @return true if the row was successfully batched; false if it was discarded.
     * @throws SQLException If an error occurs while setting the SQL parameters.
     */
    private static boolean processAndBatchRow(String row, PreparedStatement pstmt) throws SQLException {
        String[] columns = row.split(",");

        // Defensive check against missing columns
        if (columns.length < 5) return false;

        // Extract and trim to remove invisible trailing/leading spaces
        String id = columns[0].trim();
        String concept = columns[1].trim();
        String amountStr = columns[2].trim();
        String currency = columns[3].trim();
        String date = columns[4].trim();

        // Handle missing descriptions
        if (concept.isEmpty()) {
            concept = "Unknown Expense";
        }

        // Parse numeric amount safely
        double amount;
        try {
            amount = Double.parseDouble(amountStr);
        } catch (NumberFormatException e) {
            System.out.println("  [WARN] DISCARDED Row ID " + id + ": Malformed amount data.");
            return false; // Skip invalid records without crashing the pipeline
        }

        // Currency conversion logic
        if ("PLN".equalsIgnoreCase(currency)) {
            amount = amount * PLN_TO_EUR_RATE;
            currency = "EUR";
        }

        // Map clean variables to SQL parameters
        pstmt.setString(1, id);
        pstmt.setString(2, concept);
        pstmt.setDouble(3, amount);
        pstmt.setString(4, currency);
        pstmt.setDate(5, Date.valueOf(date));

        // Add the populated statement to the current batch
        pstmt.addBatch();

        System.out.printf("  [INFO] Queued -> ID: %s | %s | %.2f %s | %s%n", id, concept, amount, currency, date);
        return true;
    }
}