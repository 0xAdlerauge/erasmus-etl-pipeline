import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;

public class ErasmusETL {

    // Configuration constants
    private static final String INPUT_FILE = "expenses.csv";
    private static final double PLN_TO_EUR_RATE = 0.23;

    public static void main(String[] args) {
        System.out.println("--- STARTING ETL PIPELINE ---");
        processData(INPUT_FILE);
        System.out.println("--- ETL PIPELINE COMPLETED ---");
    }

    /**
     * Reads and coordinates the extraction of data from the CSV file.
     * @param filePath The path to the source file.
     */
    private static void processData(String filePath) {
        String line;

        // Try-with-resources ensures the file is automatically closed after reading
        try (BufferedReader reader = new BufferedReader(new FileReader(filePath))) {

            // Skip the header row
            reader.readLine();

            while ((line = reader.readLine()) != null) {
                processRow(line);
            }

        } catch (IOException e) {
            System.err.println("Critical Error: Unable to read the file. " + e.getMessage());
        }
    }

    /**
     * Transforms a single row of data, applies business rules, and outputs the result.
     * @param row The raw CSV line.
     */
    private static void processRow(String row) {
        String[] columns = row.split(",");

        // Defensive check: Ensure the row has the exact 5 columns we expect
        if (columns.length < 5) {
            System.out.println("⚠️ SKIPPED - Malformed row: " + row);
            return;
        }

        // Extract and trim spaces
        String id = columns[0].trim();
        String concept = columns[1].trim();
        String amountStr = columns[2].trim();
        String currency = columns[3].trim();
        String date = columns[4].trim();

        // 1. CLEANING: Handle empty concept
        if (concept.isEmpty()) {
            concept = "Unknown Expense";
        }

        // 2. CLEANING: Parse amount safely and detect corruption
        double amount = 0.0;
        try {
            amount = Double.parseDouble(amountStr);
        } catch (NumberFormatException e) {
            System.out.println("⚠️ DISCARDED - Row ID " + id + ": Corrupted amount (" + amountStr + ")");
            return; // Exit the method early for this specific row
        }

        // 3. TRANSFORMATION: Currency conversion from PLN to EUR
        if ("PLN".equalsIgnoreCase(currency)) {
            amount = amount * PLN_TO_EUR_RATE;
            currency = "EUR";
        }

        // 4. Output the sanitized and transformed data
        System.out.printf("✅ Processed -> ID: %s | %s | %.2f %s | %s%n", id, concept, amount, currency, date);
    }
}