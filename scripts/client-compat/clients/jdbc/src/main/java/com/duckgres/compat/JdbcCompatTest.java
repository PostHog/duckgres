// JDBC client compatibility test for duckgres.
//
// Loads shared queries from queries.yaml, executes each one,
// and reports results to the results-gatherer. Also exercises
// JDBC-specific features: DatabaseMetaData, prepared statements,
// DDL/DML, and Metabase-style connection probing queries.
package com.duckgres.compat;

import java.io.OutputStream;
import java.net.HttpURLConnection;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.*;
import java.util.*;

import org.yaml.snakeyaml.LoaderOptions;
import org.yaml.snakeyaml.Yaml;
import org.yaml.snakeyaml.constructor.SafeConstructor;

public class JdbcCompatTest {

    private static final String CLIENT_NAME = "jdbc";

    private final String resultsUrl;
    private int passed = 0;
    private int failed = 0;

    public JdbcCompatTest() {
        String url = System.getenv("RESULTS_URL");
        this.resultsUrl = (url != null && !url.isEmpty()) ? url : "http://results-gatherer:8080";
    }

    // ── Reporting ──────────────────────────────────────────────

    private void report(String suite, String testName, String status, String detail) {
        if ("pass".equals(status)) {
            passed++;
            System.out.printf("  PASS  %s%s%n", testName, detail.isEmpty() ? "" : " (" + detail + ")");
        } else {
            failed++;
            System.out.printf("  FAIL  %s: %s%n", testName, detail);
        }

        try {
            String json = String.format(
                "{\"client\":\"%s\",\"suite\":\"%s\",\"test_name\":\"%s\",\"status\":\"%s\",\"detail\":\"%s\"}",
                CLIENT_NAME, suite, testName, status, detail.replace("\"", "\\\"").replace("\n", " "));
            HttpURLConnection conn = (HttpURLConnection) URI.create(resultsUrl + "/result").toURL().openConnection();
            conn.setRequestMethod("POST");
            conn.setDoOutput(true);
            conn.setConnectTimeout(2000);
            conn.setReadTimeout(2000);
            conn.setRequestProperty("Content-Type", "application/json");
            try (OutputStream os = conn.getOutputStream()) {
                os.write(json.getBytes(StandardCharsets.UTF_8));
            }
            conn.getResponseCode();
            conn.disconnect();
        } catch (Exception ignored) {
        }
    }

    private void done() {
        try {
            String json = String.format("{\"client\":\"%s\"}", CLIENT_NAME);
            HttpURLConnection conn = (HttpURLConnection) URI.create(resultsUrl + "/done").toURL().openConnection();
            conn.setRequestMethod("POST");
            conn.setDoOutput(true);
            conn.setConnectTimeout(2000);
            conn.setReadTimeout(2000);
            conn.setRequestProperty("Content-Type", "application/json");
            try (OutputStream os = conn.getOutputStream()) {
                os.write(json.getBytes(StandardCharsets.UTF_8));
            }
            conn.getResponseCode();
            conn.disconnect();
        } catch (Exception ignored) {
        }
    }

    // ── Connection ─────────────────────────────────────────────

    private static String env(String key, String fallback) {
        String v = System.getenv(key);
        return (v != null && !v.isEmpty()) ? v : fallback;
    }

    private static String jdbcUrl() {
        return String.format("jdbc:postgresql://%s:%s/postgres?sslmode=require&sslfactory=org.postgresql.ssl.NonValidatingFactory",
            env("PGHOST", "duckgres"), env("PGPORT", "5432"));
    }

    private static Connection connect() throws SQLException {
        return DriverManager.getConnection(jdbcUrl(), env("PGUSER", "postgres"), env("PGPASSWORD", "postgres"));
    }

    private static void waitForDuckgres() throws Exception {
        System.out.println("Waiting for duckgres...");
        for (int i = 0; i < 30; i++) {
            try (Connection conn = connect()) {
                System.out.printf("Connected after %d attempt(s).%n", i + 1);
                return;
            } catch (SQLException e) {
                Thread.sleep(1000);
            }
        }
        throw new RuntimeException("Could not connect after 30 seconds");
    }

    // ── Query loading ──────────────────────────────────────────

    @SuppressWarnings("unchecked")
    private static List<Map<String, Object>> loadQueries(String path) throws Exception {
        String content = Files.readString(Path.of(path));
        Yaml yaml = new Yaml(new SafeConstructor(new LoaderOptions()));
        return yaml.load(content);
    }

    // ── Test suites ────────────────────────────────────────────

    private void testSharedQueries() {
        System.out.println("\n=== Shared catalog queries ===");

        List<Map<String, Object>> queries;
        try {
            queries = loadQueries("/queries.yaml");
        } catch (Exception e) {
            report("shared", "load_queries", "fail", e.getMessage());
            return;
        }

        try (Connection conn = connect()) {
            // Set a statement timeout to detect hangs (10s per query)
            for (Map<String, Object> q : queries) {
                String suite = (String) q.get("suite");
                String name = (String) q.get("name");
                String sql = (String) q.get("sql");

                try (Statement stmt = conn.createStatement()) {
                    stmt.setQueryTimeout(10);
                    boolean hasResults = stmt.execute(sql);
                    int count = 0;
                    if (hasResults) {
                        try (ResultSet rs = stmt.getResultSet()) {
                            while (rs.next()) {
                                count++;
                            }
                        }
                    }
                    report(suite, name, "pass", count + " rows");
                } catch (Exception e) {
                    report(suite, name, "fail", e.getMessage());
                }
            }
        } catch (Exception e) {
            report("shared", "connect", "fail", e.getMessage());
        }
    }

    private void testConnectionProperties() {
        System.out.println("\n=== Connection properties ===");
        String suite = "connection";

        try (Connection conn = connect()) {
            report(suite, "connect", "pass", "TLS + password auth");

            DatabaseMetaData md = conn.getMetaData();

            // Server version
            String ver = md.getDatabaseProductVersion();
            if (ver != null && !ver.isEmpty()) {
                report(suite, "server_version", "pass", ver);
            } else {
                report(suite, "server_version", "fail", "empty");
            }

            // Product name
            String product = md.getDatabaseProductName();
            report(suite, "product_name", "pass", product);

            // Driver info
            String driverName = md.getDriverName();
            String driverVer = md.getDriverVersion();
            report(suite, "driver_info", "pass", driverName + " " + driverVer);

        } catch (Exception e) {
            report(suite, "connect", "fail", e.getMessage());
        }
    }

    /**
     * Test queries that Metabase fires on connect. These are the ones
     * observed to hang sporadically.
     */
    private void testMetabaseQueries() {
        System.out.println("\n=== Metabase-style queries ===");
        String suite = "metabase";

        // Metabase connection probing queries - test with both simple and prepared statement protocols
        String[] probeQueries = {
            "SELECT version()",
            "SELECT current_setting('server_version_num')",
            "SELECT current_setting('server_version')",
            "SELECT 1",
            "SHOW server_version",
        };

        // Test each query via simple query protocol (Statement)
        try (Connection conn = connect()) {
            for (String sql : probeQueries) {
                String testName = "simple_" + sql.toLowerCase()
                    .replaceAll("[^a-z0-9_]+", "_")
                    .replaceAll("^_|_$", "");
                try (Statement stmt = conn.createStatement()) {
                    stmt.setQueryTimeout(5);
                    try (ResultSet rs = stmt.executeQuery(sql)) {
                        if (rs.next()) {
                            String val = rs.getString(1);
                            report(suite, testName, "pass", val);
                        } else {
                            report(suite, testName, "fail", "no rows");
                        }
                    }
                } catch (Exception e) {
                    report(suite, testName, "fail", e.getMessage());
                }
            }
        } catch (Exception e) {
            report(suite, "simple_connect", "fail", e.getMessage());
        }

        // Test each query via extended query protocol (PreparedStatement)
        try (Connection conn = connect()) {
            for (String sql : probeQueries) {
                String testName = "prepared_" + sql.toLowerCase()
                    .replaceAll("[^a-z0-9_]+", "_")
                    .replaceAll("^_|_$", "");

                // SHOW doesn't work as prepared statement, skip
                if (sql.toUpperCase().startsWith("SHOW")) {
                    continue;
                }

                try (PreparedStatement pstmt = conn.prepareStatement(sql)) {
                    pstmt.setQueryTimeout(5);
                    try (ResultSet rs = pstmt.executeQuery()) {
                        if (rs.next()) {
                            String val = rs.getString(1);
                            report(suite, testName, "pass", val);
                        } else {
                            report(suite, testName, "fail", "no rows");
                        }
                    }
                } catch (Exception e) {
                    report(suite, testName, "fail", e.getMessage());
                }
            }
        } catch (Exception e) {
            report(suite, "prepared_connect", "fail", e.getMessage());
        }

        // Test rapid repeated version() queries (simulates Metabase connection pool checks)
        try (Connection conn = connect()) {
            int successCount = 0;
            for (int i = 0; i < 20; i++) {
                try (PreparedStatement pstmt = conn.prepareStatement("SELECT version()")) {
                    pstmt.setQueryTimeout(5);
                    try (ResultSet rs = pstmt.executeQuery()) {
                        if (rs.next()) {
                            successCount++;
                        }
                    }
                }
            }
            if (successCount == 20) {
                report(suite, "rapid_version_20x", "pass", "all 20 succeeded");
            } else {
                report(suite, "rapid_version_20x", "fail", successCount + "/20 succeeded");
            }
        } catch (Exception e) {
            report(suite, "rapid_version_20x", "fail", e.getMessage());
        }

        // Test named prepared statement reuse (the exact pattern that breaks without the fix).
        // JDBC's prepareThreshold (default=5) causes the driver to switch from unnamed to
        // named server-side prepared statements after N executions of the same query.
        // After the switch, the driver sends Bind/Execute without re-Describing, reusing
        // the named statement. If the server sends an unexpected RowDescription during
        // Execute, the driver's message queue desyncs (NoSuchElementException).
        testNamedPreparedStatementReuse(suite);

        // Test concurrent version() queries across connections (simulates connection pool)
        testConcurrentVersionQueries(suite);
    }

    private void testNamedPreparedStatementReuse(String suite) {
        // Test with prepareThreshold=1 to trigger named statements immediately
        String url = jdbcUrl() + "&prepareThreshold=1";
        try (Connection conn = DriverManager.getConnection(url, env("PGUSER", "postgres"), env("PGPASSWORD", "postgres"))) {
            int successCount = 0;
            for (int i = 0; i < 10; i++) {
                try (PreparedStatement pstmt = conn.prepareStatement("SELECT version()")) {
                    pstmt.setQueryTimeout(5);
                    try (ResultSet rs = pstmt.executeQuery()) {
                        if (rs.next()) {
                            successCount++;
                        }
                    }
                }
            }
            if (successCount == 10) {
                report(suite, "named_prepared_reuse_10x", "pass", "all 10 succeeded (prepareThreshold=1)");
            } else {
                report(suite, "named_prepared_reuse_10x", "fail", successCount + "/10 succeeded");
            }
        } catch (Exception e) {
            report(suite, "named_prepared_reuse_10x", "fail", e.getMessage());
        }
    }

    private void testConcurrentVersionQueries(String suite) {
        int numThreads = 5;
        int queriesPerThread = 10;
        int[] successes = new int[numThreads];
        String[] errors = new String[numThreads];
        Thread[] threads = new Thread[numThreads];

        for (int t = 0; t < numThreads; t++) {
            final int threadIdx = t;
            threads[t] = new Thread(() -> {
                try (Connection conn = connect()) {
                    for (int i = 0; i < queriesPerThread; i++) {
                        try (PreparedStatement pstmt = conn.prepareStatement("SELECT version()")) {
                            pstmt.setQueryTimeout(5);
                            try (ResultSet rs = pstmt.executeQuery()) {
                                if (rs.next()) {
                                    successes[threadIdx]++;
                                }
                            }
                        }
                    }
                } catch (Exception e) {
                    errors[threadIdx] = e.getMessage();
                }
            });
        }

        for (Thread t : threads) t.start();
        for (Thread t : threads) {
            try { t.join(30_000); } catch (InterruptedException e) { Thread.currentThread().interrupt(); }
        }

        int totalSuccess = 0;
        List<String> errorList = new ArrayList<>();
        for (int t = 0; t < numThreads; t++) {
            totalSuccess += successes[t];
            if (errors[t] != null) errorList.add("thread" + t + ": " + errors[t]);
        }
        int expected = numThreads * queriesPerThread;
        if (totalSuccess == expected) {
            report(suite, "concurrent_version_" + numThreads + "t", "pass",
                totalSuccess + "/" + expected + " across " + numThreads + " connections");
        } else {
            report(suite, "concurrent_version_" + numThreads + "t", "fail",
                totalSuccess + "/" + expected + " succeeded; " + String.join("; ", errorList));
        }
    }

    private void testDatabaseMetaData() {
        System.out.println("\n=== DatabaseMetaData ===");
        String suite = "metadata";

        try (Connection conn = connect()) {
            DatabaseMetaData md = conn.getMetaData();

            // getTables
            try (ResultSet rs = md.getTables(null, null, "%", new String[]{"TABLE"})) {
                int count = 0;
                while (rs.next()) count++;
                report(suite, "getTables", "pass", count + " tables");
            } catch (Exception e) {
                report(suite, "getTables", "fail", e.getMessage());
            }

            // getSchemas
            try (ResultSet rs = md.getSchemas()) {
                int count = 0;
                while (rs.next()) count++;
                report(suite, "getSchemas", "pass", count + " schemas");
            } catch (Exception e) {
                report(suite, "getSchemas", "fail", e.getMessage());
            }

            // getCatalogs
            try (ResultSet rs = md.getCatalogs()) {
                int count = 0;
                while (rs.next()) count++;
                report(suite, "getCatalogs", "pass", count + " catalogs");
            } catch (Exception e) {
                report(suite, "getCatalogs", "fail", e.getMessage());
            }

            // getTypeInfo
            try (ResultSet rs = md.getTypeInfo()) {
                int count = 0;
                while (rs.next()) count++;
                report(suite, "getTypeInfo", "pass", count + " types");
            } catch (Exception e) {
                report(suite, "getTypeInfo", "fail", e.getMessage());
            }

            // getColumns (against pg_class which should exist)
            try (ResultSet rs = md.getColumns(null, null, "pg_class", "%")) {
                int count = 0;
                while (rs.next()) count++;
                report(suite, "getColumns_pg_class", "pass", count + " columns");
            } catch (Exception e) {
                report(suite, "getColumns_pg_class", "fail", e.getMessage());
            }

        } catch (Exception e) {
            report(suite, "connect", "fail", e.getMessage());
        }
    }

    private void testDDLDML() {
        System.out.println("\n=== DDL and DML ===");
        String suite = "ddl_dml";

        try (Connection conn = connect()) {
            Statement stmt = conn.createStatement();
            stmt.setQueryTimeout(10);

            // DROP IF EXISTS
            stmt.execute("DROP TABLE IF EXISTS jdbc_test");

            // CREATE TABLE
            stmt.execute("CREATE TABLE jdbc_test (id INTEGER, name VARCHAR, value DOUBLE, ts TIMESTAMP, flag BOOLEAN)");
            report(suite, "CREATE TABLE", "pass", "");

            // INSERT parameterized
            try (PreparedStatement pstmt = conn.prepareStatement(
                    "INSERT INTO jdbc_test VALUES (?, ?, ?, ?, ?)")) {
                pstmt.setInt(1, 1);
                pstmt.setString(2, "alice");
                pstmt.setDouble(3, 3.14);
                pstmt.setTimestamp(4, Timestamp.valueOf("2024-01-01 10:00:00"));
                pstmt.setBoolean(5, true);
                pstmt.executeUpdate();
                report(suite, "INSERT parameterized", "pass", "");
            } catch (Exception e) {
                report(suite, "INSERT parameterized", "fail", e.getMessage());
            }

            // INSERT literal
            stmt.execute("INSERT INTO jdbc_test VALUES (2, 'bob', 2.72, '2024-01-02 11:00:00', false)");
            report(suite, "INSERT literal", "pass", "");

            // SELECT *
            try (ResultSet rs = stmt.executeQuery("SELECT * FROM jdbc_test ORDER BY id")) {
                int count = 0;
                while (rs.next()) count++;
                if (count == 2) {
                    report(suite, "SELECT *", "pass", count + " rows");
                } else {
                    report(suite, "SELECT *", "fail", "expected 2, got " + count);
                }
            }

            // SELECT WHERE parameterized
            try (PreparedStatement pstmt = conn.prepareStatement(
                    "SELECT name FROM jdbc_test WHERE value > ?")) {
                pstmt.setDouble(1, 3.0);
                try (ResultSet rs = pstmt.executeQuery()) {
                    if (rs.next()) {
                        String name = rs.getString(1);
                        if ("alice".equals(name)) {
                            report(suite, "SELECT WHERE parameterized", "pass", "");
                        } else {
                            report(suite, "SELECT WHERE parameterized", "fail", "expected alice, got " + name);
                        }
                    } else {
                        report(suite, "SELECT WHERE parameterized", "fail", "no rows");
                    }
                }
            }

            // UPDATE
            try (PreparedStatement pstmt = conn.prepareStatement(
                    "UPDATE jdbc_test SET value = ? WHERE id = ?")) {
                pstmt.setDouble(1, 9.99);
                pstmt.setInt(2, 1);
                pstmt.executeUpdate();
                report(suite, "UPDATE parameterized", "pass", "");
            } catch (Exception e) {
                report(suite, "UPDATE parameterized", "fail", e.getMessage());
            }

            // DELETE
            try (PreparedStatement pstmt = conn.prepareStatement(
                    "DELETE FROM jdbc_test WHERE id = ?")) {
                pstmt.setInt(1, 2);
                pstmt.executeUpdate();
                report(suite, "DELETE parameterized", "pass", "");
            } catch (Exception e) {
                report(suite, "DELETE parameterized", "fail", e.getMessage());
            }

            // Verify count
            try (ResultSet rs = stmt.executeQuery("SELECT count(*) FROM jdbc_test")) {
                rs.next();
                int count = rs.getInt(1);
                if (count == 1) {
                    report(suite, "post-DML count", "pass", "1 row remaining");
                } else {
                    report(suite, "post-DML count", "fail", "expected 1, got " + count);
                }
            }

            // DROP
            stmt.execute("DROP TABLE jdbc_test");
            report(suite, "DROP TABLE", "pass", "");

            stmt.close();
        } catch (Exception e) {
            report(suite, "connect", "fail", e.getMessage());
        }
    }

    private void testPreparedStatementReuse() {
        System.out.println("\n=== Prepared statement reuse ===");
        String suite = "prepared";

        try (Connection conn = connect()) {
            Statement stmt = conn.createStatement();
            stmt.execute("DROP TABLE IF EXISTS jdbc_prep_test");
            stmt.execute("CREATE TABLE jdbc_prep_test (id INTEGER, val VARCHAR)");

            // Re-execute same prepared statement multiple times
            try (PreparedStatement pstmt = conn.prepareStatement(
                    "INSERT INTO jdbc_prep_test VALUES (?, ?)")) {
                for (int i = 0; i < 10; i++) {
                    pstmt.setInt(1, i);
                    pstmt.setString(2, "val_" + i);
                    pstmt.executeUpdate();
                }
                report(suite, "insert_reuse_10x", "pass", "10 inserts");
            } catch (Exception e) {
                report(suite, "insert_reuse_10x", "fail", e.getMessage());
            }

            // Re-execute same SELECT prepared statement
            try (PreparedStatement pstmt = conn.prepareStatement(
                    "SELECT val FROM jdbc_prep_test WHERE id = ?")) {
                for (int i = 0; i < 10; i++) {
                    pstmt.setInt(1, i);
                    try (ResultSet rs = pstmt.executeQuery()) {
                        if (rs.next()) {
                            String expected = "val_" + i;
                            String actual = rs.getString(1);
                            if (!expected.equals(actual)) {
                                report(suite, "select_reuse", "fail",
                                    "iteration " + i + ": expected " + expected + ", got " + actual);
                                break;
                            }
                        } else {
                            report(suite, "select_reuse", "fail", "no rows for id=" + i);
                            break;
                        }
                    }
                }
                report(suite, "select_reuse_10x", "pass", "10 selects");
            } catch (Exception e) {
                report(suite, "select_reuse_10x", "fail", e.getMessage());
            }

            stmt.execute("DROP TABLE jdbc_prep_test");
            stmt.close();
        } catch (Exception e) {
            report(suite, "connect", "fail", e.getMessage());
        }
    }

    private void testBatchInserts() {
        System.out.println("\n=== Batch inserts ===");
        String suite = "batch";

        try (Connection conn = connect()) {
            Statement stmt = conn.createStatement();
            stmt.execute("DROP TABLE IF EXISTS jdbc_batch_test");
            stmt.execute("CREATE TABLE jdbc_batch_test (id INTEGER, val VARCHAR)");

            // Batch via PreparedStatement
            try (PreparedStatement pstmt = conn.prepareStatement(
                    "INSERT INTO jdbc_batch_test VALUES (?, ?)")) {
                for (int i = 0; i < 50; i++) {
                    pstmt.setInt(1, i);
                    pstmt.setString(2, "batch_" + i);
                    pstmt.addBatch();
                }
                int[] results = pstmt.executeBatch();
                report(suite, "batch_insert_50", "pass", results.length + " statements");
            } catch (Exception e) {
                report(suite, "batch_insert_50", "fail", e.getMessage());
            }

            // Verify
            try (ResultSet rs = stmt.executeQuery("SELECT count(*) FROM jdbc_batch_test")) {
                rs.next();
                int count = rs.getInt(1);
                if (count == 50) {
                    report(suite, "batch_verify", "pass", count + " rows");
                } else {
                    report(suite, "batch_verify", "fail", "expected 50, got " + count);
                }
            }

            stmt.execute("DROP TABLE jdbc_batch_test");
            stmt.close();
        } catch (Exception e) {
            report(suite, "connect", "fail", e.getMessage());
        }
    }

    private void testResultSetMetaData() {
        System.out.println("\n=== ResultSet metadata ===");
        String suite = "rs_metadata";

        try (Connection conn = connect()) {
            Statement stmt = conn.createStatement();
            stmt.execute("DROP TABLE IF EXISTS jdbc_meta_test");
            stmt.execute("CREATE TABLE jdbc_meta_test (id INTEGER, name VARCHAR, price DOUBLE, active BOOLEAN)");
            stmt.execute("INSERT INTO jdbc_meta_test VALUES (1, 'test', 9.99, true)");

            try (ResultSet rs = stmt.executeQuery("SELECT * FROM jdbc_meta_test")) {
                ResultSetMetaData rsmd = rs.getMetaData();

                int colCount = rsmd.getColumnCount();
                if (colCount == 4) {
                    report(suite, "column_count", "pass", String.valueOf(colCount));
                } else {
                    report(suite, "column_count", "fail", "expected 4, got " + colCount);
                }

                // Column names
                StringBuilder names = new StringBuilder();
                for (int i = 1; i <= colCount; i++) {
                    if (i > 1) names.append(",");
                    names.append(rsmd.getColumnName(i));
                }
                report(suite, "column_names", "pass", names.toString());

                // Column types
                StringBuilder types = new StringBuilder();
                for (int i = 1; i <= colCount; i++) {
                    if (i > 1) types.append(",");
                    types.append(rsmd.getColumnTypeName(i));
                }
                report(suite, "column_types", "pass", types.toString());
            }

            stmt.execute("DROP TABLE jdbc_meta_test");
            stmt.close();
        } catch (Exception e) {
            report(suite, "connect", "fail", e.getMessage());
        }
    }

    // ── Main ───────────────────────────────────────────────────

    public static void main(String[] args) {
        try {
            waitForDuckgres();
        } catch (Exception e) {
            System.out.println("FAIL: " + e.getMessage());
            System.exit(1);
        }

        JdbcCompatTest test = new JdbcCompatTest();

        test.testConnectionProperties();
        test.testMetabaseQueries();
        test.testSharedQueries();
        test.testDatabaseMetaData();
        test.testDDLDML();
        test.testPreparedStatementReuse();
        test.testBatchInserts();
        test.testResultSetMetaData();

        System.out.println();
        System.out.println("==================================================");
        System.out.printf("Results: %d passed, %d failed%n", test.passed, test.failed);
        System.out.println("==================================================");

        test.done();

        if (test.failed > 0) {
            System.exit(1);
        }
    }
}
