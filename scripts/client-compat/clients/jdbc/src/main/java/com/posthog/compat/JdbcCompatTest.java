package com.posthog.compat;

import org.yaml.snakeyaml.Yaml;

import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.Map;

public final class JdbcCompatTest {
    private static final String CLIENT_NAME = "jdbc";
    private static final String RESULTS_URL = env("RESULTS_URL", "http://results-gatherer:8080");
    private static final int QUERY_TIMEOUT_SECONDS = 10;

    private int passed;
    private int failed;
    private String currentSuite = "";

    public static void main(String[] args) {
        JdbcCompatTest test = new JdbcCompatTest();

        if (!test.waitForDuckgres()) {
            System.out.println("FAIL: Could not connect after 30 seconds");
            System.exit(1);
        }

        test.testConnection();
        test.testSharedQueries();
        test.testDdlDml();
        test.testMetadata();
        test.testBatch();
        test.testResultSetMetadata();
        test.testMetabaseSmokeQueries();

        System.out.println("\n==================================================");
        System.out.printf(Locale.ROOT, "Results: %d passed, %d failed%n", test.passed, test.failed);
        System.out.println("==================================================");

        test.done();
        if (test.failed > 0) {
            System.exit(1);
        }
    }

    private static String env(String key, String fallback) {
        String value = System.getenv(key);
        return value == null || value.isBlank() ? fallback : value;
    }

    private Connection getConnection() throws SQLException {
        String host = env("PGHOST", "duckgres");
        String port = env("PGPORT", "5432");
        String user = env("PGUSER", "postgres");
        String password = env("PGPASSWORD", "postgres");
        String url = String.format(
            Locale.ROOT,
            "jdbc:postgresql://%s:%s/postgres?sslmode=require&sslfactory=org.postgresql.ssl.NonValidatingFactory&connectTimeout=5&socketTimeout=%d",
            host,
            port,
            QUERY_TIMEOUT_SECONDS
        );
        return DriverManager.getConnection(url, user, password);
    }

    private boolean waitForDuckgres() {
        System.out.println("Waiting for duckgres...");
        for (int attempt = 1; attempt <= 30; attempt++) {
            try (Connection ignored = getConnection()) {
                System.out.printf(Locale.ROOT, "Connected after %d attempt(s).%n", attempt);
                return true;
            } catch (SQLException ignored) {
                sleepOneSecond();
            }
        }
        return false;
    }

    private void testConnection() {
        System.out.println("\n=== Connection properties ===");
        currentSuite = "connection";

        try (Connection conn = getConnection()) {
            ok("connect", "TLS + password auth");

            try (Statement st = conn.createStatement(); ResultSet rs = st.executeQuery("SELECT current_database()")) {
                if (rs.next()) {
                    ok("current_database", rs.getString(1));
                } else {
                    fail("current_database", "no rows");
                }
            }

            try (Statement st = conn.createStatement(); ResultSet rs = st.executeQuery("SHOW server_version")) {
                if (rs.next()) {
                    ok("server_version", rs.getString(1));
                } else {
                    fail("server_version", "no rows");
                }
            }
        } catch (SQLException e) {
            fail("connect", e.getMessage());
        }
    }

    private void testSharedQueries() {
        System.out.println("\n=== Shared catalog queries ===");

        List<Map<String, Object>> queries;
        try {
            queries = loadQueries("/queries.yaml");
        } catch (IOException e) {
            currentSuite = "shared";
            fail("load_queries", e.getMessage());
            return;
        }

        try (Connection conn = getConnection()) {
            for (Map<String, Object> q : queries) {
                String suite = String.valueOf(q.get("suite"));
                String name = String.valueOf(q.get("name"));
                String sql = String.valueOf(q.get("sql"));
                currentSuite = suite;

                try (Statement st = conn.createStatement()) {
                    st.setQueryTimeout(QUERY_TIMEOUT_SECONDS);
                    int rows = executeAndCountRows(st, sql);
                    ok(name, rows + " rows");
                } catch (SQLException e) {
                    fail(name, e.getMessage());
                }
            }
        } catch (SQLException e) {
            currentSuite = "shared";
            fail("connect", e.getMessage());
        }
    }

    private void testDdlDml() {
        System.out.println("\n=== DDL and DML ===");
        currentSuite = "ddl_dml";

        try (Connection conn = getConnection(); Statement st = conn.createStatement()) {
            st.setQueryTimeout(QUERY_TIMEOUT_SECONDS);
            st.execute("DROP TABLE IF EXISTS jdbc_test");
            st.execute("CREATE TABLE jdbc_test (id INTEGER, name VARCHAR, value DOUBLE, ts TIMESTAMP, flag BOOLEAN)");
            ok("CREATE TABLE", "");

            try (PreparedStatement ps = conn.prepareStatement("INSERT INTO jdbc_test VALUES (?, ?, ?, ?, ?)")) {
                ps.setInt(1, 1);
                ps.setString(2, "alice");
                ps.setDouble(3, 3.14);
                ps.setString(4, "2024-01-01 10:00:00");
                ps.setBoolean(5, true);
                ps.executeUpdate();
            }
            ok("INSERT parameterized", "");

            st.execute("INSERT INTO jdbc_test VALUES (2, 'bob', 2.72, '2024-01-02 11:00:00', false)");
            ok("INSERT literal", "");

            int rowCount = executeAndCountRows(st, "SELECT * FROM jdbc_test ORDER BY id");
            if (rowCount == 2) {
                ok("SELECT *", "2 rows");
            } else {
                fail("SELECT *", "expected 2 rows, got " + rowCount);
            }

            try (PreparedStatement ps = conn.prepareStatement("SELECT name FROM jdbc_test WHERE value > ?")) {
                ps.setDouble(1, 3.0);
                try (ResultSet rs = ps.executeQuery()) {
                    if (rs.next() && "alice".equals(rs.getString(1))) {
                        ok("SELECT WHERE parameterized", "");
                    } else {
                        fail("SELECT WHERE parameterized", "unexpected result");
                    }
                }
            }

            try (PreparedStatement ps = conn.prepareStatement("UPDATE jdbc_test SET value = ? WHERE id = ?")) {
                ps.setDouble(1, 9.99);
                ps.setInt(2, 1);
                ps.executeUpdate();
            }
            ok("UPDATE parameterized", "");

            try (PreparedStatement ps = conn.prepareStatement("DELETE FROM jdbc_test WHERE id = ?")) {
                ps.setInt(1, 2);
                ps.executeUpdate();
            }
            ok("DELETE parameterized", "");

            try (ResultSet rs = st.executeQuery("SELECT count(*) FROM jdbc_test")) {
                if (rs.next() && rs.getInt(1) == 1) {
                    ok("post-DML count", "1 row remaining");
                } else {
                    fail("post-DML count", "expected 1 row remaining");
                }
            }

            st.execute("DROP TABLE jdbc_test");
            ok("DROP TABLE", "");
        } catch (SQLException e) {
            fail("ddl_dml", e.getMessage());
        }
    }

    private void testMetadata() {
        System.out.println("\n=== Database metadata ===");
        currentSuite = "metadata";

        try (Connection conn = getConnection()) {
            DatabaseMetaData md = conn.getMetaData();

            if (md.getDatabaseProductName() != null && !md.getDatabaseProductName().isBlank()) {
                ok("database_product_name", md.getDatabaseProductName());
            } else {
                fail("database_product_name", "empty");
            }

            try (ResultSet rs = md.getSchemas()) {
                int rows = 0;
                while (rs.next()) {
                    rows++;
                }
                ok("getSchemas", rows + " rows");
            }

            try (ResultSet rs = md.getTables(null, null, "%", new String[]{"TABLE", "VIEW"})) {
                int rows = 0;
                while (rs.next()) {
                    rows++;
                }
                ok("getTables", rows + " rows");
            }
        } catch (SQLException e) {
            fail("metadata", e.getMessage());
        }
    }

    private void testBatch() {
        System.out.println("\n=== Batch operations ===");
        currentSuite = "batch";

        try (Connection conn = getConnection(); Statement st = conn.createStatement()) {
            st.setQueryTimeout(QUERY_TIMEOUT_SECONDS);
            st.execute("DROP TABLE IF EXISTS jdbc_batch_test");
            st.execute("CREATE TABLE jdbc_batch_test (id INTEGER, label VARCHAR)");

            try (PreparedStatement ps = conn.prepareStatement("INSERT INTO jdbc_batch_test VALUES (?, ?)")) {
                for (int i = 0; i < 10; i++) {
                    ps.setInt(1, i);
                    ps.setString(2, "item_" + i);
                    ps.addBatch();
                }
                int[] counts = ps.executeBatch();
                ok("executeBatch", counts.length + " statements");
            }

            try (ResultSet rs = st.executeQuery("SELECT count(*) FROM jdbc_batch_test")) {
                if (rs.next() && rs.getInt(1) == 10) {
                    ok("batch_count", "10 rows");
                } else {
                    fail("batch_count", "expected 10 rows");
                }
            }

            st.execute("DROP TABLE jdbc_batch_test");
            ok("batch_cleanup", "");
        } catch (SQLException e) {
            fail("batch", e.getMessage());
        }
    }

    private void testResultSetMetadata() {
        System.out.println("\n=== ResultSet metadata ===");
        currentSuite = "resultset_metadata";

        try (Connection conn = getConnection(); Statement st = conn.createStatement()) {
            st.setQueryTimeout(QUERY_TIMEOUT_SECONDS);
            try (ResultSet rs = st.executeQuery("SELECT 1::integer AS int_col, 'hello'::varchar AS str_col, 3.14::double AS dbl_col")) {
                ResultSetMetaData md = rs.getMetaData();
                if (md.getColumnCount() == 3) {
                    ok("field_count", "3");
                } else {
                    fail("field_count", "expected 3, got " + md.getColumnCount());
                }

                List<String> names = new ArrayList<>();
                for (int i = 1; i <= md.getColumnCount(); i++) {
                    names.add(md.getColumnLabel(i));
                }
                if (names.equals(List.of("int_col", "str_col", "dbl_col"))) {
                    ok("field_names", String.join(", ", names));
                } else {
                    fail("field_names", String.join(", ", names));
                }
            }
        } catch (SQLException e) {
            fail("resultset_metadata", e.getMessage());
        }
    }

    private void testMetabaseSmokeQueries() {
        System.out.println("\n=== Metabase smoke queries ===");
        currentSuite = "metabase_smoke";

        runRepeatedScalarQuery("SELECT version()", "version_repeat_50", 50);
        runRepeatedScalarQuery("SELECT worker_version()", "worker_version_repeat_50", 50);
    }

    private void runRepeatedScalarQuery(String sql, String testName, int iterations) {
        try (Connection conn = getConnection(); Statement st = conn.createStatement()) {
            st.setQueryTimeout(QUERY_TIMEOUT_SECONDS);
            for (int i = 0; i < iterations; i++) {
                try (ResultSet rs = st.executeQuery(sql)) {
                    if (!rs.next()) {
                        fail(testName, "iteration " + (i + 1) + " returned no rows");
                        return;
                    }
                    rs.getString(1);
                }
            }
            ok(testName, iterations + " iterations");
        } catch (SQLException e) {
            fail(testName, e.getMessage());
        }
    }

    private static List<Map<String, Object>> loadQueries(String path) throws IOException {
        Yaml yaml = new Yaml();
        try (InputStream in = Files.newInputStream(Path.of(path))) {
            Object loaded = yaml.load(in);
            if (!(loaded instanceof List<?> list)) {
                throw new IOException("queries file is not a list");
            }

            List<Map<String, Object>> out = new ArrayList<>();
            for (Object item : list) {
                if (item instanceof Map<?, ?> raw) {
                    @SuppressWarnings("unchecked")
                    Map<String, Object> typed = (Map<String, Object>) raw;
                    out.add(typed);
                }
            }
            return out;
        }
    }

    private static int executeAndCountRows(Statement st, String sql) throws SQLException {
        try (ResultSet rs = st.executeQuery(sql)) {
            int count = 0;
            while (rs.next()) {
                count++;
            }
            return count;
        }
    }

    private void ok(String name, String detail) {
        passed++;
        String suffix = detail == null || detail.isBlank() ? "" : " (" + detail + ")";
        System.out.println("  PASS  " + name + suffix);
        report(currentSuite, name, "pass", detail == null ? "" : detail);
    }

    private void fail(String name, String reason) {
        failed++;
        String detail = reason == null ? "" : reason;
        System.out.println("  FAIL  " + name + ": " + detail);
        report(currentSuite, name, "fail", detail);
    }

    private void report(String suite, String testName, String status, String detail) {
        postJson("/result", String.format(
            Locale.ROOT,
            "{\"client\":\"%s\",\"suite\":\"%s\",\"test_name\":\"%s\",\"status\":\"%s\",\"detail\":\"%s\"}",
            escapeJson(CLIENT_NAME),
            escapeJson(suite),
            escapeJson(testName),
            escapeJson(status),
            escapeJson(detail)
        ));
    }

    private void done() {
        postJson("/done", String.format(Locale.ROOT, "{\"client\":\"%s\"}", escapeJson(CLIENT_NAME)));
    }

    private static void postJson(String path, String body) {
        HttpClient client = HttpClient.newBuilder()
            .connectTimeout(Duration.ofSeconds(2))
            .build();

        HttpRequest request = HttpRequest.newBuilder()
            .uri(URI.create(RESULTS_URL + path))
            .timeout(Duration.ofSeconds(2))
            .header("Content-Type", "application/json")
            .POST(HttpRequest.BodyPublishers.ofString(body, StandardCharsets.UTF_8))
            .build();

        try {
            client.send(request, HttpResponse.BodyHandlers.discarding());
        } catch (IOException | InterruptedException ignored) {
        }
    }

    private static String escapeJson(String value) {
        if (value == null) {
            return "";
        }
        return value
            .replace("\\", "\\\\")
            .replace("\"", "\\\"")
            .replace("\n", "\\n")
            .replace("\r", "\\r")
            .replace("\t", "\\t");
    }

    private static void sleepOneSecond() {
        try {
            Thread.sleep(1000);
        } catch (InterruptedException ignored) {
            Thread.currentThread().interrupt();
        }
    }
}
