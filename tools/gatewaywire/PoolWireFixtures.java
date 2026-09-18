import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import io.trino.gateway.ha.transaction.PoolStore;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import java.util.UUID;

/**
 * Serializes the Gateway's REAL pooled-lifecycle records with Jackson and writes the resulting
 * wire JSON to disk. The duckgres Go client consumes these files as testdata, so its decoding is
 * pinned to what the Java side actually produces rather than to a hand-written copy of a contract
 * document. Every identifier here is synthetic.
 */
public final class PoolWireFixtures
{
    private PoolWireFixtures() {}

    public static void main(String[] args)
            throws Exception
    {
        Path out = Path.of(args[0]);
        Files.createDirectories(out);
        ObjectMapper mapper = new ObjectMapper().enable(SerializationFeature.INDENT_OUTPUT);

        UUID incarnation = UUID.fromString("11111111-1111-4111-8111-111111111111");

        PoolStore.PoolState pool = new PoolStore.PoolState(
                1, "pool-001", "POOLED", 7L, 19L,
                3, 3, 1, 1,
                "r-42", "r-41", true,
                Map.of("PREPARING", 1L, "ACTIVE", 3L, "RETIRED", 4L, "LOST", 1L),
                3L, 4L, 1L, 0L, 0L, List.of(), false);
        write(mapper, out.resolve("pool_state.json"), pool);

        PoolStore.Member member = new PoolStore.Member(
                1, "pool-001", "i-0007", incarnation, "pool-001-i-0007",
                "https://i-0007.pool.invalid:8443", "https://i-0007.external.invalid:8443",
                "ACTIVE", 4L, 7L,
                "pod-uid-0007", "boot-0007", "node-0007", "abcde",
                "r-42", "r-42", "auth-9", false, null, null,
                0L, 0L, 0L, false, false, true, 19L, false);
        write(mapper, out.resolve("member.json"), member);
        write(mapper, out.resolve("members.json"), List.of(member));

        PoolStore.Obligations obligations = new PoolStore.Obligations(
                1, "i-0007", incarnation, "DRAINING", 5L, 2L, 1L, 3L, false, false);
        write(mapper, out.resolve("obligations.json"), obligations);

        PoolStore.Publication publication = new PoolStore.Publication(
                1, "pub-1", "pool-001", "tenant-a", "r-42", 19L, "OPEN",
                List.of("i-0007"),
                List.of(new PoolStore.PublicationReceipt("i-0007", incarnation, "pod-uid-0007", "boot-0007", "r-42", "fingerprint-1")),
                List.of(), "PENDING", null, false);
        write(mapper, out.resolve("publication.json"), publication);

        PoolStore.TenantAdmission tenant = new PoolStore.TenantAdmission(
                1, "pool-001", "tenant-a", "ADMITTED", "r-42", "pub-1", false);
        write(mapper, out.resolve("tenant_admission.json"), tenant);

        PoolStore.FailureReceipt failure = new PoolStore.FailureReceipt(
                1, "pool-001", "i-0007", incarnation, "PROCESS_TERMINATED",
                mapper.readTree("{\"source\":\"kubernetes-pod-absent\"}"),
                0L, 1L, 2L, "2026-09-18T00:00:00Z");
        write(mapper, out.resolve("failure_receipt.json"), failure);

        PoolStore.OperationHistory history = new PoolStore.OperationHistory(
                1, "op-1",
                List.of(new PoolStore.OperationStep(
                        "admit", "0".repeat(64), 7L, "OK", "2026-09-18T00:00:00Z",
                        mapper.readTree("{\"phase\":\"ACTIVE\"}"))));
        write(mapper, out.resolve("operation_history.json"), history);

        System.out.println("wrote fixtures to " + out.toAbsolutePath());
    }

    private static void write(ObjectMapper mapper, Path path, Object value)
            throws Exception
    {
        Files.writeString(path, mapper.writeValueAsString(value) + "\n");
    }
}
