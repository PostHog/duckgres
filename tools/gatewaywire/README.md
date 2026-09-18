# Gateway wire fixtures

`controlplane/trinogateway/testdata/*.json` is not hand-written. Each file is
produced by serializing the Gateway's real `PoolStore` records with Jackson, so
the Go client's decoding is pinned to what the Java side actually emits rather
than to a copy of a design document. A hand-written fixture would only prove
that the test and the code under test agree with each other.

Regenerate after any change to the Java records:

```sh
GATEWAY=<path to the trino-gateway checkout>
JAVA_HOME=<a JDK 21+ home>
M2="$HOME/.m2/repository/com/fasterxml/jackson/core"

CP="$M2/jackson-databind/2.21.5/jackson-databind-2.21.5.jar:\
$M2/jackson-core/2.21.4/jackson-core-2.21.4.jar:\
$M2/jackson-annotations/2.21/jackson-annotations-2.21.jar:\
$GATEWAY/gateway-ha/target/classes"

"$JAVA_HOME/bin/javac" -cp "$CP" -d /tmp/gatewaywire tools/gatewaywire/PoolWireFixtures.java
"$JAVA_HOME/bin/java" -cp "/tmp/gatewaywire:$CP" PoolWireFixtures \
    controlplane/trinogateway/testdata
```

The Gateway checkout must be built first (`./mvnw -pl gateway-ha compile`) so
`target/classes` exists. Every identifier in the generated fixtures is
synthetic.

The decoder tests use `DisallowUnknownFields`, so a field added on the Java
side fails the Go build-out rather than being silently ignored — which is the
point: a consumer that quietly drops a new field keeps making decisions on a
stale view of the member.
