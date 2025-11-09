package org.novendu.consumer.leader;

import jakarta.annotation.PreDestroy;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import software.amazon.awssdk.services.dynamodb.model.AttributeValue;
import software.amazon.awssdk.services.dynamodb.model.ConditionalCheckFailedException;
import software.amazon.awssdk.services.dynamodb.model.ReturnValue;
import software.amazon.awssdk.services.dynamodb.model.UpdateItemRequest;

import java.net.InetAddress;
import java.time.Instant;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

@Service
public class LeaderElectionService {

    private static final Logger log = LoggerFactory.getLogger(LeaderElectionService.class);

    private enum LeadershipStatus { FOLLOWER, PENDING, LEADER }

    private final DynamoDbClient dynamo;
    private final String tableName;
    private final String electionId;
    private final int leaseDurationSec;
    private final int confirmationDelaySeconds;
    private final int heartbeatIntervalSeconds;
    private final String useCaseType;
    private final String nodeId;

    private final AtomicReference<LeadershipStatus> status = new AtomicReference<>(LeadershipStatus.FOLLOWER);
    private volatile String currentLeaderId = null;
    private volatile Long currentGeneration = 0L;

    private final ScheduledExecutorService scheduler = Executors.newSingleThreadScheduledExecutor();

    public LeaderElectionService(
            DynamoDbClient dynamo,
            @Value("${app.leader.table}") String tableName,
            @Value("${app.leader.electionId}") String electionId,
            @Value("${app.leader.leaseDurationSec:30}") int leaseDurationSec,
            @Value("${app.leader.confirmationDelaySeconds:5}") int confirmationDelaySeconds,
            @Value("${app.leader.heartbeatIntervalSeconds:10}") int heartbeatIntervalSeconds,
            @Value("${app.leader.useCaseType:FDAM_TALOS_WS}") String useCaseType
    ) {
        this.dynamo = dynamo;
        this.tableName = tableName;
        this.electionId = electionId;
        this.leaseDurationSec = leaseDurationSec;
        this.confirmationDelaySeconds = confirmationDelaySeconds;
        this.heartbeatIntervalSeconds = heartbeatIntervalSeconds;
        this.useCaseType = useCaseType;
        this.nodeId = hostname() + "-" + UUID.randomUUID().toString().substring(0, 8);

        log.info("NodeId for leader election: {}", nodeId);

        scheduler.scheduleWithFixedDelay(this::tryAcquireIfNotLeader, 3, 5, TimeUnit.SECONDS);
        scheduler.scheduleWithFixedDelay(this::heartbeat, 10, heartbeatIntervalSeconds, TimeUnit.SECONDS);
    }

    @PreDestroy
    public void shutdown() {
        scheduler.shutdown();
        releaseLeadership();
    }

    public boolean isLeader() {
        return status.get() == LeadershipStatus.LEADER;
    }

    public String getCurrentLeaderId() {
        return currentLeaderId;
    }

    public Long getCurrentGeneration() {
        return currentGeneration;
    }

    public Map<String, Object> getLeadershipStatus() {
        return Map.of(
                "isLeader", isLeader(),
                "nodeId", this.nodeId,
                "currentLeaderId", String.valueOf(getCurrentLeaderId()),
                "currentGeneration", String.valueOf(getCurrentGeneration()),
                "status", status.get().name()
        );
    }

    public void tryAcquireIfNotLeader() {
        if (status.get() != LeadershipStatus.FOLLOWER) return;
        tryAcquireLeadership();
    }

    public void heartbeat() {
        if (status.get() != LeadershipStatus.LEADER) return;

        long now = Instant.now().getEpochSecond();
        long newTtl = now + leaseDurationSec;

        Map<String, AttributeValue> key = Map.of("id", AttributeValue.fromS(electionId));
        Map<String, String> names = Map.of(
                "#lid", "leaderId",
                "#lu", "lastUpdatedEpochSec",
                "#ttl", "ttlEpochSec"
        );
        Map<String, AttributeValue> values = Map.of(
                ":me", AttributeValue.fromS(nodeId),
                ":now", AttributeValue.fromN(Long.toString(now)),
                ":ttl", AttributeValue.fromN(Long.toString(newTtl))
        );

        UpdateItemRequest req = UpdateItemRequest.builder()
                .tableName(tableName)
                .key(key)
                .updateExpression("SET #lu = :now, #ttl = :ttl")
                .conditionExpression("#lid = :me AND :now <= #ttl")
                .expressionAttributeNames(names)
                .expressionAttributeValues(values)
                .build();

        try {
            dynamo.updateItem(req);
            log.info("Heartbeat successful for LeaderId: {}", nodeId);
        } catch (ConditionalCheckFailedException e) {
            log.warn("Heartbeat failed; lost leadership for {}", nodeId);
            status.set(LeadershipStatus.FOLLOWER);
            readLeaderState();
        } catch (Exception e) {
            log.error("Heartbeat failed for {} with error: {}", nodeId, e.getMessage());
        }
    }

   /* private void tryAcquireLeadership() {
        long now = Instant.now().getEpochSecond();
        long ttl = now + leaseDurationSec;

        Map<String, String> names = Map.of(
                "#lid", "leaderId",
                "#lease", "leaseDurationSeconds",
                "#lu", "lastUpdatedEpochSec",
                "#ttl", "ttlEpochSec",
                "#gen", "generation",
                "#uct", "useCaseType"
        );

        Map<String, AttributeValue> values = Map.of(
                ":leaderId", AttributeValue.fromS(nodeId),
                ":lease", AttributeValue.fromN(Integer.toString(leaseDurationSec)),
                ":now",AttributeValue.fromN(Long.toString(now)),
                ":ttl", AttributeValue.fromN(Long.toString(ttl)),
                ":zero", AttributeValue.fromN("0"),
                ":one", AttributeValue.fromN("1"),
                ":uct", AttributeValue.fromS(useCaseType)
        );

        UpdateItemRequest req = UpdateItemRequest.builder()
                .tableName(tableName)
                .key(Map.of("id", AttributeValue.fromS(electionId)))
                .updateExpression("SET #lid = :lid, #lease = :lease, #lu = :lu, #ttl = :ttl, #gen = if_not_exists(#gen, :gen) + 1, #uct = :uct")
                .conditionExpression("attribute_not_exists(#lid) OR #ttl < :now")
                .expressionAttributeNames(names)
                .expressionAttributeValues(values)
                .returnValues(ReturnValue.UPDATED_NEW)
                .build();

        try {
            var result = dynamo.updateItem(req);
            var newAttrs = result.attributes();
            if (newAttrs.containsKey("generation")) {
                currentGeneration = Long.parseLong(newAttrs.get("generation").n());
            }
            log.info("Leadership acquired by {} with generation {}", nodeId, currentGeneration);
            status.set(LeadershipStatus.PENDING);
            currentLeaderId = nodeId;

            scheduler.schedule(() -> {
                if (status.get() == LeadershipStatus.PENDING) {
                    status.set(LeadershipStatus.LEADER);
                    log.info("Leadership confirmed for {}", nodeId);
                }
            }, confirmationDelaySeconds, TimeUnit.SECONDS);

        } catch (ConditionalCheckFailedException e) {
            log.debug("Could not acquire leadership, another node is likely leader.");
        } catch (Exception e) {
            log.error("Error trying to acquire leadership: {}", e.getMessage());
            e.printStackTrace();
        }
    }*/
      private void tryAcquireLeadership() {
           long now = Instant.now().getEpochSecond();
           long ttl = now + leaseDurationSec;

           Map<String, String> names = Map.of(
                   "#lid", "leaderId",
                   "#lease", "leaseDurationSeconds",
                   "#lu", "lastUpdatedEpochSec",
                   "#ttl", "ttlEpochSec",
                   "#gen", "generation",
                   "#uct", "useCaseType"
           );

           Map<String, AttributeValue> values = Map.of(
                   ":leaderId", AttributeValue.fromS(nodeId),
                   ":lease", AttributeValue.fromN(Integer.toString(leaseDurationSec)),
                   ":now", AttributeValue.fromN(Long.toString(now)),
                   ":ttl", AttributeValue.fromN(Long.toString(ttl)),
                   ":zero", AttributeValue.fromN("0"),
                   ":one", AttributeValue.fromN("1"),
                   ":uct", AttributeValue.fromS(useCaseType)
           );

           UpdateItemRequest req = UpdateItemRequest.builder()
                   .tableName(tableName)
                   .key(Map.of("id", AttributeValue.fromS(electionId)))
                   .updateExpression("SET #lid = :leaderId, #lease = :lease, #lu = :now, #ttl = :ttl, #gen = if_not_exists(#gen, :zero) + :one, #uct = :uct")
                   .conditionExpression("attribute_not_exists(#lid) OR #ttl < :now")
                   .expressionAttributeNames(names)
                   .expressionAttributeValues(values)
                   .returnValues(ReturnValue.UPDATED_NEW)
                   .build();

           try {
               var result = dynamo.updateItem(req);
               var newAttributes = result.attributes();
               if (newAttributes.containsKey("generation")) {
                   currentGeneration = Long.parseLong(newAttributes.get("generation").n());
               }

               log.info("Leadership acquired by {} with generation {}", nodeId, currentGeneration);
               status.set(LeadershipStatus.PENDING);
               currentLeaderId = nodeId;

               // Schedule confirmation after a delay
               scheduler.schedule(() -> {
                   if (status.get() == LeadershipStatus.PENDING) {
                       status.set(LeadershipStatus.LEADER);
                       log.info("Leadership confirmed for {}", nodeId);
                   }
               }, confirmationDelaySeconds, TimeUnit.SECONDS);

           } catch (ConditionalCheckFailedException e) {
               // This is expected when another node is leader
               log.debug("Could not acquire leadership, another node is likely leader.");
           } catch (Exception e) {
               log.error("Error trying to acquire leadership: {}", e.getMessage(), e);
           }
       }

    private void readLeaderState() {
        try {
            var result = dynamo.getItem(r -> r
                    .tableName(tableName)
                    .key(Map.of("id", AttributeValue.fromS(electionId)))
                    .consistentRead(true)
            );
            if (result.hasItem()) {
                var item = result.item();
                currentLeaderId = item.getOrDefault("leaderId", AttributeValue.fromS("unknown")).s();
                currentGeneration = Long.parseLong(item.getOrDefault("generation", AttributeValue.fromN("0")).n());
            } else {
                currentLeaderId = null;
                currentGeneration = 0L;
            }
        } catch (Exception e) {
            log.error("Failed to read leader state: {}", e.getMessage());
            currentLeaderId = null;
            currentGeneration = 0L;
        }
    }

    private void releaseLeadership() {
        if (!isLeader()) return;
        try {
            dynamo.deleteItem(r -> r.tableName(tableName)
                    .key(Map.of("id", AttributeValue.fromS(electionId))));
            log.info("Leadership released by node {}", nodeId);
        } catch (Exception e) {
            log.error("Error releasing leadership: {}", e.getMessage());
        }
    }

    private static String hostname() {
        try {
            return InetAddress.getLocalHost().getHostName();
        } catch (Exception e) {
            return "unknown-host";
        }
    }
}
