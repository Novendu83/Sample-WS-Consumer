package org.novendu.consumer.leader;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import software.amazon.awssdk.services.dynamodb.model.AttributeValue;
import software.amazon.awssdk.services.dynamodb.model.ConditionalCheckFailedException;
import software.amazon.awssdk.services.dynamodb.model.PutItemRequest;
import software.amazon.awssdk.services.dynamodb.model.UpdateItemRequest;

import java.net.InetAddress;
import java.time.Instant;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

@Service
public class LeaderElectionService {

    private static final Logger log = LoggerFactory.getLogger(LeaderElectionService.class);

    private final DynamoDbClient dynamo;
    private final String tableName;
    private final String electionId;
    private final int leaseDurationSec;
    private final String nodeId;

    private volatile boolean leader = false;
    private volatile String currentLeaderId = null;

    public LeaderElectionService(
            DynamoDbClient dynamo,
            @Value("${app.leader.table:LeaderElection}") String tableName,
            @Value("${app.leader.electionId:poc-ws-events-leader-election}") String electionId,
            @Value("${app.leader.leaseSeconds:30}") int leaseDurationSec,
            @Value("${app.env:USEast1-Blue}") String envTag
    ) {
        this.dynamo = dynamo;
        this.tableName = tableName;
        this.electionId = electionId;
        this.leaseDurationSec = leaseDurationSec;
        this.nodeId = envTag + "-" + hostname() + "-" + UUID.randomUUID().toString().substring(0, 8);
        log.info("NodeId for leader election: {}", nodeId);
    }

    public boolean isLeader() { return leader; }
    public String getCurrentLeaderId() { return currentLeaderId; }

    @Scheduled(initialDelayString = "PT3S", fixedDelayString = "PT5S")
    public void tryAcquireIfNotLeader() {
        if (leader) return;
        tryAcquireLeadership();
    }

    @Scheduled(initialDelayString = "PT10S", fixedDelayString = "PT10S")
    public void heartbeat() {
        if (!leader) return;

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
                .updateExpression("SET #lid = :me, #lu = :now, #ttl = :ttl")
                .conditionExpression("#lid = :me AND :now <= #ttl")
                .expressionAttributeNames(names)
                .expressionAttributeValues(values)
                .build();

        try {
            dynamo.updateItem(req);
            currentLeaderId = nodeId;
            log.debug("Heartbeat OK; leaderId={}", nodeId);
        } catch (ConditionalCheckFailedException e) {
            log.warn("Heartbeat failed; lost leadership.");
            leader = false;
            currentLeaderId = readLeaderId();
        } catch (Exception e) {
            log.error("Heartbeat error: {}", e.getMessage());
        }
    }

    private void tryAcquireLeadership() {
        long now = Instant.now().getEpochSecond();
        long ttl = now + leaseDurationSec;

        Map<String, AttributeValue> item = new HashMap<>();
        item.put("id", AttributeValue.fromS(electionId));
        item.put("leaderId", AttributeValue.fromS(nodeId));
        item.put("leaseDurationSeconds", AttributeValue.fromN(Integer.toString(leaseDurationSec)));
        item.put("lastUpdatedEpochSec", AttributeValue.fromN(Long.toString(now)));
        item.put("ttlEpochSec", AttributeValue.fromN(Long.toString(ttl)));

        PutItemRequest req = PutItemRequest.builder()
                .tableName(tableName)
                .item(item)
                .conditionExpression("attribute_not_exists(leaderId) OR :now > ttlEpochSec")
                .expressionAttributeValues(Map.of(":now", AttributeValue.fromN(Long.toString(now))))
                .build();

        try {
            dynamo.putItem(req);
            leader = true;
            currentLeaderId = nodeId;
            log.info("✅ Acquired leadership: {}", nodeId);
        } catch (ConditionalCheckFailedException e) {
            leader = false;
            currentLeaderId = readLeaderId();
        } catch (Exception e) {
            leader = false;
            log.error("Error acquiring leadership: {}", e.getMessage());
        }
    }

    private String readLeaderId() {
        try {
            var resp = dynamo.getItem(b -> b
                    .tableName(tableName)
                    .key(Map.of("id", AttributeValue.fromS(electionId)))
                    .consistentRead(false));
            if (resp.hasItem() && resp.item().containsKey("leaderId")) {
                return resp.item().get("leaderId").s();
            }
        } catch (Exception e) {
            log.warn("readLeaderId failed: {}", e.getMessage());
        }
        return null;
    }

    private static String hostname() {
        try { return InetAddress.getLocalHost().getHostName(); }
        catch (Exception e) { return "unknown-host"; }
    }
}
