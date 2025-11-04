# Sample-WS-Consumer

Spring Boot **WebSocket consumer** that prints events from a producer topic and uses **DynamoDB leader election** so that only the leader consumes.

## Features
- STOMP over WebSocket client (SockJS) -> subscribes to `/topic/events`
- Prints each received event to stdout and logs
- Leader election with DynamoDB (SDK v2) using conditional writes + lease TTL
- REST endpoint `GET /leader` -> returns `{ isLeader, currentLeaderId }`
- Auto-creates DynamoDB table `LeaderElection` (for LocalStack/dev)

## Prerequisites
- Java 17 (Amazon Corretto 17)
- Maven 3.9+
- **LocalStack** for local DynamoDB (or real AWS account)
- Environment variables:
  ```bash
  export AWS_ACCESS_KEY_ID=test
  export AWS_SECRET_ACCESS_KEY=test
  export AWS_DEFAULT_REGION=us-east-1
  ```

## Quick Start (LocalStack)
1. Start LocalStack (make sure edge is on 4566):
   ```bash
   localstack start
   ```

2. Configure app (already set in `src/main/resources/application.yml`):
   ```yaml
   app:
     ws:
       url: http://localhost:8080/ws   # producer's SockJS endpoint
       topic: /topic/events
     leader:
       table: LeaderElection
       electionId: poc-ws-events-leader-election
       leaseSeconds: 30
     dynamo:
       region: us-east-1
       endpointOverride: http://localhost:4566
   ```

3. Build & run:
   ```bash
   mvn clean package
   java -jar target/Sample-WS-Consumer-1.0-SNAPSHOT.jar
   ```

4. Verify table and item (optional):
   ```bash
   aws dynamodb list-tables --endpoint-url http://localhost:4566
   aws dynamodb describe-table --table-name LeaderElection --endpoint-url http://localhost:4566
   ```

5. Check leader state:
   ```bash
   curl http://localhost:8080/leader
   # -> { "isLeader": true/false, "currentLeaderId": "..." }
   ```

6. Open another terminal and start **another** consumer instance.
   Only one will be `isLeader=true`; stop the leader to see automatic re-election.

## How the leader election works
- **Acquire**: `PutItem` with condition `attribute_not_exists(leaderId) OR :now > ttlEpochSec`
- **Heartbeat** (every 10s): `UpdateItem` with condition `leaderId = :me AND :now <= ttlEpochSec`
- **Lease/TTL**: item stores `ttlEpochSec` (`now + leaseSeconds`). If heartbeat stops, others can acquire.
- Table is auto-created on startup in dev/local (PAY_PER_REQUEST + HASH key `id`).
  Enable TTL on `ttlEpochSec` in real AWS if you want automatic cleanup.

## Logs
- File: `logs/consumer.log`
- Console shows `EVENT: {...}` lines for each received message while this node is leader.

## Notes
- If your producer doesn’t use SockJS, change `app.ws.url` to `ws://host:8080/ws` and swap the client to `StandardWebSocketClient` without `SockJsClient`.
- For real AWS, remove `endpointOverride` and rely on IAM instance/profile or real credentials.
