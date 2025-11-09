package org.novendu.consumer.ws;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;

import org.novendu.consumer.leader.LeaderElectionService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.messaging.simp.stomp.*;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;
import org.springframework.web.socket.client.standard.StandardWebSocketClient;
import org.springframework.web.socket.messaging.WebSocketStompClient;

import java.lang.reflect.Type;
import java.nio.charset.StandardCharsets;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * WebSocket/STOMP event consumer that remains connected only while this node is leader.
 *
 * <p>
 * A periodic reconciliation (ensureConnection) compares desired state (based on {@link LeaderElectionService#isLeader()})
 * to actual state tracked by the {@code connected} flag and opens or closes a STOMP session accordingly.
 * The connection attempt is a blocking call (Future#get) until the handshake completes or fails.
 * </p>
 *
 * <p><b>Thread-safety:</b></p>
 * <ul>
 *   <li>{@code connected} is an {@link AtomicBoolean} to allow safe reads/writes from the scheduled task and STOMP callbacks.</li>
 *   <li>{@code session} is {@code volatile} to make the established session visible across threads once set.</li>
 * </ul>
 *
 * <p>
 * Message handling subscribes to a single topic (configured) and treats payloads as raw {@code byte[]}
 * to avoid converter issues; they are decoded as UTF-8 JSON and optionally mapped to a {@code Map<String,Object>}.
 * </p>
 */
@Component
public class WsEventConsumer {

    private static final Logger log = LoggerFactory.getLogger(WsEventConsumer.class);

    private final LeaderElectionService leaderService;
    private final String wsConnectUrl;
    private final String topic;

    private final WebSocketStompClient stompClient;
    private final AtomicBoolean connected = new AtomicBoolean(false);
    private volatile StompSession session;

    private final ObjectMapper mapper = new ObjectMapper();

    public WsEventConsumer(
            LeaderElectionService leaderService,
            @Value("${ws.url:ws://localhost:8080/ws}") String wsConnectUrl,
            @Value("${ws.topic:/topic/events}") String topic
    ) {
        this.leaderService = leaderService;
        this.wsConnectUrl = wsConnectUrl;
        this.topic = topic;

        // Native WebSocket STOMP client (no SockJS)
        this.stompClient = new WebSocketStompClient(new StandardWebSocketClient());

        log.info("Initialized WsEventConsumer for URL={} and topic={}", wsConnectUrl, topic);
    }

    /**
     * Periodically reconciles desired connection state with actual state.
     * <ul>
     *   <li>If we have become leader and are not connected, initiates a blocking connect().</li>
     *   <li>If we are no longer leader but still connected, disconnects().</li>
     *   <li>If state matches desired, no operation.</li>
     * </ul>
     * Runs every 5 seconds after an initial 5-second delay.
     */
    @Scheduled(initialDelayString = "PT5S", fixedDelayString = "PT5S")
    public void ensureConnection() {
        boolean shouldBeConnected = leaderService.isLeader();

        if (shouldBeConnected && !connected.get()) {
            connect();
        } else if (!shouldBeConnected && connected.get()) {
            disconnect();
        }
    }

    /**
     * Establishes a STOMP session and subscribes to the configured topic.
     * Blocks until connection attempt completes (success or failure) via Future#get.
     * The {@code connected} flag is set only after subscription is registered.
     */
    private void connect() {
        try {
            log.info("Attempting native WS connect to {}", wsConnectUrl);
            this.session = stompClient.connect(wsConnectUrl, new StompSessionHandlerAdapter() {
                @Override
                public void afterConnected(StompSession session, StompHeaders connectedHeaders) {
                    log.info("Connected to WS; subscribing to {}", topic);
                    session.subscribe(topic, new StompFrameHandler() {
                        @Override
                        public Type getPayloadType(StompHeaders headers) {
                            return byte[].class;
                        }

                        @Override
                        public void handleFrame(StompHeaders headers, Object payload) {
                            try {
                                byte[] bytes = (byte[]) payload;
                                String json = new String(bytes, StandardCharsets.UTF_8);
                                log.info("EVENT RECEIVED: {}", json);

                                // Optional: parse JSON to Map for structured handling
                                Map<String, Object> asMap = mapper.readValue(json, new TypeReference<>() {});
                                // Future enhancement: domain-specific processing of asMap
                            } catch (Exception ex) {
                                log.warn("Failed to decode/parse payload: {}", ex.getMessage());
                            }
                        }
                    });
                    connected.set(true);
                }

                @Override
                public void handleTransportError(StompSession session, Throwable exception) {
                    log.warn("Transport error: {}", exception.getMessage());
                    connected.set(false);
                }
            }).get();
        } catch (Exception e) {
            connected.set(false);
            log.warn("WS connect failed: {}", e.getMessage());
        }
    }

    /**
     * Disconnects the STOMP session if active and clears local connection state.
     * Safe to call multiple times; errors during disconnect are swallowed.
     */
    private void disconnect() {
        try {
            if (session != null && session.isConnected()) {
                session.disconnect();

            }
        } catch (Exception e) {
            log.warn("Error during WS disconnect: {}", e.getMessage());
        } finally {
            connected.set(false);
            log.info("Disconnected from WS [Not Leader]");
        }
    }
}
