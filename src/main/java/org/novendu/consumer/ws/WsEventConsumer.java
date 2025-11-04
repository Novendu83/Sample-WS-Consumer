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

    public WsEventConsumer(LeaderElectionService leaderService,
                           @Value("${app.ws.url:ws://localhost:8080/ws}") String wsConnectUrl,
                           @Value("${app.ws.topic:/topic/events}") String topic) {
        this.leaderService = leaderService;
        this.wsConnectUrl = wsConnectUrl;
        this.topic = topic;

        // Native WebSocket STOMP client (no SockJS)
        this.stompClient = new WebSocketStompClient(new StandardWebSocketClient());
        // No explicit MessageConverter: we’ll handle byte[] ourselves
    }

    @Scheduled(initialDelayString = "PT5S", fixedDelayString = "PT5S")
    public void ensureConnection() {
        boolean shouldBeConnected = leaderService.isLeader();
        if (shouldBeConnected && !connected.get()) {
            connect();
        } else if (!shouldBeConnected && connected.get()) {
            disconnect();
        }
    }

    private void connect() {
        try {
            log.info("Attempting native WS connect to {}", wsConnectUrl);
            this.session = stompClient.connect(wsConnectUrl, new StompSessionHandlerAdapter() {
                @Override
                public void afterConnected(StompSession session, StompHeaders connectedHeaders) {
                    log.info("✅ Connected to WS; subscribing to {}", topic);
                    session.subscribe(topic, new StompFrameHandler() {
                        @Override
                        public Type getPayloadType(StompHeaders headers) {
                            // Force byte[] so we can decode regardless of content-type headers
                            return byte[].class;
                        }
                        @Override
                        public void handleFrame(StompHeaders headers, Object payload) {
                            try {
                                byte[] bytes = (byte[]) payload;
                                String json = new String(bytes, StandardCharsets.UTF_8);
                                System.out.println("EVENT(JSON): " + json);
                                log.info("🟢 EVENT RECEIVED: {}", json);
                                // Optional: parse JSON to Map for structured handling
                                Map<String, Object> asMap = mapper.readValue(json, new TypeReference<>() {});
                                // Do something with asMap if needed
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
            log.warn("❌ WS connect failed: {}", e.getMessage());
        }
    }

    private void disconnect() {
        try {
            if (session != null && session.isConnected()) {
                session.disconnect();
            }
        } catch (Exception ignore) {}
        connected.set(false);
        log.info("Disconnected WS (not leader).");
    }
}
