package org.novendu.consumer.config;

import jakarta.annotation.PostConstruct;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.DefaultCredentialsProvider;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.core.client.config.ClientOverrideConfiguration;
import software.amazon.awssdk.http.urlconnection.UrlConnectionHttpClient;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import software.amazon.awssdk.services.dynamodb.model.*;

import java.net.URI;
import java.time.Duration;

@Configuration
public class AwsConfig {

    private static final Logger log = LoggerFactory.getLogger(AwsConfig.class);

    @Value("${app.dynamo.region:us-east-1}")
    private String region;

    @Value("${app.dynamo.endpointOverride:}")
    private String endpointOverride;

    @Value("${app.dynamo.accessKey:test}")
    private String accessKey;

    @Value("${app.dynamo.secretKey:test}")
    private String secretKey;

    @Value("${app.leader.table:LeaderElection}")
    private String leaderTable;

    @Bean
    public DynamoDbClient dynamoDbClient() {
        var builder = DynamoDbClient.builder()
                .region(Region.of(region))
                .httpClient(UrlConnectionHttpClient.create())
                .overrideConfiguration(ClientOverrideConfiguration.builder()
                        .apiCallTimeout(Duration.ofSeconds(10))
                        .build());

        if (endpointOverride != null && !endpointOverride.isBlank()) {
            builder = builder
                    .endpointOverride(URI.create(endpointOverride))
                    .credentialsProvider(
                            StaticCredentialsProvider.create(
                                    AwsBasicCredentials.create(accessKey, secretKey)
                            ));
            log.info("Using static credentials for LocalStack ({} / {})", accessKey, secretKey);
        } else {
            builder = builder.credentialsProvider(DefaultCredentialsProvider.create());
            log.info("Using DefaultCredentialsProvider for AWS");
        }

        var client = builder.build();

        // 🟢 Ensure the table exists before returning the bean
        ensureTableExists(client);

        return client;
    }

    private void ensureTableExists(DynamoDbClient client) {
        try {
            var tables = client.listTables();
            if (!tables.tableNames().contains(leaderTable)) {
                log.info("Creating DynamoDB table '{}' ...", leaderTable);

                var request = CreateTableRequest.builder()
                        .tableName(leaderTable)
                        .attributeDefinitions(
                                AttributeDefinition.builder()
                                        .attributeName("id")
                                        .attributeType(ScalarAttributeType.S)
                                        .build()
                        )
                        .keySchema(
                                KeySchemaElement.builder()
                                        .attributeName("id")
                                        .keyType(KeyType.HASH)
                                        .build()
                        )
                        .billingMode(BillingMode.PAY_PER_REQUEST)
                        .build();

                client.createTable(request);
                log.info("Waiting for table '{}' to become ACTIVE ...", leaderTable);
                client.waiter().waitUntilTableExists(
                        DescribeTableRequest.builder().tableName(leaderTable).build());
                log.info("✅ Table '{}' created successfully and is ACTIVE", leaderTable);

            } else {
                log.info("✅ Table '{}' already exists", leaderTable);
            }
        } catch (ResourceInUseException e) {
            log.warn("Table '{}' already in use.", leaderTable);
        } catch (Exception e) {
            log.error("Error ensuring DynamoDB table exists: {}", e.getMessage(), e);
        }
    }
}
