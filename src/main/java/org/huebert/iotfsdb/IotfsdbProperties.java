package org.huebert.iotfsdb;

import lombok.Data;
import org.huebert.iotfsdb.api.schema.NumberType;
import org.huebert.iotfsdb.api.schema.PartitionPeriod;
import org.huebert.iotfsdb.api.schema.SeriesDefinition;
import org.huebert.iotfsdb.api.schema.SeriesFile;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.boot.context.properties.NestedConfigurationProperty;
import org.springframework.stereotype.Component;
import org.springframework.validation.annotation.Validated;

import java.nio.file.Path;
import javax.validation.Valid;
import javax.validation.constraints.Min;
import javax.validation.constraints.NotNull;

/**
 * Configuration properties for IoTFSDB
 */
@Data
@Component
@Validated
@ConfigurationProperties("iotfsdb")
public class IotfsdbProperties {

    /**
     * Whether the database is in read-only mode.
     * When true, all write operations will be rejected.
     * Default: false
     */
    private boolean readOnly = false;

    /**
     * API-related configuration properties.
     * Controls which APIs are enabled and their behavior.
     */
    @NotNull
    @Valid
    @NestedConfigurationProperty
    private ApiProperties api = new ApiProperties();

    /**
     * Persistence-related configuration properties.
     * Controls where and how data is stored.
     */
    @NotNull
    @Valid
    @NestedConfigurationProperty
    private PersistenceProperties persistence = new PersistenceProperties();

    /**
     * Query-related configuration properties.
     * Controls parameters like maximum result size for queries.
     */
    @NotNull
    @Valid
    @NestedConfigurationProperty
    private QueryProperties query = new QueryProperties();

    /**
     * Series-related configuration properties.
     * Controls behavior related to time series creation and defaults.
     */
    @NotNull
    @Valid
    @NestedConfigurationProperty
    private SeriesProperties series = new SeriesProperties();

    /**
     * Statistics-related configuration properties.
     * Controls collection and reporting of runtime statistics.
     */
    @NotNull
    @Valid
    @NestedConfigurationProperty
    private StatsProperties stats = new StatsProperties();

    /**
     * Configuration properties for APIs.
     */
    @Data
    @Validated
    public static class ApiProperties {

        /**
         * Whether to enable REST API.
         * When true, HTTP endpoints for database operations will be available.
         * Default: true
         */
        private boolean rest = true;

        /**
         * Whether to enable UI.
         * When true, a web-based user interface will be available.
         * Default: false
         */
        private boolean ui = false;

        /**
         * Whether to enable gRPC API.
         * When true, gRPC endpoints for database operations will be available.
         * Default: false
         */
        private boolean grpc = false;

        /**
         * Whether to enable internal gRPC API.
         * When true, internal gRPC endpoints for system operations will be available.
         * These endpoints are primarily used for inter-service communication.
         * Default: false
         */
        private boolean internal = false;

        /**
         * AI-related configuration properties.
         * Controls AI-specific features and behaviors.
         */
        @NotNull
        @Valid
        @NestedConfigurationProperty
        private AiProperties ai = new AiProperties();

    }

    /**
     * Configuration properties for AI features.\
     */
    @Data
    @Validated
    public static class AiProperties {
        /**
         * Whether to enable chat functionality.
         * When true, AI-powered chat interfaces will be available.
         * Default: false
         */
        private boolean chat = false;

        /**
         * Whether to enable MCP (Model Control Protocol) functionality.
         * When true, AI model control interfaces will be available.
         * Default: false
         */
        private boolean mcp = false;
    }

    /**
     * Configuration properties for data persistence.
     */
    @Data
    @Validated
    public static class PersistenceProperties {

        /**
         * Root directory for database storage.
         * Use "memory" for in-memory storage (no persistence).
         * Use an absolute path for file-based storage.
         * Default: "memory"
         */
        @NotNull
        private Path root = Path.of("memory");

        /**
         * Configuration for the partition cache using Caffeine cache syntax.
         * Format: comma-separated list of key=value pairs.
         * Common options:
         * - expireAfterAccess: Duration after which entries are expired if not accessed
         * - maximumSize: Maximum number of entries in the cache
         * - softValues: Whether to use soft references for values
         * Default: "expireAfterAccess=5m,maximumSize=10000,softValues"
         */
        @NotNull
        private String partitionCache = "expireAfterAccess=5m,maximumSize=10000,softValues";
    }

    /**
     * Configuration properties for queries.
     */
    @Data
    @Validated
    public static class QueryProperties {

        /**
         * Maximum number of results returned by a query.
         * This setting helps prevent out-of-memory errors from very large result sets.
         * Queries that would return more results will be resampled.
         * Minimum value: 1
         * Default: 1000
         */
        @Min(1)
        private int maxSize = 1000;

    }

    /**
     * Configuration properties for Series.
     */
    @Data
    @Validated
    public static class SeriesProperties {

        /**
         * Whether to create series automatically on insert.
         * When true, series will be created using the defaultSeries configuration if they don't exist.
         * When false, attempts to insert data into non-existent series will fail.
         * Default: true
         */
        private boolean createOnInsert = true;

        /**
         * Default series configuration used when creating new series.
         * This configuration is used when series are auto-created or when no specific configuration is provided.
         * Includes data type, collection interval, partition strategy, and metadata.
         */
        @NotNull
        @Valid
        private SeriesFile defaultSeries = SeriesFile.builder()
            .definition(SeriesDefinition.builder()
                .type(NumberType.FLOAT4)
                .interval(60000L)
                .partition(PartitionPeriod.DAY)
                .build())
            .metadata("createdBy", "Iotfsdb")
            .build();

    }

    /**
     * Configuration properties for statistics collection.
     */
    @Data
    @Validated
    public static class StatsProperties {

        /**
         * Whether to collect and report statistics.
         * When true, runtime statistics about database operations will be collected and made available.
         * This may have a small performance impact.
         * Default: false
         */
        private boolean enabled = false;

    }

}
