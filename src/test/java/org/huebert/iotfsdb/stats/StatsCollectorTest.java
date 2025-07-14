package org.huebert.iotfsdb.stats;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import org.aspectj.lang.ProceedingJoinPoint;
import org.huebert.iotfsdb.IotfsdbProperties;
import org.huebert.iotfsdb.api.schema.InsertRequest;
import org.huebert.iotfsdb.api.schema.NumberType;
import org.huebert.iotfsdb.api.schema.SeriesFile;
import org.huebert.iotfsdb.service.DataService;
import org.huebert.iotfsdb.service.InsertService;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.lang.reflect.Constructor;
import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.atomic.AtomicLong;

@ExtendWith(MockitoExtension.class)
class StatsCollectorTest {

    @Mock
    private IotfsdbProperties properties;

    @Mock
    private IotfsdbProperties.StatsProperties statsProperties;

    @Mock
    private InsertService insertService;

    @Mock
    private DataService dataService;

    @Mock
    private ProceedingJoinPoint joinPoint;

    private StatsCollector statsCollector;

    private CaptureStats captureStats;

    @BeforeEach
    void setUp() throws Exception {

        // Create StatsCollector instance
        statsCollector = new StatsCollector(properties, insertService, dataService);

        // Reset static maps
        resetStaticMaps();

        // Create test CaptureStats annotation
        captureStats = createCaptureStatsAnnotation();
    }

    @Test
    void testCaptureExecutionTime() throws Throwable {

        // Mock IotfsdbProperties
        when(properties.getStats()).thenReturn(statsProperties);
        when(properties.isReadOnly()).thenReturn(false);
        when(statsProperties.isEnabled()).thenReturn(true);

        // Arrange
        when(joinPoint.proceed()).thenReturn("test result");

        // Act
        Object result = statsCollector.captureExecutionTime(joinPoint, captureStats);

        // Assert
        assertEquals("test result", result);

        // Verify that stats were accumulated
        Map<CaptureStats, Object> statsMap = getStatsMap();
        assertEquals(1, statsMap.size());
        assertTrue(statsMap.containsKey(captureStats));
    }

    @Test
    void testCaptureExecutionTimeWhenDisabled() throws Throwable {

        // Mock IotfsdbProperties
        when(properties.getStats()).thenReturn(statsProperties);
        when(properties.isReadOnly()).thenReturn(false);
        when(statsProperties.isEnabled()).thenReturn(true);

        // Arrange
        when(statsProperties.isEnabled()).thenReturn(false);
        when(joinPoint.proceed()).thenReturn("test result");

        // Act
        Object result = statsCollector.captureExecutionTime(joinPoint, captureStats);

        // Assert
        assertEquals("test result", result);

        // Verify that no stats were accumulated
        Map<CaptureStats, Object> statsMap = getStatsMap();
        assertEquals(0, statsMap.size());
    }

    @Test
    void testCaptureExecutionTimeWhenReadOnly() throws Throwable {

        // Arrange
        when(properties.isReadOnly()).thenReturn(true);
        when(joinPoint.proceed()).thenReturn("test result");

        // Act
        Object result = statsCollector.captureExecutionTime(joinPoint, captureStats);

        // Assert
        assertEquals("test result", result);

        // Verify that no stats were accumulated
        Map<CaptureStats, Object> statsMap = getStatsMap();
        assertEquals(0, statsMap.size());
    }

    @Test
    void testCalculateMeasurementsWithEmptyStats() {

        // Act
        statsCollector.calculateMeasurements();

        // Assert
        verify(insertService, never()).insert(any());
        verify(dataService, never()).saveSeries(any());
    }

    @Test
    void testCalculateMeasurementsWithStats() throws Throwable {

        // Mock IotfsdbProperties
        when(properties.getStats()).thenReturn(statsProperties);
        when(properties.isReadOnly()).thenReturn(false);
        when(statsProperties.isEnabled()).thenReturn(true);

        // Arrange
        // Add some stats to the collector
        when(joinPoint.proceed()).thenReturn("test result");
        statsCollector.captureExecutionTime(joinPoint, captureStats);
        when(dataService.getSeries(anyString())).thenReturn(Optional.empty());

        // Act
        statsCollector.calculateMeasurements();

        // Assert
        // Verify series are created
        ArgumentCaptor<SeriesFile> seriesCaptor = ArgumentCaptor.forClass(SeriesFile.class);
        verify(dataService, times(4)).saveSeries(seriesCaptor.capture());

        List<SeriesFile> capturedSeries = seriesCaptor.getAllValues();
        assertEquals(4, capturedSeries.size()); // Should have 4 series (MIN, MAX, MEAN, COUNT)

        // Verify that values were inserted
        ArgumentCaptor<InsertRequest> insertCaptor = ArgumentCaptor.forClass(InsertRequest.class);
        verify(insertService, times(4)).insert(insertCaptor.capture());

        List<InsertRequest> capturedInserts = insertCaptor.getAllValues();
        assertEquals(4, capturedInserts.size());
    }

    @Test
    void testCalculateMeasurementsWithExistingSeries() throws Throwable {

        // Mock IotfsdbProperties
        when(properties.getStats()).thenReturn(statsProperties);
        when(properties.isReadOnly()).thenReturn(false);
        when(statsProperties.isEnabled()).thenReturn(true);

        // Arrange
        // Add some stats to the collector
        when(joinPoint.proceed()).thenReturn("test result");
        statsCollector.captureExecutionTime(joinPoint, captureStats);

        // Mock existing series
        SeriesFile existingSeries = SeriesFile.builder()
            .metadata(Map.of("existing", "metadata"))
            .build();
        when(dataService.getSeries(anyString())).thenReturn(Optional.of(existingSeries));

        // Act
        statsCollector.calculateMeasurements();

        // Assert
        // Verify existing series are updated
        ArgumentCaptor<SeriesFile> seriesCaptor = ArgumentCaptor.forClass(SeriesFile.class);
        verify(dataService, times(4)).saveSeries(seriesCaptor.capture());

        List<SeriesFile> capturedSeries = seriesCaptor.getAllValues();
        assertEquals(4, capturedSeries.size());

        // Metadata should be merged with existing
        assertTrue(capturedSeries.getFirst().getMetadata().containsKey("existing"));

        // Verify that values were inserted
        ArgumentCaptor<InsertRequest> insertCaptor = ArgumentCaptor.forClass(InsertRequest.class);
        verify(insertService, times(4)).insert(insertCaptor.capture());
    }

    @Test
    void testAccumulatorAddAndGetStat() throws Exception {

        // Access the Accumulator class via reflection
        Class<?> accumulatorClass = getAccumulatorClass();
        Object accumulator = createAccumulator();

        // Test add method
        Method addMethod = accumulatorClass.getMethod("add", long.class);
        addMethod.invoke(accumulator, 1000000000L); // 1000ms
        addMethod.invoke(accumulator, 2000000000L); // 2000ms
        addMethod.invoke(accumulator, 3000000000L); // 3000ms

        // Test getStat method with different Stat values
        Method getStatMethod = accumulatorClass.getMethod("getStat", getStatClass());
        Object minValue = getStatMethod.invoke(accumulator, getStatEnum("MIN"));
        Object maxValue = getStatMethod.invoke(accumulator, getStatEnum("MAX"));
        Object meanValue = getStatMethod.invoke(accumulator, getStatEnum("MEAN"));
        Object countValue = getStatMethod.invoke(accumulator, getStatEnum("COUNT"));

        // Assert
        assertEquals(1000.0, (Double) minValue, 0.001);
        assertEquals(3000.0, (Double) maxValue, 0.001);
        assertEquals(2000.0, (Double) meanValue, 0.001);
        assertEquals(3.0, (Double) countValue, 0.001);
    }

    @Test
    void testAccumulatorWithZeroCount() throws Exception {

        // Access the Accumulator class via reflection
        Class<?> accumulatorClass = getAccumulatorClass();
        Object accumulator = createAccumulator();

        // Test getStat method with no values added
        Object minValue = accumulatorClass.getMethod("getStat", getStatClass())
            .invoke(accumulator, getStatEnum("MIN"));

        // Assert
        assertEquals(0.0, (Double) minValue, 0.001);
    }

    @Test
    void testToMilliseconds() throws Exception {

        // Access the Accumulator class via reflection
        Class<?> accumulatorClass = getAccumulatorClass();
        Object accumulator = createAccumulator();

        // Create a method to access the private toMilliseconds method
        Method toMillisecondsMethod = accumulatorClass.getDeclaredMethod("toMilliseconds", AtomicLong.class);
        toMillisecondsMethod.setAccessible(true);

        // Test toMilliseconds method
        AtomicLong testValue = new AtomicLong(1000000000L); // 1000ms
        Object result = toMillisecondsMethod.invoke(accumulator, testValue);

        // Assert
        assertEquals(1000.0, (Double) result, 0.001);
    }

    @Test
    void testStatEnum() {
        // Test Stat enum values
        Class<?> statClass;
        try {
            statClass = getStatClass();
            Object[] enumConstants = statClass.getEnumConstants();

            assertEquals(4, enumConstants.length);

            // Test MIN
            Object minStat = enumConstants[0];
            assertEquals("MIN", minStat.toString());
            assertEquals("min", getEnumField(minStat, "key"));
            assertEquals("millisecond", getEnumField(minStat, "unit"));
            assertEquals(NumberType.FLOAT4, getEnumField(minStat, "type"));

            // Test MAX
            Object maxStat = enumConstants[1];
            assertEquals("MAX", maxStat.toString());
            assertEquals("max", getEnumField(maxStat, "key"));
            assertEquals("millisecond", getEnumField(maxStat, "unit"));
            assertEquals(NumberType.FLOAT4, getEnumField(maxStat, "type"));

            // Test MEAN
            Object meanStat = enumConstants[2];
            assertEquals("MEAN", meanStat.toString());
            assertEquals("mean", getEnumField(meanStat, "key"));
            assertEquals("millisecond", getEnumField(meanStat, "unit"));
            assertEquals(NumberType.FLOAT4, getEnumField(meanStat, "type"));

            // Test COUNT
            Object countStat = enumConstants[3];
            assertEquals("COUNT", countStat.toString());
            assertEquals("count", getEnumField(countStat, "key"));
            assertEquals("count", getEnumField(countStat, "unit"));
            assertEquals(NumberType.INTEGER4, getEnumField(countStat, "type"));

        } catch (ClassNotFoundException e) {
            fail("Stat enum not found");
        }
    }

    private void resetStaticMaps() throws Exception {

        // Reset STATS_MAP
        Field statsMapField = StatsCollector.class.getDeclaredField("STATS_MAP");
        statsMapField.setAccessible(true);
        Map<?, ?> statsMap = (Map<?, ?>) statsMapField.get(null);
        statsMap.clear();

        // Reset SERIES_MAP
        Field seriesMapField = StatsCollector.class.getDeclaredField("SERIES_MAP");
        seriesMapField.setAccessible(true);
        Set<?> seriesMap = (Set<?>) seriesMapField.get(null);
        seriesMap.clear();

    }

    private Map<CaptureStats, Object> getStatsMap() throws Exception {
        Field statsMapField = StatsCollector.class.getDeclaredField("STATS_MAP");
        statsMapField.setAccessible(true);
        @SuppressWarnings("unchecked")
        Map<CaptureStats, Object> statsMap = (Map<CaptureStats, Object>) statsMapField.get(null);
        return statsMap;
    }

    private static Class<?> getAccumulatorClass() throws ClassNotFoundException {
        return Class.forName("org.huebert.iotfsdb.stats.StatsCollector$Accumulator");
    }

    private static Class<?> getStatClass() throws ClassNotFoundException {
        return Class.forName("org.huebert.iotfsdb.stats.StatsCollector$Stat");
    }

    private Object createAccumulator() throws ClassNotFoundException, NoSuchMethodException, InvocationTargetException, InstantiationException, IllegalAccessException {
        Class<?> accumulatorClass = getAccumulatorClass();
        Constructor<?> declaredConstructor = accumulatorClass.getDeclaredConstructor(CaptureStats.class);
        declaredConstructor.setAccessible(true);
        return declaredConstructor.newInstance(captureStats);
    }

    private CaptureStats createCaptureStatsAnnotation() {
        return new CaptureStats() {
            @Override
            public Class<? extends java.lang.annotation.Annotation> annotationType() {
                return CaptureStats.class;
            }

            @Override
            public String group() {
                return "test-group";
            }

            @Override
            public String type() {
                return "test-type";
            }

            @Override
            public String operation() {
                return "test-operation";
            }

            @Override
            public String version() {
                return "v1";
            }

            @Override
            public Class<?> javaClass() {
                return StatsCollectorTest.class;
            }

            @Override
            public String javaMethod() {
                return "testMethod";
            }

            @Override
            public Metadata[] metadata() {
                return new Metadata[] {
                    new Metadata() {
                        @Override
                        public Class<? extends java.lang.annotation.Annotation> annotationType() {
                            return Metadata.class;
                        }

                        @Override
                        public String key() {
                            return "test-key";
                        }

                        @Override
                        public String value() {
                            return "test-value";
                        }
                    }
                };
            }
        };
    }

    private Object getStatEnum(String name) throws Exception {
        Class<?> statClass = Class.forName("org.huebert.iotfsdb.stats.StatsCollector$Stat");
        Field field = statClass.getDeclaredField(name);
        field.setAccessible(true);
        return field.get(null);
    }

    private <T> T getEnumField(Object enumConstant, String fieldName) {
        try {
            Field field = enumConstant.getClass().getDeclaredField(fieldName);
            field.setAccessible(true);
            @SuppressWarnings("unchecked")
            T value = (T) field.get(enumConstant);
            return value;
        } catch (Exception e) {
            fail("Could not access field: " + fieldName);
            return null;
        }
    }
}
