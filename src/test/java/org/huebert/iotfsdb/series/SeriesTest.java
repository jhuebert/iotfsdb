package org.huebert.iotfsdb.series;

import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.junit.jupiter.MockitoExtension;

import java.time.Duration;
import java.time.LocalDateTime;
import java.util.List;

@ExtendWith(MockitoExtension.class)
public class SeriesTest {

    private static final LocalDateTime START = LocalDateTime.parse("2024-02-01T00:00:00");

    private static final LocalDateTime END = LocalDateTime.parse("2024-03-01T00:00:00").minusNanos(1);

    private static final int DAYS = (int) Duration.between(START, END.plusNanos(1)).toDays();

    private static final float VALUE = 123.456f;

    private static final List<Float> LIST = List.of(VALUE);

//    @Mock
//    private FileBasedArray<Float> fileBasedArray;
//
//    @Test
//    public void testClose() throws Exception {
//        when(fileBasedArray.size()).thenReturn(DAYS);
//        SeriesFile<Float> seriesFile = new SeriesFile<>(fileBasedArray, START, PartitionPeriod.MONTH);
//        seriesFile.close();
//        verify(fileBasedArray).size();
//        verify(fileBasedArray).close();
//    }
//
//    @Test
//    public void testSetStart() {
//        when(fileBasedArray.size()).thenReturn(DAYS);
//        SeriesFile<Float> seriesFile = new SeriesFile<>(fileBasedArray, START, PartitionPeriod.MONTH);
//        seriesFile.set(START, VALUE);
//        verify(fileBasedArray).set(0, VALUE);
//    }
//
//    @Test
//    public void testSetEnd() {
//        when(fileBasedArray.size()).thenReturn(DAYS);
//        SeriesFile<Float> seriesFile = new SeriesFile<>(fileBasedArray, START, PartitionPeriod.MONTH);
//        seriesFile.set(END, VALUE);
//        verify(fileBasedArray).set(28, VALUE);
//    }
//
//    @Test
//    public void testSetArbitrary() {
//        when(fileBasedArray.size()).thenReturn(DAYS);
//        SeriesFile<Float> seriesFile = new SeriesFile<>(fileBasedArray, START, PartitionPeriod.MONTH);
//        seriesFile.set(START.plusDays(14), VALUE);
//        verify(fileBasedArray).set(14, VALUE);
//    }
//
//    @Test
//    public void testGetAllExact() {
//        when(fileBasedArray.size()).thenReturn(DAYS);
//        when(fileBasedArray.get(0, 29)).thenReturn(LIST);
//        SeriesFile<Float> seriesFile = new SeriesFile<>(fileBasedArray, START, PartitionPeriod.MONTH);
//        assertThat(seriesFile.get(Range.closed(START, END))).isEqualTo(LIST);
//        verify(fileBasedArray).get(0, 29);
//    }
//
//    @Test
//    public void testGetEmpty() {
//        when(fileBasedArray.size()).thenReturn(DAYS);
//        SeriesFile<Float> seriesFile = new SeriesFile<>(fileBasedArray, START, PartitionPeriod.MONTH);
//        assertThat(seriesFile.get(Range.closedOpen(START.plusDays(14), START.plusDays(14)))).isEqualTo(List.of());
//    }
//
//    @Test
//    public void testGetSingle() {
//        when(fileBasedArray.size()).thenReturn(DAYS);
//        when(fileBasedArray.get(14, 1)).thenReturn(LIST);
//        SeriesFile<Float> seriesFile = new SeriesFile<>(fileBasedArray, START, PartitionPeriod.MONTH);
//        assertThat(seriesFile.get(Range.closed(START.plusDays(14), START.plusDays(14)))).isEqualTo(LIST);
//        verify(fileBasedArray).get(14, 1);
//    }
//
//    @Test
//    public void testGetRangeContained() {
//        when(fileBasedArray.size()).thenReturn(DAYS);
//        when(fileBasedArray.get(14, 8)).thenReturn(LIST);
//        SeriesFile<Float> seriesFile = new SeriesFile<>(fileBasedArray, START, PartitionPeriod.MONTH);
//        assertThat(seriesFile.get(Range.closed(START.plusDays(14), START.plusDays(21)))).isEqualTo(LIST);
//        verify(fileBasedArray).get(14, 8);
//    }
//
//    @Test
//    public void testGetRangeLowerOverlap() {
//        when(fileBasedArray.size()).thenReturn(DAYS);
//        when(fileBasedArray.get(0, 8)).thenReturn(LIST);
//        SeriesFile<Float> seriesFile = new SeriesFile<>(fileBasedArray, START, PartitionPeriod.MONTH);
//        assertThat(seriesFile.get(Range.closed(START.minusDays(7), START.plusDays(7)))).isEqualTo(LIST);
//        verify(fileBasedArray).get(0, 8);
//    }
//
//    @Test
//    public void testGetRangeUpperOverlap() {
//        List<Float> expected = List.of(1.0f, 2.0f, 3.0f, 4.0f);
//        when(fileBasedArray.size()).thenReturn(DAYS);
//        when(fileBasedArray.get(21, 8)).thenReturn(expected);
//        SeriesFile<Float> seriesFile = new SeriesFile<>(fileBasedArray, START, PartitionPeriod.MONTH);
//        assertThat(seriesFile.get(Range.closed(START.plusDays(21), END.plusDays(7)))).isEqualTo(expected);
//        verify(fileBasedArray).get(21, 8);
//    }
//
//    @Test
//    public void testGetRangeCompleteOverlap() {
//
//        when(fileBasedArray.size()).thenReturn(DAYS);
//        when(fileBasedArray.get(0, 29)).thenReturn(LIST);
//        SeriesFile<Float> seriesFile = new SeriesFile<>(fileBasedArray, START, PartitionPeriod.MONTH);
//        assertThat(seriesFile.get(Range.closed(START.minusDays(7), END.plusDays(7)))).isEqualTo(LIST);
//        verify(fileBasedArray).get(0, 29);
//    }

}
