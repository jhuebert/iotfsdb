package org.huebert.iotfsdb.file;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.ArrayList;
import java.util.List;
import java.util.regex.Pattern;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.fail;
import static org.junit.jupiter.api.Assertions.assertThrows;

public class IntegerFileBasedArrayTest {

    private File file;

    @BeforeEach
    public void beforeEach() throws IOException {
        file = File.createTempFile(IntegerFileBasedArrayTest.class.getSimpleName(), "");
        file.deleteOnExit();
        file.delete();
    }

    @AfterEach
    public void afterEach() {
        file.delete();
    }

    @Test
    public void testCreateFileExists() throws Exception {
        file.createNewFile();
        try (IntegerFileBasedArray array = IntegerFileBasedArray.create(file, 100)) {
            fail("expected creation to fail since the file already exists");
        } catch (IllegalArgumentException e) {
            assertThat(e.getMessage()).matches(Pattern.compile("file .* already exists"));
        }
    }

    @Test
    public void testCreateInvalidSize() throws Exception {
        assertThrows(IllegalArgumentException.class, () -> IntegerFileBasedArray.create(file, -10));
    }

    @Test
    public void testCreate() throws Exception {
        try (IntegerFileBasedArray array = IntegerFileBasedArray.create(file, 10)) {
            assertThat(array.size()).isEqualTo(10);
            assertThat(file.exists()).isEqualTo(true);
            assertThat(file.canRead()).isEqualTo(true);
            assertThat(file.canWrite()).isEqualTo(true);
            assertThat(file.isFile()).isEqualTo(true);
            assertThat(file.length()).isEqualTo(40);
            for (int i = 0; i < 10; i++) {
                assertThat(array.get(i, 1).get(0)).isEqualTo(null);
            }
        }
    }

    @Test
    public void testReadEmpty() throws Exception {
        assertThrows(IllegalArgumentException.class, () -> IntegerFileBasedArray.read(file, false));
    }

    @Test
    public void testReadInvalidSize() throws Exception {
        RandomAccessFile raf = new RandomAccessFile(file, "rw");
        raf.setLength(5);
        raf.close();
        assertThrows(IllegalArgumentException.class, () -> IntegerFileBasedArray.read(file, false));
    }

    @Test
    public void testRead() throws Exception {
        RandomAccessFile raf = new RandomAccessFile(file, "rw");
        raf.setLength(40);
        raf.close();
        try (IntegerFileBasedArray array = IntegerFileBasedArray.read(file, false)) {
            List<Integer> result = array.get(0, 10);
            for (int i = 0; i < 10; i++) {
                assertThat(result.get(i)).isEqualTo(0);
            }
            array.set(0, 500);
            assertThat(array.get(0, 1).get(0)).isEqualTo(500);
        }
    }

    @Test
    public void testReadReadOnly() throws Exception {
        RandomAccessFile raf = new RandomAccessFile(file, "rw");
        raf.setLength(40);
        raf.close();
        try (IntegerFileBasedArray array = IntegerFileBasedArray.read(file, true)) {
            assertThrows(IllegalStateException.class, () -> array.set(0, 500));
        }
    }

    @Test
    public void testSetAndGet() throws Exception {
        try (IntegerFileBasedArray array = IntegerFileBasedArray.create(file, 10)) {

            array.set(0, 1001);
            array.set(1, 1003);
            array.set(1, null);
            array.set(9, 1002);

            assertThat(array.get(0, 1)).isEqualTo(List.of(1001));
            List<Float> expectedList = new ArrayList<>();
            expectedList.add(null);
            assertThat(array.get(1, 1)).isEqualTo(expectedList);
            assertThat(array.get(9, 1)).isEqualTo(List.of(1002));

            List<Integer> result = array.get(0, 10);
            for (int i = 0; i < 10; i++) {
                Integer expected = null;
                if (i == 0) {
                    expected = 1001;
                } else if (i == 9) {
                    expected = 1002;
                }
                assertThat(result.get(i)).isEqualTo(expected);
            }
        }
    }

    @Test
    public void testGetZeroLength() throws Exception {
        try (IntegerFileBasedArray array = IntegerFileBasedArray.create(file, 10)) {
            List<Integer> result = array.get(5, 0);
            assertThat(result.size()).isEqualTo(0);
        }
    }

    @Test
    public void testGetInvalidIndex() throws Exception {
        try (IntegerFileBasedArray array = IntegerFileBasedArray.create(file, 10)) {
            assertThrows(IndexOutOfBoundsException.class, () -> array.get(-1, 1));
            assertThrows(IndexOutOfBoundsException.class, () -> array.get(10, 1));
        }
    }

    @Test
    public void testSetInvalidIndex() throws Exception {
        try (IntegerFileBasedArray array = IntegerFileBasedArray.create(file, 10)) {
            assertThrows(IndexOutOfBoundsException.class, () -> array.set(-1, 500));
            assertThrows(IndexOutOfBoundsException.class, () -> array.set(10, 500));
        }
    }

    @Test
    public void testGetInvalidLength() throws Exception {
        try (IntegerFileBasedArray array = IntegerFileBasedArray.create(file, 10)) {
            assertThrows(NegativeArraySizeException.class, () -> array.get(0, -1));
            assertThrows(IndexOutOfBoundsException.class, () -> array.get(0, 11));
            assertThrows(IndexOutOfBoundsException.class, () -> array.get(5, 6));
        }
    }
}
