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

public class BooleanFileBasedArrayTest {

    private File file;

    @BeforeEach
    public void beforeEach() throws IOException {
        file = File.createTempFile(BooleanFileBasedArrayTest.class.getSimpleName(), "");
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
        try (BooleanFileBasedArray array = BooleanFileBasedArray.create(file, 100)) {
            fail("expected creation to fail since the file already exists");
        } catch (IllegalArgumentException e) {
            assertThat(e.getMessage()).matches(Pattern.compile("file .* already exists"));
        }
    }

    @Test
    public void testCreateInvalidSize() throws Exception {
        assertThrows(IllegalArgumentException.class, () -> BooleanFileBasedArray.create(file, -10));
    }

    @Test
    public void testCreate() throws Exception {
        try (BooleanFileBasedArray array = BooleanFileBasedArray.create(file, 10)) {
            assertThat(array.size()).isEqualTo(10);
            assertThat(file.exists()).isEqualTo(true);
            assertThat(file.canRead()).isEqualTo(true);
            assertThat(file.canWrite()).isEqualTo(true);
            assertThat(file.isFile()).isEqualTo(true);
            assertThat(file.length()).isEqualTo(10);
            for (int i = 0; i < 10; i++) {
                assertThat(array.get(i, 1).get(0)).isEqualTo(null);
            }
        }
    }

    @Test
    public void testReadEmpty() throws Exception {
        assertThrows(IllegalArgumentException.class, () -> BooleanFileBasedArray.read(file, false));
    }

    @Test
    public void testRead() throws Exception {
        RandomAccessFile raf = new RandomAccessFile(file, "rw");
        raf.setLength(10);
        raf.close();
        try (BooleanFileBasedArray array = BooleanFileBasedArray.read(file, false)) {
            List<Boolean> result = array.get(0, 10);
            for (int i = 0; i < 10; i++) {
                assertThat(result.get(i)).isEqualTo(false);
            }
            array.set(0, true);
            assertThat(array.get(0, 1).get(0)).isEqualTo(true);
        }
    }

    @Test
    public void testReadReadOnly() throws Exception {
        RandomAccessFile raf = new RandomAccessFile(file, "rw");
        raf.setLength(40);
        raf.close();
        try (BooleanFileBasedArray array = BooleanFileBasedArray.read(file, true)) {
            assertThrows(IllegalStateException.class, () -> array.set(0, true));
        }
    }

    @Test
    public void testSetAndGet() throws Exception {
        try (BooleanFileBasedArray array = BooleanFileBasedArray.create(file, 10)) {

            array.set(0, true);
            array.set(1, true);
            array.set(1, null);
            array.set(9, false);

            assertThat(array.get(0, 1)).isEqualTo(List.of(true));
            List<Boolean> expectedList = new ArrayList<>();
            expectedList.add(null);
            assertThat(array.get(1, 1)).isEqualTo(expectedList);
            assertThat(array.get(9, 1)).isEqualTo(List.of(false));

            List<Boolean> result = array.get(0, 10);
            for (int i = 0; i < 10; i++) {
                Boolean expected = null;
                if (i == 0) {
                    expected = true;
                } else if (i == 9) {
                    expected = false;
                }
                assertThat(result.get(i)).isEqualTo(expected);
            }
        }
    }

    @Test
    public void testGetZeroLength() throws Exception {
        try (BooleanFileBasedArray array = BooleanFileBasedArray.create(file, 10)) {
            List<Boolean> result = array.get(5, 0);
            assertThat(result.size()).isEqualTo(0);
        }
    }

    @Test
    public void testGetInvalidIndex() throws Exception {
        try (BooleanFileBasedArray array = BooleanFileBasedArray.create(file, 10)) {
            assertThrows(IndexOutOfBoundsException.class, () -> array.get(-1, 1));
            assertThrows(IndexOutOfBoundsException.class, () -> array.get(10, 1));
        }
    }

    @Test
    public void testSetInvalidIndex() throws Exception {
        try (BooleanFileBasedArray array = BooleanFileBasedArray.create(file, 10)) {
            assertThrows(IndexOutOfBoundsException.class, () -> array.set(-1, true));
            assertThrows(IndexOutOfBoundsException.class, () -> array.set(10, true));
        }
    }

    @Test
    public void testGetInvalidLength() throws Exception {
        try (BooleanFileBasedArray array = BooleanFileBasedArray.create(file, 10)) {
            assertThrows(NegativeArraySizeException.class, () -> array.get(0, -1));
            assertThrows(IndexOutOfBoundsException.class, () -> array.get(0, 11));
            assertThrows(IndexOutOfBoundsException.class, () -> array.get(5, 6));
        }
    }
}
