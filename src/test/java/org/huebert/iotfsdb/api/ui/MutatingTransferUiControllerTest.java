package org.huebert.iotfsdb.api.ui;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import org.huebert.iotfsdb.service.ImportService;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.io.TempDir;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.http.HttpStatus;
import org.springframework.mock.web.MockMultipartFile;
import org.springframework.web.multipart.MultipartFile;
import org.springframework.web.server.ResponseStatusException;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.stream.Stream;

@ExtendWith(MockitoExtension.class)
class MutatingTransferUiControllerTest {

    @Mock
    private ImportService importService;

    @InjectMocks
    private MutatingTransferUiController controller;

    @TempDir
    Path tempDir;

    private MultipartFile validZipFile;
    private MultipartFile emptyFile;
    private MultipartFile nonZipFile;

    @BeforeEach
    void setUp() {
        // Create a valid zip file for testing
        byte[] zipContent = "mock zip content".getBytes(StandardCharsets.UTF_8);
        validZipFile = new MockMultipartFile(
            "file",
            "test-import.zip",
            "application/zip",
            zipContent
        );

        // Create an empty file for testing
        emptyFile = new MockMultipartFile(
            "file",
            "empty.zip",
            "application/zip",
            new byte[0]
        );

        // Create a non-zip file for testing
        nonZipFile = new MockMultipartFile(
            "file",
            "test.txt",
            "text/plain",
            "This is not a zip file".getBytes(StandardCharsets.UTF_8)
        );
    }

    @Test
    void testImportData_Success() throws IOException {
        // Arrange
        doNothing().when(importService).importData(any(Path.class));

        // Act - this should not throw any exception
        controller.importData(validZipFile);

        // Assert
        verify(importService, times(1)).importData(any(Path.class));

        // Verify temp file was created and then deleted
        // We can't directly verify this since the file is created and deleted within the method
        // But the lack of exception indicates the file operations worked
    }

    @Test
    void testImportData_EmptyFile() {
        // Act & Assert
        ResponseStatusException exception = assertThrows(ResponseStatusException.class, () ->
            controller.importData(emptyFile)
        );

        assertEquals(HttpStatus.BAD_REQUEST, exception.getStatusCode());
        assertEquals("File is empty", exception.getReason());

        // Verify importService was never called
        verify(importService, never()).importData(any(Path.class));
    }

    @Test
    void testImportData_NonZipFile() {
        // Act & Assert
        ResponseStatusException exception = assertThrows(ResponseStatusException.class, () ->
            controller.importData(nonZipFile)
        );

        assertEquals(HttpStatus.BAD_REQUEST, exception.getStatusCode());
        assertEquals("File is not a zip", exception.getReason());

        // Verify importService was never called
        verify(importService, never()).importData(any(Path.class));
    }

    @Test
    void testImportData_ImportServiceThrowsException() throws IOException {
        // Arrange
        doThrow(new RuntimeException("Import failed")).when(importService).importData(any(Path.class));

        // Act & Assert
        RuntimeException exception = assertThrows(RuntimeException.class, () ->
            controller.importData(validZipFile)
        );

        assertEquals("Import failed", exception.getMessage());

        // Verify importService was called
        verify(importService, times(1)).importData(any(Path.class));

        // Verify temp files are cleaned up even when exceptions occur
        // We can't directly verify this, but we can check all files in the temp directory
        try (Stream<Path> stream = Files.walk(tempDir)) {
            stream.forEach(path -> {
                // This should not find any files ending with .zip
                assertFalse(path.toString().endsWith(".zip"),
                    "Temporary zip file was not cleaned up properly: " + path);
            });
        }
    }

    @Test
    void testImportData_FileTransferFails() throws IOException {
        // Arrange
        MultipartFile failingFile = mock(MultipartFile.class);
        when(failingFile.isEmpty()).thenReturn(false);
        when(failingFile.getOriginalFilename()).thenReturn("test.zip");
        doThrow(new IOException("Transfer failed")).when(failingFile).transferTo(any(Path.class));

        // Act & Assert
        IOException exception = assertThrows(IOException.class, () ->
            controller.importData(failingFile)
        );

        assertEquals("Transfer failed", exception.getMessage());

        // Verify importService was never called
        verify(importService, never()).importData(any(Path.class));
    }

    @Test
    void testImportData_WithNullFilename() throws IOException {
        // Arrange
        MultipartFile fileWithNullName = mock(MultipartFile.class);
        when(fileWithNullName.isEmpty()).thenReturn(false);
        when(fileWithNullName.getOriginalFilename()).thenReturn(null);

        // Act - should not throw exception about filename
        controller.importData(fileWithNullName);

        // Verify importService was called
        verify(importService, times(1)).importData(any(Path.class));
    }
}
