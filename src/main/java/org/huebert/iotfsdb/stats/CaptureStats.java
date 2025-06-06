package org.huebert.iotfsdb.stats;


import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * Annotation to capture statistics for a method.
 * This annotation can be used to associate an ID and metadata with a method
 * for tracking or logging purposes.
 */
@Retention(RetentionPolicy.RUNTIME)
@Target(ElementType.METHOD)
public @interface CaptureStats {

    /**
     * A unique identifier for the statistics being captured.
     *
     * @return the ID as a String
     */
    String id();

    /**
     * An array of metadata key-value pairs associated with the statistics.
     *
     * @return an array of Metadata objects
     */
    Metadata[] metadata() default {};

    /**
     * Annotation to define a metadata key-value pair.
     */
    @interface Metadata {

        /**
         * The key of the metadata entry.
         *
         * @return the key as a String
         */
        String key();

        /**
         * The value of the metadata entry.
         *
         * @return the value as a String
         */
        String value();
    }
}
