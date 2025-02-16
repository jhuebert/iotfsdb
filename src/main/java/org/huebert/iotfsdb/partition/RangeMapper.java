package org.huebert.iotfsdb.partition;

import lombok.Data;

@Data
public class RangeMapper {

    private final boolean constrain;
    private final double decodedMin;
    private final double decodedMax;
    private final double encodedMin;
    private final double encodedMax;
    private final double encodeConversion;
    private final double decodeConversion;

    public RangeMapper(double decodedMin, double decodedMax, double encodedMin, double encodedMax, boolean constrain) {
        this.decodedMin = decodedMin;
        this.decodedMax = decodedMax;
        this.encodedMin = encodedMin;
        this.encodedMax = encodedMax;
        this.constrain = constrain;

        double decodedRange = decodedMax - decodedMin;
        if (decodedRange <= 0.0) {
            throw new IllegalArgumentException("decoded range must be positive");
        }

        double encodedRange = encodedMax - encodedMin;
        if (encodedRange <= 0.0) {
            throw new IllegalArgumentException("encoded range must be positive");
        }

        this.encodeConversion = encodedRange / decodedRange;
        this.decodeConversion = decodedRange / encodedRange;
    }

    public Double encode(double decoded) {
        if (constrain) {
            if (decoded < decodedMin) {
                return encodedMin;
            } else if (decoded > decodedMax) {
                return encodedMax;
            }
        }
        return convert(decoded, decodedMin, encodedMin, encodeConversion);
    }

    public Double decode(double encoded) {
        if (constrain) {
            if (encoded < encodedMin) {
                return decodedMin;
            } else if (encoded > encodedMax) {
                return decodedMax;
            }
        }
        return convert(encoded, encodedMin, decodedMin, decodeConversion);
    }

    private static double convert(double value, double inMin, double outMin, double conversion) {
        return ((value - inMin) * conversion) + outMin;
    }

}
