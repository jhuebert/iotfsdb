package org.huebert.iotfsdb.partition;

import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.Warmup;

import java.util.concurrent.TimeUnit;

@State(Scope.Benchmark)
@OutputTimeUnit(TimeUnit.NANOSECONDS)
public class Float3Benchmark {

    private Float3 float3;

    @Setup
    public void setup() {
        float3 = Float3.fromDouble(Math.PI);
    }

    @Benchmark
    @Fork(value = 1, warmups = 1)
    @Warmup(iterations = 1, time = 5, timeUnit = TimeUnit.SECONDS)
    @BenchmarkMode(Mode.Throughput)
    public double testDoubleValue() {
        return float3.floatValue();
    }

//    @Benchmark
//    @Fork(value = 1, warmups = 1)
//    @Warmup(iterations = 1, time = 5, timeUnit = TimeUnit.SECONDS)
//    @BenchmarkMode(Mode.Throughput)
//    public Float3 testFromDouble() {
//        return Float3.fromDouble(Math.PI);
//    }
}
