# iotfsdb

Time series database that leverages the unique properties of IOT to efficiently store and retrieve
data.

**Getting Started**

- [Running](#running)
- [Querying Data](#querying-data)

## Goals

* Simple data storage and directory structure
  * Human navigable and updatable
  * Easy backup
  * Easy data pruning
  * Memory mapped files
* Efficient storage of data
  * No timestamp storage
  * Null value support
  * Fast query performance
  * Data compression
* Easy to use
  * REST API
  * Spring Boot
  * Docker Image

### Leveraged IOT Properties

- Regular interval of data
- Interval often limited to one value per second
- Data is of limited resolution

### Drawbacks

- Exact moment of a value is lost as it is placed in a bucket for the interval it belongs to

## Core Concepts

## Number Type

Each series value has a data type that defines how it is stored in a file. The data type defines the range
and type of values supported. Pick the smallest type that represents your series for minimal storage,
faster processing, and quicker retrieval.

The number after each type name indicates how many bytes are used to store each value.

| Name    | Minimum                 | Maximum                | Null Value           | Bytes |
|---------|-------------------------|------------------------|----------------------|-------|
| FLOAT4  | -3.4028235E38           | 3.4028235E38           | NaN                  | 4     |
| FLOAT8  | -1.7976931348623157E308 | 1.7976931348623157E308 | NaN                  | 8     |
| INT1    | -127                    | 127                    | -128                 | 1     |
| INT2    | -32767                  | 32767                  | -32768               | 2     |
| INT4    | -2147483647             | 2147483647             | -2147483648          | 4     |
| INT8    | -9223372036854775807    | 9223372036854775807    | -9223372036854775808 | 8     |
| MAPPED1 | -3.4028235E38           | 3.4028235E38           | -128                 | 1     |
| MAPPED2 | -3.4028235E38           | 3.4028235E38           | -32768               | 2     |
| MAPPED4 | -3.4028235E38           | 3.4028235E38           | -2147483648          | 4     |

### Integer Types

Integer types are signed.
Null uses the minimum allowable value for a given type.
This means that any attempt to store that specific value will result in null being returned when querying data.

### Float Types

Floating points use IEEE format.
Null uses NaN
This means that any attempt to store that specific value will result in null being returned when querying data.

### Mapped Types

Backed by integer types but adds floating point mapping on top using a series defined minimum and maximum value range.
Mapped types use an integer to store the value and the value is mapped back to the range that is set on the series definitiion.

This results in increased floating point resolution and a reduction of space if the range of data is known.

Mapping defines a minimum and maximum value that the integer values range is mapped into.

Benefit being that you get a floating point value with much less storage at the expense of knowing a range of values and losing resolution.
You don't have to mess with converting to a fixed point as this will do that for you.
Half precision IEEE doesn't have many decimal values supported
This supports any value that can be represented in a double

#### Example

| Property             | Value               |
|----------------------|---------------------|
| Number Type          | `MAPPED1`           |
| Minimum Value        | `-10.0`             |
| Maximum Value        | `10.0`              |

| Property             | Value               |
|----------------------|---------------------|
| Input Value          | `5.3`               |
| Input mapped to INT1 | `67.31`             |
| INT1 value stored    | `67`                |
| Restored Value       | `5.275590551181102` |

## Directory Layout

All series stored as directories under root database directory.
Each series directory has a single series.json file that hold the definition and any metadata about the series.
Ideally it isn't edited by hand. The only mutable field in the json is the metadata. The interval could be changed as that only affects the size of new partition files.

The ID must match the regex `[a-z0-9][a-z0-9._-]{0,127}`

The JSON file also includes series metadata.

The series directory name is set to the ID of the series. The series directory name is not important and can be changed without any affect on the database or the results.

The only other files in the series directory are the data files. These files store the data in sequential time.
The name of the files is based on the partition selected for the series and represent a period of time.

Each series can use a different partition scheme.

- root
  - series01
    - series.json
    - 2024
  - series02
    - series.json
    - 20241101

## Partition

The partitioning scheme allows a specific range of time to be represented in a file. The partitioning scheme in combination with the data size, we can jump to a data value for a specific time as we know exactly which file and location in a file a data value resides.
Each partitioned file represents a calendar date range of time.

| Name  | Format     | Example    |
|-------|------------|------------|
| DAY   | `yyyyMMdd` | `20241101` |
| MONTH | `yyyyMM`   | `202411`   |
| YEAR  | `yyyyMM`   | `2024`     |

### Archival

Archiving partitions results in the partition being compressed and read only
Querying data causes the partion to be decompressed to a temporary file that is removed after the partion is closed for being idle

### Examples

If we want the data value for `2024-12-13T12:34:56`, we don't know where to get it without the partition and the number type.

For these examples, we will use the `INT4` type.

If we use the DAY partition, the file the data value is in is `20241213`. Furthermore, the specific bytes that represent the data value can be found.
If we use the MONTH partition, the file the data value is in is `202412`.
If we use the YEAR partition, the file the data value is in is `2024`.

TODO Show calculations of specific location

#### Example 1

##### Series Configuration

| Property          | Value      |
|-------------------|------------|
| Partition Period  | `DAY`      |
| Interval          | `60000`    |
| Number Type       | `INT1`     |
| Parition Filename | `20241213` |

##### File Contents

| Byte Index | Stored Value | Timestamp Range Represented                  | Type Representation |
|------------|--------------|----------------------------------------------|---------------------|
| 0          | `Ox7F`       | `[2024-12-13T00:00:00, 2024-12-13T00:01:00)` | `null`              |
| ...        | ...          | ...                                          | ...                 |
| YYYY       | `Ox07`       | `[2024-12-13T12:34:56, 2024-12-13T12:34:56)` | `7`                 |
| ...        | ...          | ...                                          | ...                 |
| XXXX       | `Ox07`       | `[2024-12-13T23:59:00, 2024-12-14T00:00:00)` | `7`                 |

## API

### Creating Series

Immutable

    @Schema(description = "Series ID")
    @NotBlank
    @Pattern(regexp = "[a-z0-9][a-z0-9._-]{0,127}")
    private String id;

    @Schema(description = "Data type of the numbers stored for this series.")
    @NotNull
    private NumberType type;

    @Schema(description = "Minimum time interval in milliseconds that the series will contain. The interval should exactly divide a day with no remainder.")
    @NotNull
    @Min(1)
    @Max(86400000)
    private int interval;

    @Schema(description = "Time period of data contained in a single partition file.")
    @NotNull
    private PartitionPeriod partition;

    @Schema(description = "Minimum supported value when using a mapped range type. Values to be stored will be constrained to this minimum value.")
    private Double min;

    @Schema(description = "Maximum supported value when using a mapped range type. Values to be stored will be constrained to this maximum value.")
    private Double max;

#### Metadata

Map<String, String>

### Querying Series

    @Schema(description = "Regular expression that is used to match series IDs", defaultValue = ".*")
    @NotNull
    private Pattern pattern = Pattern.compile(".*");

    @Schema(description = "Key and values that matching series metadata must contain")
    @NotNull
    private Map<String, String> metadata = new HashMap<>();

### Inserting Data

Either specific to a series or not
batch

### Querying Data

    @Schema(description = "Earliest date and time that values should be have")
    @NotNull
    private ZonedDateTime from;

    @Schema(description = "Latest date and time that values should be have", defaultValue = "Current date and time")
    @NotNull
    private ZonedDateTime to = ZonedDateTime.now();

    @Schema(description = "Regular expression that is used to match series IDs", defaultValue = ".*")
    @NotNull
    private Pattern pattern = Pattern.compile(".*");

    @Schema(description = "Key and values that series metadata must contain")
    @NotNull
    private Map<String, String> metadata = new HashMap<>();

    @Schema(description = "Interval in milliseconds of the returned data for each series")
    @Positive
    private Integer interval;

    @Schema(description = "Maximum number of points to return for each series")
    @Positive
    private Integer size;

    @Schema(description = "Indicates whether to include null values in the list of values for a series", defaultValue = "false")
    private boolean includeNull = false;

    @Schema(description = "Indicates whether to use BigDecimal for mathematical operations", defaultValue = "false")
    private boolean useBigDecimal = false;

    @Schema(description = "Indicates whether to return the previous non-null value when a null value is encountered", defaultValue = "false")
    private boolean usePrevious = false;

    @Schema(description = "Value to use in place of null in a series", defaultValue = "null")
    private Number nullValue = null;

    @Schema(description = "Reducing function used to produce a single value from a series for a given time period", defaultValue = "AVERAGE")
    @NotNull
    private Reducer timeReducer = Reducer.AVERAGE;

    @Schema(description = "Reducing function used to produce a single value for a given time period for all series. This results in a single series in the response named \"reduced\"")
    private Reducer seriesReducer;

#### Reducer

`AVERAGE` is the default

| Name             | Description                              | Floating Point | Empty Values |
|------------------|------------------------------------------|----------------|--------------|
| `AVERAGE`        | Average of values                        | `true`         | `null`       |
| `COUNT`          | Count of non-null values                 | `false`        | `0`          |
| `COUNT_DISTINCT` | Count of unique non-null values          | `false`        | `0`          |
| `FIRST`          | First non-null value                     | `false`        | `null`       |
| `LAST`           | Last non-null value                      | `false`        | `null`       |
| `MAXIMUM`        | Maximum value                            | `true`         | `null`       |
| `MEDIAN`         | Median value                             | `true`         | `null`       |
| `MINIMUM`        | Minimum value                            | `true`         | `null`       |
| `MODE`           | Most frequently occurring non-null value | `false`        | `null`       |
| `SQUARE_SUM`     | Sum of squares                           | `true`         | `null`       |
| `SUM`            | Sum of values                            | `true`         | `null`       |

List which ones get converted to floating point

BigDecimal vs double math

## Authentication

Out of scope

## Building

```shell
./gradlew build
```

## Running

### Command Line

```shell
java -jar iotfsdb.jar
```

### Docker

[Image](https://hub.docker.com/repository/docker/jhuebert/iotfsdb/general)

```bash
docker run -it -p 8080:8080 -v iotfsdb-data:/data --name iotfsdb jhuebert/iotfsdb:1
```

#### Docker Compose

```yaml
services:
  iotfsdb:
    image: jhuebert/iotfsdb:1
    container_name: iotfsdb
    volumes:
    - ./data:/data
    ports:
    - 8080:8080
    restart: always
```

### Properties

| Java Property              | Environment Variable       | Description                                                              | Default Value  | Docker Default Value |
|----------------------------|----------------------------|--------------------------------------------------------------------------|----------------|----------------------|
| `iotfsdb.root`             | `IOTFSDB_ROOT`             | Root data directory for the database                                     | `/tmp/iotfsdb` | `/data`              |
| `iotfsdb.read-only`        | `IOTFSDB_READ_ONLY`        | Indicates whether any changes to the database are allowed                | `true`         | `false`              |
| `iotfsdb.max-query-size`   | `IOTFSDB_MAX_QUERY_SIZE`   | Maximum number of values returned for any series query                   | `1000`         | `1000`               |
| `iotfsdb.max-idle-seconds` | `IOTFSDB_MAX_IDLE_SECONDS` | Maximum amount of time to keep a series partition file open after access | `60`           | `60`                 |
