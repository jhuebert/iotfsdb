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

| Name       | Minimum                 | Maximum                | Null Value           | Bytes |
|------------|-------------------------|------------------------|----------------------|-------|
| `FLOAT2`   | -65504                  | 65504                  | NaN                  | 2     |
| `FLOAT4`   | -3.4028235E38           | 3.4028235E38           | NaN                  | 4     |
| `FLOAT8`   | -1.7976931348623157E308 | 1.7976931348623157E308 | NaN                  | 8     |
| `INTEGER1` | -127                    | 127                    | -128                 | 1     |
| `INTEGER2` | -32767                  | 32767                  | -32768               | 2     |
| `INTEGER4` | -2147483647             | 2147483647             | -2147483648          | 4     |
| `INTEGER8` | -9223372036854775807    | 9223372036854775807    | -9223372036854775808 | 8     |
| `MAPPED1`  | -3.4028235E38           | 3.4028235E38           | -128                 | 1     |
| `MAPPED2`  | -3.4028235E38           | 3.4028235E38           | -32768               | 2     |
| `MAPPED4`  | -3.4028235E38           | 3.4028235E38           | -2147483648          | 4     |

### Integer Types

`INTEGER1`, `INTEGER2`, `INTEGER4`, `INTEGER8` store signed integer values. Null values are represented as the minimum value for the
base type. This means that any attempt to store that specific value will result in null being
returned when querying data.

### Float Types

`FLOAT2`, `FLOAT4`, `FLOAT8` store floating point values in IEEE-754 format. Null values are represented by `NaN`.
This means that any attempt to store that specific value will result in null being returned when
querying data.

### Mapped Types

`MAPPED1`, `MAPPED2`, `MAPPED4` store floating point values that are backed by the corresponding sized integer
types. This is accomplished by utilizing a minimum and maximum value range that is specified in the
series definition. This range is used the map to and from the floating point range to the
corresponding integer range.

This results in increased floating point resolution and a reduction of space if the range of data is
known. A downside is that reduced resolution results in the saved value not being identical to the
value that is retrieved (see [example](#mapping-example))

#### Mapping Example

**Series Definition**

| Property             | Value               |
|----------------------|---------------------|
| Number Type          | `MAPPED1`           |
| Minimum Value        | `-10.0`             |
| Maximum Value        | `10.0`              |

**Value Mapping**

| Property                   | Value                |
|----------------------------|----------------------|
| Input Value                | `-5.3`               |
| Value mapped to `INTEGER1` | `-67.31`             |
| `INTEGER1` value stored    | `-67`                |
| Restored Value             | `-5.275590551181103` |

## Directory Layout

The database root directory contains only directories where each directory represents a single series.
The series directory name matches the ID the series is created with.

### Series Directory

The series directory name and ID must match the regular expression `[a-z0-9][a-z0-9._-]{0,127}`.
Each series directory contains a `series.json` and period partitioned data files. The `series.json`
file contains the series definition and metadata. The only mutable field in the json is the 
metadata. 

The other files in the series directory are the data files. The name of the files is based on the
partition selected for the series and represent a period of time. The data is stored in sequential
time.

### Example Layout

```
root
├── series-1
│   ├── series.json
│   ├── 2023
│   └── 2024
├── series-2
│   ├── series.json
│   ├── 202306
│   ├── 202307
│   └── 202410
└── series-3
    ├── series.json
    ├── 20230611
    ├── 20230704
    └── 20241006
```

## Partition

The partitioning scheme allows a specific range of time to be represented in a file. Each
partitioned file represents a calendar date range of time. The partitioning scheme in combination
with the type size allows us to jump directly to a data value.

| Name    | Format     | Example    | Description                                                                                   |
|---------|------------|------------|:----------------------------------------------------------------------------------------------|
| `DAY`   | `yyyyMMdd` | `20241101` | Represents 00:00 through 23:59 of a single date                                               |
| `MONTH` | `yyyyMM`   | `202411`   | Represents 00:00 of the first day of the month through 23:59 of the last day of a given month |
| `YEAR`  | `yyyyMM`   | `2024`     | Represents 00:00 of the first day of January through 23:59 of December 31 of a given year     |

### Archival

Partitions can be archived which results in the file data being compressed and the data becomes
read only. Querying data in an archived partition results in the partition being decompressed to a
temporary file. The temporary file is removed after the partition is closed due to being idle.

### Partition Example

#### Series Configuration

| Property         | Value    |
|------------------|----------|
| Partition Period | `MONTH`  |
| Interval (ms)    | `60000`  |
| Number Type      | `FLOAT4` |

We would like to fetch the value for `2024-12-13T12:34:56`. Because we know the partition period is
`MONTH`, we know that the data will be located in a file named `202412`. If that file doesn't exist,
we know the data doesn't exist and is `null`.

Since we know that the data interval for the file is
`60000ms`, we can calculate which bytes the data value is located in from `12:34:56`.
We know that the value has an index of `18034` in the array of `float` values. We multiply that by `4`
to get the byte offset of the first byte of the value in the file - `72136`

##### File Contents

| Byte Index | Stored Value | Timestamp Range Represented                  | Type Representation |
|------------|--------------|----------------------------------------------|---------------------|
| 0          | `0x7FC00000` | `[2024-12-01T00:00:00, 2024-12-01T00:01:00)` | `null`              |
| ...        | ...          | ...                                          | ...                 |
| 72136      | `0x3E9E0652` | `[2024-12-13T12:34:00, 2024-12-13T12:35:00)` | `1.2345679`         |
| ...        | ...          | ...                                          | ...                 |
| XXXX       | `0x7FC00000` | `[2024-12-31T23:59:00, 2025-01-01T00:00:00)` | `null`              |

## API

### Creating Series

Immutable

| Property    | Type              | Description                                                                                                                     | Validation                      | Required |
|-------------|-------------------|---------------------------------------------------------------------------------------------------------------------------------|:--------------------------------|----------|
| `id`        | `String`          | Series ID                                                                                                                       | `[a-z0-9][a-z0-9._-]{0,127}`    | Yes      |
| `type`      | `NumberType`      | Data type of the numbers stored for this series                                                                                 |                                 | Yes      |
| `interval`  | `int`             | Minimum time interval in milliseconds that the series will contain. The interval should exactly divide a day with no remainder. | Value in range of 1 to 86400000 | Yes      |
| `partition` | `PartitionPeriod` | Time period of data contained in a single partition file                                                                        |                                 | Yes      |
| `min`       | `Double`          | Minimum supported value when using a mapped range type. Values to be stored will be constrained to this minimum value.          | Must be smaller than max        | No       |
| `max`       | `Double`          | Maximum supported value when using a mapped range type. Values to be stored will be constrained to this maximum value.          | Must be larger than min         | No       |

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

[Image on Docker Hub](https://hub.docker.com/repository/docker/jhuebert/iotfsdb/general)

```bash
docker run -it -p 8080:8080 -v iotfsdb-data:/data jhuebert/iotfsdb:1
```

#### Docker Compose

```yaml
services:
  iotfsdb:
    image: jhuebert/iotfsdb:1
    volumes:
    - ./data:/data
    ports:
    - 8080:8080
```

### Properties

| Java Property              | Environment Variable       | Description                                                              | Default Value  | Docker Default Value |
|----------------------------|----------------------------|--------------------------------------------------------------------------|----------------|----------------------|
| `iotfsdb.root`             | `IOTFSDB_ROOT`             | Root data directory for the database                                     | `/tmp/iotfsdb` | `/data`              |
| `iotfsdb.read-only`        | `IOTFSDB_READ_ONLY`        | Indicates whether any changes to the database are allowed                | `true`         | `false`              |
| `iotfsdb.max-query-size`   | `IOTFSDB_MAX_QUERY_SIZE`   | Maximum number of values returned for any series query                   | `1000`         | `1000`               |
| `iotfsdb.max-idle-seconds` | `IOTFSDB_MAX_IDLE_SECONDS` | Maximum amount of time to keep a series partition file open after access | `60`           | `60`                 |
