# iotfsdb

Time series database that leverages the unique properties of IOT to efficiently store and retrieve data.

## Core Concepts

The goal of this database is to have a file system organization of data that improves efficiency of space used and time for retrieval.

The IOT concepts that are leveraged are:
- Regular interval of data usually at minimum of one value per second
- Data comes from sensors of limited resolution

Benefits:
- Quick access of data as it is known exactly where a value is located for a specific time
- No other data in files other than just the data values themselves
- No need to store timestamps as data location corresponds to a timestamp

Drawbacks of the approach:
- Exact moment of the measurement is lost as it is placed in the bucket for the interval it belongs to

The database leverages memory mapped files for fast data access.

### Directory Layout

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

### Partition

The partitioning scheme allows a specific range of time to be represented in a file. The partitioning scheme in combination with the data size, we can jump to a data value for a specific time as we know exactly which file and location in a file a data value resides.
Each partitioned file represents a calendar date range of time.

| Name  | Format     | Example    |
|-------|------------|------------|
| DAY   | `yyyyMMdd` | `20241101` |
| MONTH | `yyyyMM`   | `202411`   |
| YEAR  | `yyyyMM`   | `2024`     |

#### Example

If we want the data value for `2024-12-13T12:34:56`, we don't know where to get it without the partition and the number type.

For these examples, we will use the `INT4` type.

If we use the DAY partition, the file the data value is in is `20241213`. Furthermore, the specific bytes that represent the data value can be found.
If we use the MONTH partition, the file the data value is in is `202412`.
If we use the YEAR partition, the file the data value is in is `2024`.

TODO Show calculations of specific location



### Number Type

Number after name is how many bytes per value is used to store it.
 
| Name   | Minimum | Maximum | Null Value | Resolution (bits) |
|--------| ------- | ------- |------------|-------------------|
| INT1   | -127 | 127 | -128       | 8                 |
| INT8   | -127 | 127 | -128       | 64                |
| FLOAT4 | -127 | 127 | NaN        | 32                |

#### Integer Types

Integer types are signed. Null uses the minimum allowable value for a given type. This means that any attempt to store that specific value will result in null being returned when querying data. 

#### Float Types

Floating points use IEEE format.
Null uses NaN

#### Mapped Types

Mapped types use an integer to store the value and the value is mapped back to the range that is set on the series definitiion.

This results in increased floating point resolution and a reduction of space if the range of data is known.

Mapping defines a minimum and maximum value that the integer values range is mapped into.

### Reducer

`AVERAGE` is the default

| Name             | Description                    | Empty Result |
|------------------|--------------------------------|--------------|
| `AVERAGE`        | Average of the non-null values | `null`       |
| `COUNT`          | Count of non-null values       | `0`           |
| `COUNT_DISTINCT` |                                |              |
| `FIRST`          |                                |              |
| `LAST`           |                                |              |
| `MAXIMUM`        |                                |              |
| `MEDIAN`         |                                |              |
| `MINIMUM`        |                                |              |
| `MODE`           |                                |              |
| `SQUARE_SUM`     |                                |              |
| `SUM`            |                                |              |

List which ones get converted to floating point

BigDecimal vs double math



### Archival

Archiving partitions results in the partition being compressed and read only

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
