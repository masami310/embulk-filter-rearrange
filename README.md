# Rearrange filter plugin for Embulk

A filter plugin for Embulk to rearrange an input row data to multiple output row data.

## Overview

* **Plugin type**: filter

## Configuration

- **columns**: Columns of output data. (array of hash, required)
  - **name**: Name of column.
  - **type**: Type of column.
- **output_data**: Contents of output data. (array of array, required)
  - Each element is specified in column name of the input data.
  - Length of each line of output data is equal to or less than the length of the input data.
- **add_row_number**: If true, add row number to output data. (boolean, optional, default: `false`)
  - Row number starts 0, and it increments in a same input data. If we read next input row data, row number resets to 0.
- **row_number_column_name**: Column name of row number. (string, optional, default: `row_number`)

## Example
### sample input
```
date,item1,value1,item2,value2
2015-08-19 00:00:00,hoge1,100,fuga1,200
2015-08-20 00:00:00,hoge2,300,fuga2,400
2015-08-21 00:00:00,hoge3,500,fuga3,600
```

### sample config
```yaml
filters:
  - type: rearrange
    columns:
      - { name:date, type:timestamp }
      - { name:item, type:string }
      - { name:value, type:long }
    output_data:
      - ["date", "item1", "value1"]
      - ["date", "item2", "value2"]
    add_row_number: true
    row_number_column_name: "\"rownum\""
```

### result
```
+-------------------------+-------------+------------+-------------+
|          date:timestamp | item:string | value:long | rownum:long |
+-------------------------+-------------+------------+-------------+
| 2015-08-19 00:00:00 UTC |       hoge1 |        100 |           0 |
| 2015-08-19 00:00:00 UTC |       fuga1 |        200 |           1 |
| 2015-08-20 00:00:00 UTC |       hoge2 |        300 |           0 |
| 2015-08-20 00:00:00 UTC |       fuga2 |        400 |           1 |
| 2015-08-21 00:00:00 UTC |       hoge3 |        500 |           0 |
| 2015-08-21 00:00:00 UTC |       fuga3 |        600 |           1 |
+-------------------------+-------------+------------+-------------+
```

## TODO
* Write test

## Build

```
$ ./gradlew gem
```
