# Directory for generated test files.
Files are generated with org.biodatageeks.rangejoins.generation.TestDataGenerator

## Generation parameters
Data is generated semi-randomly with following parameters:

- amount - amount of intervals to generate
- maxOffset - max interval to start generation
- maxRange - max size of interval
- maxStep - max space between intervals

## Datasets
Parameters for generated datasets
1. Denser Labels (expecting less nulls after join)
    - queries [1000, 50, 5, 30]
    - labes [1000, 50, 15, 20]

2. Denser Queries (expecting more nulls after join)
    - queries [1000, 50, 15, 20]
    - labels [1000, 50, 5, 30]