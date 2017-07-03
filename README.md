# Spark ETL Demo

There are two sources of data on this demo:
1. CSV archives, ranging from 1 thousand upto 1 million rows
2. PostgreSQL table preloaded with 1 million or 10 millions rows depending on the scenario

A CSV archive represents daily/hourly increments to be joined with corresponding rows in PostgreSQL table, cleaned, and inserted/updated back to PostgreSQL again. However since the goal is to make the join effective, no cleanup neither insertion back to PostgreSQL is covered here.

## Who does it work?

Pretty simple, instead of performing a sequence of lookups to PostgreSQL, a single query is dynamically created for every batch (CSV). It's also known as N+1 queries problem, where all the joining keys read from CSV archive are concatenated leaving a single query that looks like:

`SELECT * FROM person WHERE id IN ('1', '2', ...)`

During reading the RDD corresponding to CSV is (hash) partitioned and cached, since it is used twice, first time for creating the query, and a second one for joining data.

## Hardware
All experiments were executed in:
* Laptop HP EliteBook
* Intel(R) Core(TM) i7-4600U CPU @ 2.10GHz, cache size 4096 KB (4 Cores)
* 16GB RAM
* SSD

### Experiment 1: 10 partitions x 1 Million rows in Pg
| # of rows in CSV  | Time (s)  |
| ----------:|:---------:|
| 1K        | 1.3       |
| 4K        | 1.9       |
| 8K        | 1.9       |
| 16K       | 1.9       |
| 32K       | 3.3       |
| 128K      | 8.7       |
| 256K      | 26.5      |

### Experiment 2: 10 partitions x 8 Million rows in Pg
| # of rows in CSV  | Time (s)  |
| ----------:|:---------:|
| 1K        | 1.3       |
| 4K        | 1.6       |
| 8K        | 1.9       |
| 16K       | 1.8       |
| 32K       | 3.6       |
| 128K      | 9.0       |
| 256K      | 25.7      |

### Experiment 3: 10 partitions x 16 Million rows in Pg
| # of rows in CSV  | Time (s)  |
| ----------:|:---------:|
| 1K        | 1.3       |
| 4K        | 1.6       |
| 8K        | 1.9       |
| 16K       | 1.9       |
| 32K       | 3.5       |
| 128K      | 9.3       |
| 256K      | 25.7      |

