- Added add'l projection for every constrained column.
- Added correct intended queries if it wasn't produced automatically.
- Manually labeled intended queries. Only have multiple intended queries if output is identical.

## IMDB

https://s3.amazonaws.com/umdb-users/cjbaik/IMDB.sql

- Removed most queries from 118 onward (except 1) from IMDB because it involved nested queries.
- Removed those with empty queries.
- Removed 1 duplicate (when was Kevin Spacey born)
- TODO: Q94, 95, 99, 101, 102 should remove final projection column (but doesn't matter because they're non-SPJ)


## MAS

https://s3.amazonaws.com/umdb-users/cjbaik/mas.sql
