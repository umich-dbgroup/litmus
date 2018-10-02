- Added add'l projection for every constrained column.
- Added correct intended queries if it wasn't produced automatically.
- Manually labeled intended queries. Only have multiple intended queries if output is identical.

## IMDB

https://s3.amazonaws.com/umdb-users/cjbaik/IMDB.sql

- Removed most queries from 118 onward (except 1) from IMDB because it involved nested queries.
- Removed those with empty queries.
- Removed 1 duplicate (when was Kevin Spacey born)


## MAS

https://s3.amazonaws.com/umdb-users/cjbaik/mas.sql

- Modified Q65, Q68 because there were no papers with more than 200 citations.
