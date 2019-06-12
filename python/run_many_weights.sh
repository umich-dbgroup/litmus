#!/bin/bash

db=$1
tq_rank=$2

rm cache/${db}_greedyall_tq${tq_rank}.pkl
python -u main.py $db greedyall --tq_rank=${tq_rank} --email=chrisjbaik@gmail.com | tee ${db}_greedyall.out
service mysql restart

rm cache/${db}_greedybb_range_tq${tq_rank}.pkl
python -u main.py $db greedybb --info=range --tq_rank=${tq_rank} --email=chrisjbaik@gmail.com | tee ${db}_greedybb.out
service mysql restart

rm cache/${db}_greedyfirst_range${tq_rank}.pkl
python -u main.py $db greedyfirst --info=range --tq_rank=${tq_rank} --email=chrisjbaik@gmail.com | tee ${db}_greedyfirst.out
service mysql restart

rm cache/${db}_topw_tq${tq_rank}.pkl
python -u main.py $db topw --tq_rank=${tq_rank} --email=chrisjbaik@gmail.com | tee ${db}_gav.out
service mysql restart

rm cache/${db}_l1s_tq${tq_rank}.pkl
python -u main.py $db l1s --tq_rank=${tq_rank} --email=chrisjbaik@gmail.com | tee ${db}_gav.out
service mysql restart
