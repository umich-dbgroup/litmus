#!/bin/bash

db=$1

for i in {1..5}
do
rm cache/${db}_greedyall.pkl
python -u main.py $db greedyall --email=chrisjbaik@gmail.com | tee ${db}_greedyall.out
service mysql restart

rm cache/${db}_greedybb_range.pkl
python -u main.py $db greedybb --info=range --email=chrisjbaik@gmail.com | tee ${db}_greedybb.out
service mysql restart

rm cache/${db}_greedyfirst_range.pkl
python -u main.py $db greedyfirst --info=range --email=chrisjbaik@gmail.com | tee ${db}_greedyfirst.out
service mysql restart

rm cache/${db}_topw.pkl
python -u main.py $db topw --email=chrisjbaik@gmail.com | tee ${db}_topw.out
service mysql restart

rm cache/${db}_l1s.pkl
python -u main.py $db l1s --email=chrisjbaik@gmail.com | tee ${db}_l1s.out
service mysql restart
done
