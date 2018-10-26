#!/bin/bash

db=$1

rm cache/${db}_greedyall.pkl
python -u main.py $db greedyall --email=chrisjbaik@gmail.com | tee ${db}_greedyall.out

rm cache/${db}_greedybb_range.pkl
python -u main.py $db greedybb --info=range --email=chrisjbaik@gmail.com | tee ${db}_greedybb.out

rm cache/${db}_greedyfirst_range.pkl
python -u main.py $db greedyfirst --info=range --email=chrisjbaik@gmail.com | tee ${db}_greedyfirst.out

for i in {1..5}
do
  rm cache/${db}_gav.pkl
  python -u main.py $db gav --email=chrisjbaik@gmail.com | tee ${db}_gav.out
done
