#!/bin/bash

db=$1

rm cache/${db}_greedyall.pkl
python -u main.py $db greedyall --email=chrisjbaik@gmail.com | tee ${db}_greedyall.out

rm cache/${db}_greedybb_range.pkl
python -u main.py $db greedybb --info=range --email=chrisjbaik@gmail.com | tee ${db}_greedybb.out

rm cache/${db}_greedyguess_range.pkl
python -u main.py $db greedyguess --info=range --email=chrisjbaik@gmail.com | tee ${db}_greedyguess.out

for i in {1..5}
do
  echo "rm cache/${db}_random.pkl"
  echo "python -u main.py $db random --email=chrisjbaik@gmail.com | tee ${db}_random.out"
done
