#!/bin/bash

db=$1

for i in {1..5}
do
rm cache/${db}_l1s.pkl
python -u main.py $db l1s --email=chrisjbaik@gmail.com | tee ${db}_l1s.out
done
