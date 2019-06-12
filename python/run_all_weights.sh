#!/bin/bash

for i in {1..5}
do
./run_many_weights.sh yelp 1
./run_many_weights.sh yelp n
./run_many_weights.sh yelp q1
./run_many_weights.sh yelp half
./run_many_weights.sh yelp q3

#./run_many_weights.sh imdb 1
#./run_many_weights.sh imdb n
#./run_many_weights.sh imdb q1
#./run_many_weights.sh imdb half
#./run_many_weights.sh imdb q3

#./run_many_weights.sh mas 1
#./run_many_weights.sh mas n
#./run_many_weights.sh mas q1
#./run_many_weights.sh mas half
#./run_many_weights.sh mas q3

#./run_many_weights.sh mondial 1
#./run_many_weights.sh mondial n
#./run_many_weights.sh mondial q1
#./run_many_weights.sh mondial half
#./run_many_weights.sh mondial q3
done
