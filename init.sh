#!/bin/sh
for i in `seq 1 1000`; do echo $i | ./10M.txt; done
