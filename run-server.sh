#!/bin/bash

python3 src/server.py config.txt 0 > out0 &
python3 src/server.py config.txt 1 > out1 & 
python3 src/server.py config.txt 2 > out2 &