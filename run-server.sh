#!/bin/bash

python3 src/server.py config.txt 0 &
python3 src/server.py config.txt 1 &
python3 src/server.py config.txt 2 &
python3 src/server.py config.txt 3 &
python3 src/server.py config.txt 4 &