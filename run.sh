#!/bin/sh

#python ./src/process_log.py <input_log.txt> <output_top_ten_hosts.txt> <output_top_ten_resources.txt> <output_top_ten_busy_hour.txt> <output_block_list.txt>

python src/process_log.py    \
 log_input/log.txt \
 log_output/hosts.txt \
 log_output/resources.txt \
 log_output/hours.txt \
 log_output/blocked.txt

