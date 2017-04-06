# Project Name

Coding challenge for Insight Data Engineering Followship



## Environment and Installation

This program has been developed and tested based on Python 2.7.10 under GCC 4.2.1 Compatible Apple LLVM 6.0 (clang-600.0.39) in OS X Yosemite Version 10.10.5. It's compatiable for most main stream OS platform including MacOS and Linux/Unix. I haven't tested it on Windows, guess it should work :)



In order to run it, simply pull the whole folder into local machine and launch with run.sh script.  



## Argument list



python ./src/process_log.py <input_log.txt> <output_top_ten_hosts.txt> <output_top_ten_resources.txt> <output_top_ten_busy_hour.txt> <output_block_list.txt>



## Design

This program is develeloped following object-oriented design, and each features is wrapped as a indepdent service classes. When program running, each line of data stream will firstly be parsered to a log data structure. Next, this log structure is processed by four classes to achieve different features. Finally each service classes will output result seperatly.   



## Credits

Shanshan Qin