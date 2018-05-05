#!/bin/bash
source namespace.sh
dev=${intel545}
trace=/root/tracefiles/new/seqr.trace
result_dir=/root/newtest/result/newsamsung3.rcd
test_time=100
sleep_list=(0 50 100 150 300 600 1000 2000 5000 8000 100000 200000 300000 500000)
for((i=0;i<1;i++))
{
    ((sleep_us=100000000))
    #((sleep_us=${sleep_list[${i}]}))
    ./TracePlayer ${dev} ${trace} ${result_dir} ${test_time} ${sleep_us}
}
