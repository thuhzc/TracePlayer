#!/bin/bash
source namespace.sh
dev=${samsung}
trace=/root/hzc/traces/new/randr60.trace
result_dir=/root/hzc/results/intel545/test.rcd
test_time=30
sleep_list=(0 50 100 150 300 600 1000 2000 5000 8000 100000 200000 300000 500000)
sync=1
for((i=0;i<1;i++))
{
    ((sleep_us=0))
    #((sleep_us=${sleep_list[${i}]}))
    ./TracePlayer ${dev} ${trace} ${result_dir} ${test_time} ${sync} ${sleep_us}
}
