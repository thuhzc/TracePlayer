#!/bin/bash
source namespace.sh
dev=${samsung}
trace=/root/tracefiles/new/seqr.trace
result_dir=/root/newtest/result/newsamsung3.rcd
test_time=100
../TracePlayer ${dev} ${trace} ${result_dir} ${test_time} 
