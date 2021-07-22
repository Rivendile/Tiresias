#!/bin/bash
placement=("yarn") 
#schedule=("fifo" "fjf" "sjf" "shortest" "shortest-gpu" "dlas" "dlas-gpu")
#schedule=("dlas" "dlas-gpu" "dlas-gpu-100" "dlas-gpu-8" "dlas-gpu-4" "dlas-gpu-2" "dlas-gpu-1" "dlas-gpu-05")
# schedule=("dlas-gpu")
schedule=("shortest" "shortest-gpu" "multi-resource-same" "multi-resource-same-gpu" )
# schedule=("dlas" "dlas-gpu" "multi-resource-same-unaware" "multi-resource-same-gpu-unaware" )
#schedule=("shortest-gpu")
#schedule=("dlas" "dlas-gpu")
# schedule=("dlas-gpu-05")
# schedule=("dlas-gpu-1" "dlas-gpu-2" "dlas-gpu-4" "dlas-gpu-8" "dlas-gpu-10" "dlas-gpu-100" "dlas-gpu-1000")
#schedule=("fifo")

# philly trace
#jobs=("philly_traces_7f04ca" "philly_traces_6214e9" "philly_traces_ee9e8c" "philly_traces_b436b2" "philly_traces_ed69ec" "philly_traces_e13805" "philly_traces_103959" "philly_traces_6c71a0" "philly_traces_2869ce" "philly_traces_11cb48" "philly_traces_0e4a51" )
# jobs=("philly_traces_7f04ca" "philly_traces_ed69ec" "philly_traces_e13805" "philly_traces_2869ce" )

#philly trace - submit at time 0
jobs=("philly_traces_7f04ca_0" "philly_traces_6214e9_0" "philly_traces_ee9e8c_0" "philly_traces_b436b2_0" "philly_traces_ed69ec_0" "philly_traces_e13805_0" "philly_traces_103959_0" "philly_traces_6c71a0_0" "philly_traces_2869ce_0" "philly_traces_11cb48_0" "philly_traces_0e4a51_0" )
# jobs=("philly_traces_7f04ca_0" "philly_traces_ed69ec_0" "philly_traces_e13805_0" "philly_traces_2869ce_0")

setups=("n32g4")
multi_resources=3


for setup in ${setups[@]};do
    cluster_spec="${setup}.csv"
    for job in ${jobs[@]};do
        job_file="trace-data/${job}.csv"
        log_folder="${setup}j${job}"
        mkdir ${log_folder}
        echo ${job}
        for p in ${placement[@]};do
            for s in ${schedule[@]};do
                log_name="${log_folder}/${s}-${p}"
                cmd="python3 run_sim.py --cluster_spec=${cluster_spec} --print --scheme=${p} --trace_file=${job_file} --schedule=${s} --log_path=${log_name}"
                echo ${cmd} 
                python3 run_sim.py --cluster_spec=${cluster_spec} --print --scheme=${p} --trace_file=${job_file} --schedule=${s} --log_path=${log_name} --multi_resource ${multi_resources} >tmp.out
                python3 calc.py ${log_name}
            done
        done
    done
done
