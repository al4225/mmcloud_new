#!/usr/bin/env bash
# Gao Wang and MemVerge Inc.

set -o errexit -o nounset -o pipefail

calculate_max_parallel_jobs() {
    # Required minimum resources per job
    min_cores_per_cmd=$1  # Minimum CPU cores required per job
    min_mem_per_cmd=$2    # Minimum memory required per job in GB

    # Available system resources
    available_cores=$(lscpu | grep "CPU(s):" | head -n 1 | awk '{print $2}')  # Total available CPU cores
    available_memory_kb=$(grep MemTotal /proc/meminfo | awk '{print $2}')
    available_memory_gb=$((available_memory_kb / 1024 / 1024))

    # Initialize max_parallel_jobs to default parallen_commands
    max_parallel_jobs=$3
    max_jobs_by_cpu=0
    max_jobs_by_mem=0

    # Calculate the maximum number of jobs based on CPU constraints, if applicable
    if [ -n "$min_cores_per_cmd" ] && [ "$min_cores_per_cmd" -gt 0 ]; then
        max_jobs_by_cpu=$((available_cores / min_cores_per_cmd))
        if [ "$max_jobs_by_cpu" -eq 0 ]; then
            max_jobs_by_cpu=1  # Ensure at least 1 job can run if the division results in 0
        fi
    fi

    # Calculate the maximum number of jobs based on memory constraints, if applicable
    if [ -n "$min_mem_per_cmd" ] && [ "$min_mem_per_cmd" -gt 0 ]; then
        max_jobs_by_mem=$((available_memory_gb / min_mem_per_cmd))
        if [ "$max_jobs_by_mem" -eq 0 ]; then
            max_jobs_by_mem=1  # Ensure at least 1 job can run if the division results in 0
        fi
    fi

    # Determine the maximum number of parallel jobs based on the more restrictive resource (CPU or memory)
    if [ "$max_jobs_by_cpu" -gt 0 ] && [ "$max_jobs_by_mem" -gt 0 ]; then
        max_parallel_jobs=$(( max_jobs_by_cpu < max_jobs_by_mem ? max_jobs_by_cpu : max_jobs_by_mem ))
    elif [ "$max_jobs_by_cpu" -gt 0 ]; then
        max_parallel_jobs=$max_jobs_by_cpu
    elif [ "$max_jobs_by_mem" -gt 0 ]; then
        max_parallel_jobs=$max_jobs_by_mem
    fi

    if [ "$max_parallel_jobs" -eq 0 ]; then
        max_parallel_jobs=1
    fi

    echo -e "${available_cores} ${available_memory_gb} ${max_parallel_jobs}"
}

create_download_commands() {
    IFS=';' read -ra download_local <<< "${download_local_string}"
    IFS=';' read -ra download_include <<< "${download_include_string}"
    IFS=';' read -ra download_remote <<< "${download_remote_string}"

    local download_cmd_string=""

    for i in "${!download_local[@]}"; do
        # If local folder has a trailing slash, we are copying into a folder, therefore we make the folder
        if [[ ${download_local[$i]} =~ /$ ]]; then
            echo "mkdir -p ${download_local[$i]%\/}" >> "${download_mkdir}"
        fi
        download_cmd_string="aws s3 cp s3://${download_remote[$i]} ${download_local[$i]} --recursive"

        # Separate include commands with space
        if [ ${#download_include[@]} -gt 0 ]; then
        # Split by space
        IFS='|' read -ra INCLUDES <<< "${download_include[$i]}"
        if [ ${#INCLUDES[@]} -gt 0 ]; then
            # If an include command is used, we want to make sure we don't include the entire folder
            download_cmd_string+=" --exclude '*'"
        fi
        for j in "${!INCLUDES[@]}"; do
            download_cmd_string+=" --include '${INCLUDES[$j]}'"
        done
        fi
        echo "${download_cmd_string}" >> "${download_cmd}"
    done
}

create_upload_commands() {
    IFS=';' read -ra upload_local <<< "${upload_local_string}"
    IFS=';' read -ra upload_remote <<< "${upload_remote_string}"

    local last_folder=""

    for i in "${!upload_local[@]}"; do
        echo "mkdir -p ${upload_local[$i]%\/}" >> "${upload_mkdir}"
        local upload_folder=${upload_remote[$i]%\/}
        if [[ ${upload_local[$i]} =~ /$ ]]; then
            echo "aws s3 sync ${upload_local[$i]} s3://${upload_folder}" >> "${upload_cmd}"
        else  
            last_folder=$(basename "${upload_local[$i]}")
            echo "aws s3 sync ${upload_local[$i]} s3://${upload_folder}/$last_folder" >> "${upload_cmd}"
        fi
    done
}

calculate_max_parallel_jobs_def=$(declare -f calculate_max_parallel_jobs)
download_local_string=""
upload_local_string=""
temp_dir=$(mktemp -d)
download_mkdir="${temp_dir}/download_mkdir"
download_cmd="${temp_dir}/download_cmd"
upload_mkdir="${temp_dir}/upload_mkdir"
upload_cmd="${temp_dir}/upload_cmd"
touch "${download_cmd}"
touch "${download_mkdir}"
touch "${upload_cmd}"
touch "${upload_mkdir}"

while (( "$#" )); do
    case "$1" in
        --script_file) script_file="$2"; shift 2;;
        --start) start="$2"; shift 2;;
        --end) end="$2"; shift 2;;
        --cwd) cwd="$2"; shift 2;;
        --download-local) download_local_string="$2"; shift 2;;
        --upload-local) upload_local_string="$2"; shift 2;;
        --download-remote) download_remote_string="$2"; shift 2;;
        --upload-remote) upload_remote_string="$2"; shift 2;;
        --download-include) download_include_string="$2"; shift 2;;
        --job-filename) job_filename="$2"; shift 2;;
        --min-cores-per-command) min_cores_per_command="$2"; shift 2;;
        --min-mem-per-command) min_mem_per_command="$2"; shift 2;;
        --no-fail) no_fail="$2"; shift 2;;
        --no-fail-parallel) no_fail_parallel="$2"; shift 2;;
        --parallel-commands) parallel_commands="$2"; shift 2;;
        --*) echo "Unrecognized parameter $1"; exit 1;;
    esac 
done

# Only create download and upload commands if there are corresponding parameters
if [[ -n ${download_local_string} ]]; then
    create_download_commands
fi
if [[ -n ${upload_local_string} ]]; then
    create_upload_commands
fi

eof="EOF"

tee -a "${job_filename}" >/dev/null << EOF
# Function definition for calculate_max_parallel_jobs
${calculate_max_parallel_jobs_def}

# Create directories if they don't exist for download
$(cat "${download_mkdir}")
# Create directories if they don't exist for upload
$(cat "${upload_mkdir}")
# Create directories if they don't exist for cwd
mkdir -p ${cwd}

# Execute the download commands to fetch data from S3
$(cat "${download_cmd}")

# Change to the specified working directory
cd ${cwd}

# Compute parallel command numbers based on runtime values
read available_cores available_memory_gb num_parallel_commands < <(calculate_max_parallel_jobs ${min_cores_per_command} ${min_mem_per_command} ${parallel_commands})
echo "Available CPU cores: \$available_cores"
echo "Available Memory: \$available_memory_gb GB"
echo "Maximum parallel jobs: \$num_parallel_commands"

# Initialize a flag to track command success, which can be changed in no_fail
command_failed=0

# Conditional execution based on num_parallel_commands and also length of commands
cmd_script=\$(mktemp -d)/cmds.sh
tee \${cmd_script} >/dev/null << ${eof}
$(sed '/^\s*$/d' "${script_file}" | sed -n "${start},${end}p")
${eof}

if [[ \$num_parallel_commands -gt 1 && \$(wc -l \${cmd_script}) -gt 1 ]]; then
    parallel -j \$num_parallel_commands ${no_fail_parallel} < \${cmd_script}
else
    bash \${cmd_script} ${no_fail}
fi

# Always execute the upload commands to upload data to S3
$(cat "${upload_cmd}")

# Check if any command failed
if [ \$command_failed -eq 1 ]; then
    exit 1
fi
EOF
