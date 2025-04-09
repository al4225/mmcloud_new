#!/usr/bin/env bash
# Gao Wang and MemVerge Inc.

set -o errexit -o nounset -o pipefail

RED='\033[0;31m'
NC='\033[0m' # No Color

# Required values (given by CLI)
opcenter=""
gateway=""
securityGroup=""
efs_ip=""

# Modes
batch_mode=""
interactive_mode=""
oem_admin="" # For updating shared packages in interactive jobs
mount_packages="" # For accessing one's own packages in interactive and batch jobs
oem_packages="" # For accessing shared packages in interactive and batch jobs. Default.

# Global optional values (given by CLI) - some default values given
declare -a ebs_mount=()
declare -a ebs_mount_size=()
declare -a mount_local=()
declare -a mount_remote=()
declare -a mountOpt=()
declare -a env_parameters_array=()
core=2
mem=16
dryrun=false
entrypoint=""
float_executable="float"
image="quay.io/danielnachun/tmate-minimal"
image_vol_size=3
root_vol_size=""
job_name=""
user=""
password=""
vm_policy=""
extra_parameters=""
parallel_commands_given=""
migratePolicy=""

# Batch-specific optional values
job_script=""
job_size=""
cwd="~"
parallel_commands=""
min_cores_per_command=0
min_mem_per_command=0
no_fail="|| { command_failed=1; break; }"
no_fail_parallel="--halt now,fail=1"
declare -a download_local=()
declare -a download_remote=()
declare -a download_include=()
declare -a upload_local=()
declare -a upload_remote=()

# Interactive-specific optional values
ide=""
idle_time=7200
publish=""
publish_set=false
suspend_off=""

# Function to display usage information
usage() {
    echo ""
    echo "Usage: $0 [options]"
    echo "Required Options:"
    echo "  -o, --opcenter <ip>                   Set the OP IP address"
    echo "  -sg, --securityGroup <group>          Set the security group"
    echo "  -g, --gateway <id>                    Set gateway"
    echo "  -efs <ip>                             Set EFS IP"
    echo ""

    echo "Required Batch Options:"
    echo "  --job-script <file>                   Main job script to be run on MMC."                          
    echo "  --job-size <value>                    Set the number of commands per job for creating virtual machines."
    echo ""

    echo "Batch-specific Options:"
    echo "  --cwd <value>                         Define the working directory for the job (default: ~)."
    echo "  --download <remote>:<local>           Download files/folders from S3. Format: <S3 path>:<local path> (space-separated)."
    echo "  --upload <local>:<remote>             Upload folders to S3. Format: <local path>:<S3 path>."
    echo "  --download-include '<value>'          Include certain files for download (space-separated), encapsulate in quotations."
    echo "  --no-fail-fast                        Continue executing subsequent commands even if one fails."
    echo "  --parallel-commands <value>           Set the number of commands to run in parallel (default: CPU value)."
    echo "  --min-cores-per-command <value>       Specify the minimum number of CPU cores required per command."
    echo "  --min-mem-per-command <value>         Specify the minimum amount of memory in GB required per command."
    echo ""

    echo "Required Interactive Options:"
    echo "  -ide, --interactive-develop-env <env> Set the IDE"
    echo ""

    echo "Interactive-specific Options:"
    echo "  --idle <seconds>                      Amount of idle time before suspension. Only works for jupyter instances (default: 7200 seconds)"
    echo "  --suspend-off                         For Jupyter jobs, turn off the auto-suspension feature"
    echo "  -pub, --publish <ports>               Set the port publishing in the form of port:port"
    echo "  --entrypoint <dir>                    Set entrypoint of interactive job - please give Github link"
    echo "  --oem-admin                        Run in admin mode to make changes to shared packages in interactive mode"
    echo ""

    echo "Global Options:"
    echo "  -u, --user <username>                 Set the username"
    echo "  -p, --password <password>             Set the password"
    echo "  -i, --image <image>                   Set the Docker image"
    echo "  -c <min>:<optional max>               Specify the exact number or a range of CPUs to use."
    echo "  -m <min>:<optional max>               Specify the exact amount or a range of memory to use (in GB)."
    echo "  --mount-packages                      Grant the ability to use user packages in interactive mode"
    echo "  --oem-packages                        Grant the ability to use shared packages in interactive mode"
    echo "  -vp, --vmPolicy <policy>              Set the VM policy"
    echo "  -ivs, --imageVolSize <size>           Set the image volume size"
    echo "  -rvs, --rootVolSize <size>            Set the root volume size"
    echo "  --ebs-mount <folder>=<size>           Mount an EBS volume to a local directory. Format: <local path>=<size>. Size in GB (space-separated)."
    echo "  --mount <s3_path:vm_path>             Add S3:VM mounts, separated by spaces"
    echo "  --mountOpt <value>                    Specify mount options for the bucket (required if --mount is used) (space-separated)."
    echo "  --env <variable>=<value>              Specify additional environmental variables (space-separated)."
    echo "  -jn, --job-name <name>                Set the job name (batch jobs will have a number suffix)"
    echo "  --float-executable <path>             Set the path to the float executable (default: float)"
    echo "  --dryrun                              Execute a dry run, printing commands without running them."
    echo "  -h, --help                            Display this help message"
}

# Parse command line options
while (( "$#" )); do
  case "$1" in
        -o|--opcenter) opcenter="$2"; shift 2;;
        -u|--user) user="$2"; shift 2;;
        -p|--password) password="$2"; shift 2;;
        -i|--image) image="$2"; shift 2;;
        -sg|--securityGroup) securityGroup="$2"; shift 2;;
        -g|--gateway) gateway="$2"; shift 2;;
        -c|--core) core="$2"; parallel_commands="$2"; shift 2;;
        -m|--mem) mem="$2"; shift 2;;
        -pub|--publish) publish="$2"; publish="$2"; publish_set=true; shift 2;;
        -efs) efs_ip="$2"; shift 2;;
        -vp|--vmPolicy) vm_policy="$2"; shift 2;;
        -ivs|--imageVolSize) image_vol_size="$2"; shift 2;;
        -rvs|--rootVolSize) root_vol_size="$2"; shift 2;;
        -ide|--interactive-develop-env) ide="$2"; interactive_mode="true"; shift 2;;
        -jn|--job-name) job_name="$2"; shift 2;;
        --float-executable) float_executable="$2"; shift 2;;
        --entrypoint) entrypoint="$2"; shift 2;;
        --idle) idle_time="$2"; shift 2;;
        --suspend-off) suspend_off=true; shift ;;
        --oem-admin) oem_admin=true; shift ;;
        --mount-packages) mount_packages=true; shift ;;
        --oem-packages) oem_packages=true; shift ;;
        --dryrun) dryrun=true; shift ;;
        --parallel-commands) parallel_commands="$2"; parallel_commands_given=true; shift 2;;
        --min-cores-per-command) min_cores_per_command="$2"; shift 2;;
        --min-mem-per-command) min_mem_per_command="$2"; shift 2;;
        --job-script) job_script="$2"; batch_mode="true"; shift 2;;
        --job-size) job_size="$2"; shift 2;;
        --cwd) cwd="$2"; shift 2;;
        --no-fail-fast) no_fail="|| true"; no_fail_parallel="--halt never || true" ; shift ;;
        --ebs-mount)
            shift
            while [ $# -gt 0 ] && [[ $1 != -* ]]; do
                IFS='=' read -ra PARTS <<< "$1"
                ebs_mount+=("${PARTS[0]}") 
                ebs_mount_size+=("${PARTS[1]}")
                shift
            done
            ;;
        --download-include|--mountOpt|--env)
            current_flag="$1"
            shift
            while [ $# -gt 0 ] && [[ $1 != -* ]]; do
                IFS='' read -ra ARG <<< "$1"
                if [ "$current_flag" == "--download-include" ]; then
                    download_include+=("${ARG[0]// /|}")
                elif [ "$current_flag" == "--mountOpt" ]; then
                    mountOpt+=("${ARG[0]}")
                elif [ "$current_flag" == "--env" ]; then
                    env_parameters_array+=("${ARG[0]}")
                fi
                shift
            done
            ;;
        --mount|--download|--upload)
            current_flag="$1"
            shift
            while [ $# -gt 0 ] && [[ $1 != -* ]]; do
                IFS=':' read -ra PARTS <<< "$1"
                if [ "$current_flag" == "--mount" ]; then
                    mount_local+=("${PARTS[1]}")
                    mount_remote+=("${PARTS[0]}")
                elif [ "$current_flag" == "--download" ]; then
                    download_remote+=("${PARTS[0]}")
                    download_local+=("${PARTS[1]}")
                elif [ "$current_flag" == "--upload" ]; then
                    upload_local+=("${PARTS[0]}")
                    upload_remote+=("${PARTS[1]}")
                fi
                shift
            done
            ;;
        -h|--help) usage; exit 0 ;;
        -*)  # Unsupported flags
            extra_parameters+="$1"  # Add the unsupported flag to extra_parameters
            shift  # Move past the flag
            # We expect the user to understand float cli commands if using this option
            # Therefore, all unsupported flags will be added to the end of the float command as they are
            # Add all subsequent arguments until the next flag to extra_parameters
            while [ $# -gt 0 ] && ! [[ "$1" =~ ^- ]]; do
                extra_parameters+="$1"
                shift
            done
            ;;
        *) echo "Unknown parameter passed: $1"; usage; exit 1 ;;
    esac
done

check_missing_params() {
    # Check for missing params
    local missing_params=""
    local is_missing=false

    if [ -z "$opcenter" ]; then
        missing_params+="-o, "
        is_missing=true
    fi
    if [ -z "$gateway" ]; then
        missing_params+="-g, "
        is_missing=true
    fi
    if [ -z "$securityGroup" ]; then
        missing_params+="-sg, "
        is_missing=true
    fi
    if [ -z "$efs_ip" ]; then
        missing_params+="-efs, "
        is_missing=true
    fi

    if [[ ${batch_mode} == "true" ]]; then
        if [ -z "$job_script" ]; then
            missing_params+="--job-script, "
            is_missing=true
        fi
        if [ -z "$job_size" ]; then
            missing_params+="--job-size, "
            is_missing=true
        fi
        if [[ ${#download_include[@]} -gt ${#download_local[@]} ]]; then
            missing_params+="--download-include (cannot surpass number of --download values), "
            is_missing=true
        fi
    fi

    # Remove trailing comma and space
    missing_params=${missing_params%, }
    if [ "$is_missing" = true ]; then
        echo "Error: Missing required parameters: $missing_params" >&2
        usage
        exit 1
    fi
}

check_conflicting_params() {
    # Already checked if either (but not both or neither) modes are set
    local conflicting_params=""
    local is_conflicting=false

    if [[ -n $oem_admin && (-n "$mount_packages" || -n "$oem_packages") ]]; then
        echo ""
        echo "Error: --oem-admin cannot be used with the other package modes."
        exit 1
    fi
    if [[ -n "$mount_packages" && -n "$oem_packages" ]]; then
        echo ""
        echo "Error: only one of --mount-packages and --oem-packages can be used."
        exit 1
    fi

    # Check if using interactive-specific parameters for batch mode
    if [[ "$batch_mode" == "true" ]]; then
        # If the interactive mode variables are populated, return error
        if [[ -n "$oem_admin" ]]; then
            conflicting_params+="--oem-admin "
            is_conflicting=true
        fi
        if [[ "$idle_time" != 7200 ]]; then
            conflicting_params+="--idle "
            is_conflicting=true
        fi
        if [[ "$suspend_off" != "" ]]; then
            conflicting_params+="--suspend-off "
            is_conflicting=true
        fi
        if [[ "$publish_set" != false ]]; then
            conflicting_params+="-pub,--publish "
            is_conflicting=true
        fi
        if [[ "$entrypoint" != "" ]]; then
            conflicting_params+="--entrypoint "
            is_conflicting=true
        fi
        if [[ "$ide" != "" ]]; then
            conflicting_params+="--ide "
            is_conflicting=true
        fi
        conflicting_params=${conflicting_params%, }
        if [ "$is_conflicting" = true ]; then
            echo ""
            echo "Error: Conflicting parameters for batch mode: $conflicting_params" >&2
            usage
            exit 1
        fi
    fi

    # Check if using batch-specific parameters for interactive mode
    if [[ "$interactive_mode" == "true" ]]; then
        if [[ -n "$job_size" ]]; then
            conflicting_params+="--job-size "
            is_conflicting=true
        fi
        if [[ "$cwd" != "~" ]]; then
            conflicting_params+="--cwd "
            is_conflicting=true
        fi
        if [[ ${#download_local[@]} -ne 0 ]]; then
            conflicting_params+="--download "
            is_conflicting=true
        fi
        if [[ ${#upload_local[@]} -ne 0 ]]; then
            conflicting_params+="--upload "
            is_conflicting=true
        fi
        if [[ ${#download_include[@]} -gt 0 ]]; then
            conflicting_params+="--download-include "
            is_conflicting=true
        fi
        if [[ "$no_fail" != "|| { command_failed=1; break; }" ]]; then
            conflicting_params+="--no-fail-fast "
            is_conflicting=true
        fi
        if [[ -n "$parallel_commands_given" ]]; then
            conflicting_params+="--parallel-commands "
            is_conflicting=true
        fi
        if [[ "$min_cores_per_command" -gt 0 ]]; then
            conflicting_params+="--min-cores-per-command "
            is_conflicting=true
        fi
        if [[ "$min_mem_per_command" -gt 0 ]]; then
            conflicting_params+="--min-mem-per-command "
            is_conflicting=true
        fi
        conflicting_params=${conflicting_params%, }
        if [ "$is_conflicting" = true ]; then
            echo ""
            echo "Error: Conflicting parameters for interactive mode: $conflicting_params" >&2
            usage
            exit 1
        fi

    fi
}

# Check required parameters are given and don't conflict
check_params() {
    # Both `-ide` or `--job-script` cannot be specified
    if [ -n "$ide" ] && [ -n "$job_script" ]; then
        echo ""
        echo "Error: Please specify either an IDE for interactive jobs, or a job script for a batch job."
        exit 1
    # However, if neither are specified, we will be in the default tmate interactive job in oem-packages mode
    elif [ -z "$ide" ] && [ -z "$job_script" ]; then
        # It's possible for users to do `--oem-admin` when ide and job script are not defined.
        # If oem-admin is defined, set the right parameters
        if [ -z "$oem_admin" ]; then
            echo ""
            echo "Warning: Neither an IDE nor a job script was specified. Starting interactive tmate job."
            interactive_mode="true"
            ide="tmate"
        else
            echo ""
            echo "Warning: Oem-admin mode was specified without ide or job script. Starting interactive tmate job in oem-admin mode"
            interactive_mode="true"
            ide="tmate"
            oem_admin="true"
        fi
    fi

    check_missing_params
    check_conflicting_params

    # Additional parameter checks
    # Batch and Interactive jobs use the same format to mount buckets
    if [[ ${#mount_local[@]} -ne ${#mountOpt[@]} ]]; then
        # Number of mountOptions > 1 and dne number of buckets
        echo ""
        echo -e "\n[ERROR] If there are multiple mount options, please provide the same number of mount options and same number of buckets\n"
        exit 1
    fi

    # Check for overlapping directories between ebs_mount and mount_local
    if (( ${#ebs_mount[@]} )) && (( ${#mount_local[@]} )); then
        for mount_dir in "${ebs_mount[@]}"; do
            for local_dir in "${mount_local[@]}"; do
                if [ "$mount_dir" == "$local_dir" ]; then
                    echo ""
                    echo "Error: Overlapping directories found in ebs_mount and mount_local: $mount_dir"
                    exit 1
                fi
            done
        done
    fi

    # Check for interactive mode params
    # If none of the modes are given, default to oem-packages mode
    if [[ ${interactive_mode} == true ]]; then
        if [[ -z "$oem_admin" && -z "$mount_packages" && -z "$oem_packages" ]]; then
            echo ""
            echo "Warning: No mode specified for interactive job. Defaulting to oem-packages mode."
            oem_packages=true
        fi
    else
        # Batch mode can also specify either mount-packages or oem-packages
        # However, if neither are given, default to oem-packages
        if [[ -z "$mount_packages" && -z "$oem_packages" ]]; then
            echo ""
            echo "Warning: No mode specified for job. Defaulting to oem-packages mode."
            oem_packages=true
        fi

        # Additional check for --parallel-commands when --min-cores-per-command or --min-mem-per-command is specified
        if [[ "$min_cores_per_command" -gt 0 || "$min_mem_per_command" -gt 0 ]] && [[ "$parallel_commands" -gt 0 ]]; then
            echo ""
            echo "Error: --parallel-commands must be set to 0 for automatic determination when --min-cores-per-command or --min-mem-per-command is specified."
            exit 1
        fi

        # Check for overlapping directories between download_local and mount_local
        if (( ${#download_local[@]} )) && (( ${#mount_local[@]} )); then
            for download_dir in "${download_local[@]}"; do
                for mount_dir in "${mount_local[@]}"; do
                    if [ "$download_dir" == "$mount_dir" ]; then
                        echo ""
                        echo "Error: Overlapping directories found in download_local and mount_local: $download_dir"
                        exit 1
                    fi
                done
            done
        fi
    fi
}

# If it is an interactive job, prompt for user and password if not provided
# If it is a batch job, will check if already logged in
login() {
    local output=""
    local address=""
    echo ""

    output=$($float_executable login --info 2>&1 || true)
    address=$(echo "$output" | grep -o 'address: [0-9.]*' | awk '{print $2}' || true)

    if [ "$address" == "" ]; then
        if [[ -z "$user" ]]; then
            read -rp "Enter user for $opcenter: " user
        fi
        if [[ -z "$password" ]]; then
            read -rsp "Enter password for $opcenter: " password
            echo ""
        fi

        # If error returned by login, exit the script
        if ! "$float_executable" login -a "$opcenter" -u "$user" -p "$password"; then
            echo ""
            echo "Error: Login failed. Please check username and password"
            exit 1
        fi
    elif [ "$opcenter" != "$address" ]; then
        echo -e "\n[ERROR] The provided opcenter address $opcenter does not match the logged in opcenter $address. Please log in to $opcenter."
        exit 1
    else
        # If login was successful, save the float username
        user=$(echo "$output" | grep 'username' | awk '{print $2}')
    fi
}

# Find parent directory of scripts to use the right one
find_script_dir() {
    SOURCE=${BASH_SOURCE[0]}
    while [ -L "$SOURCE" ]; do # resolve $SOURCE until the file is no longer a symlink
        DIR=$( cd -P "$( dirname "$SOURCE" )" >/dev/null 2>&1 && pwd )
        SOURCE=$(readlink "$SOURCE")
        [[ $SOURCE != /* ]] && SOURCE=$DIR/$SOURCE # if $SOURCE was a relative symlink, we need to resolve it relative to the path where the symlink file was located
    done
    DIR=$( cd -P "$( dirname "$SOURCE" )" >/dev/null 2>&1 && pwd )
    echo "${DIR}"
}

mount_buckets() {
    local dataVolume_cmd=""

    # If more than one mount option, we expect there to be the same number of mounted buckets
    # TODO: Make endpoint a configurable setting
    for i in "${!mountOpt[@]}"; do
        dataVolume_cmd+="--dataVolume [${mountOpt[$i]},endpoint=s3.us-east-1.amazonaws.com]s3://${mount_remote[$i]}:${mount_local[$i]} "
    done

    echo -e "${dataVolume_cmd}"
}

mount_volumes() {
  local volumeMount_cmd=""

  for i in "${!ebs_mount[@]}"; do
    local folder="${ebs_mount[i]}"
    local size="${ebs_mount_size[i]}"

    volumeMount_cmd+="--dataVolume [size=$size]:$folder "
  done

  echo -e "${volumeMount_cmd}"
}

# Additional float parameter checks
set_env_parameters() {
    local env_parameters=""

    # Set MODE env variable
    if [[ -n "$mount_packages" ]]; then
        env_parameters_array+=("MODE=mount_packages")
    elif [[ -n "$oem_packages" ]]; then
        env_parameters_array+=("MODE=oem_packages")
    elif [[ -n "$oem_admin" ]]; then
        env_parameters_array+=(
            "MODE=oem_admin"
            "PIXI_HOME=/mnt/efs/shared/.pixi"
        )
    fi

    if [[ ${batch_mode} = true ]]; then
        ide="batch"
    fi

    # Build the float submit command as an array
    env_parameters_array+=(
        "GRANT_SUDO=yes"
        "VMUI=$ide"
        "EFS=$efs_ip"
        "PYDEVD_DISABLE_FILE_VALIDATION=1"
        "JUPYTER_RUNTIME_DIR=/tmp/jupyter_runtime"
        "JUPYTER_ENABLE_LAB=TRUE"
        "ALLOWABLE_IDLE_TIME_SECONDS=$idle_time"
    )

    # If entrypoint provided, add it
    if [[ -n "$entrypoint" ]]; then
        env_parameters_array+=("ENTRYPOINT=$entrypoint")
    fi

    # If suspend_on is empty, suspension feature is on
    # If it is populated, turn off suspension with an env variable
    if [[ "$suspend_off" == "true" ]]; then
        env_parameters_array+=("SUSPEND_FEATURE=false")
    fi

    for param in "${env_parameters_array[@]}"; do
        env_parameters+=" --env $param"
    done

    echo -e "${env_parameters}"
}

# Determine VM policy
determine_vm_policy() {
    local lowercase_vm_policy=""

    if [[ -z $vm_policy ]]; then
        if [[ ${interactive_mode} == "true" ]]; then
            vm_policy_command="[onDemand=true]"
        else
            vm_policy_command="[spotOnly=true,retryInterval=900s]"
        fi
    else
        lowercase_vm_policy=$(echo "$vm_policy" | tr '[:upper:]' '[:lower:]')
        if [ "${lowercase_vm_policy}" == "spotonly" ]; then
            vm_policy_command="[spotOnly=true,retryInterval=900s]"
        elif [ "${lowercase_vm_policy}" == "ondemand" ]; then
            vm_policy_command="[onDemand=true]"
        elif [ "${lowercase_vm_policy}" == "spotfirst" ]; then
            vm_policy_command="[spotFirst=true]"
        else
            echo ""
            echo "Invalid VM Policy setting '$vm_policy'. Please use 'spotOnly', 'onDemand', or 'spotFirst'"
            return 1
        fi
    fi

    echo "${vm_policy_command}"
}

# # # Helper functions for batch jobs # # #
# Determine batch job names
determine_batch_job_name() {
    local job_index=$1
    
    # Set default job_name if not provided by user
    if [[ -z "$job_name" ]]; then
        # Default batch job name is username with numeric suffix
        job_name="${user}_${job_index}"
    else
        # If custom job name provided, append numeric suffix
        job_name="${job_name}_${job_index}"
    fi
    
    echo -e "${job_name}"
}

submit_each_line_with_float() {
    local script_file="$1"

    # Check if the script file exists
    if [ ! -f "$script_file" ]; then
        echo ""
        echo "Script file does not exist: $script_file"
        return 1
    fi

    # Check if the script file is empty
    if [ ! -s "$script_file" ]; then
        echo ""
        echo "Script file is empty: $script_file"
        return 0
    fi

    # Divide the commands into jobs based on job-size
    total_commands=$(grep -cve '^\s*$' "${script_file}")
    num_jobs=$(( (total_commands + job_size - 1) / job_size )) # Ceiling division

    # Loop to create job submission commands
    for (( j = 0; j < num_jobs; j++ )); do
        # Generate path to job script
        if [ "$dryrun" = true ]; then
            full_cmd+="#-------------\n"
            job_filename=${script_file%.*}_"$j".mmjob.sh 
        else
            mkdir -p "${TMPDIR:-/tmp}/${script_file%.*}"
            job_filename="${TMPDIR:-/tmp}/${script_file%.*}/${j}.mmjob.sh"
        fi

        # Using a sliding-window effect, take the next job_size number of jobs
        start=$(((j * job_size) + 1))
        end=$((start + job_size - 1))

        # Begin jobs script with bind_mount.sh
        cat "$script_dir/bind_mount.sh" > "${job_filename}"

        "${script_dir}/generate_job_script.sh" \
            --script_file "${script_file}" \
            --start "${start}" \
            --end "${end}" \
            --cwd "${cwd}" \
            --download-local "$(echo "${download_local[@]}" | tr ' ' ';')" \
            --upload-local "$(echo "${upload_local[@]}" | tr ' ' ';')" \
            --download-remote "$(echo "${download_remote[@]}" | tr ' ' ';')" \
            --download-include "$(echo "${download_include[@]}" | tr ' ' ';')" \
            --upload-remote "$(echo "${upload_remote[@]}" | tr ' ' ';')" \
            --job-filename "${job_filename}" \
            --min-cores-per-command "${min_cores_per_command}" \
            --min-mem-per-command "${min_mem_per_command}" \
            --no-fail "${no_fail}" \
            --no-fail-parallel "${no_fail_parallel}" \
            --parallel-commands "${parallel_commands}"

        # Set batch job name with numeric suffix
        batch_job_name=$(determine_batch_job_name $((j+1)))

        # Submit the job and retrieve job ID
        # Execute or echo the full command
        # publish parameters is deliberately omiteed
        "${script_dir}/float_wrapper.sh" \
            --float-executable "${float_executable}" \
            --opcenter "${opcenter}" \
            --image "${image}" \
            --core "${core}" \
            --mem "${mem}" \
            --securityGroup "${securityGroup}" \
            --vmPolicy "${vm_policy}" \
            --dataVolumeOption "${dataVolume_params// /;};${volumeMount_params// /;}" \
            --imageVolSize "${image_vol_size}" \
            --rootVolSize "${root_vol_size}" \
            --host-script "${host_script}" \
            --job-script "${job_filename}" \
            --dryrun "${dryrun}" \
            --ide "batch" \
            --verbose \
            --env-parameters "${env_parameters// /;}" \
            --extra-parameters "${extra_parameters// /;}" \
            --job-name "${batch_job_name}"

        rm -rf "${TMPDIR:-/tmp}/${script_file%.*}"
    done
}

###########################################

# # # Helper functions for interactive jobs # # #
prompt_user() {
    while true; do
        echo -e "Do you wish to proceed (y/N)? \c"
        read -r input
        input=${input:-n}  # Default to "n" if no input is given
        case $input in
            [yY])
                break
                ;;
            [nN]) 
                # If warning is not accepted, exit the script
                echo "Exiting the script."
                exit 0
                ;;
            *) 
                echo "Invalid input. Please enter 'y' or 'n'."
                ;;
        esac
    done
}

# Validate mode combinations
determine_running_jobs() {
    published_port=$(echo "$publish" | cut -d':' -f1)
    # Allowable combinations: oem-packages and mount-packages
    if [[ -n "$oem_admin" ]]; then
        running_int_jobs=$("${float_executable}" list -a "${opcenter}" -f "status=Executing or status=Floating or status=Suspended or status=Suspending or status=Starting or status=Initializing" | awk '{print $4}' | grep -v -e '^$' -e 'NAME' || true)
        if [[ -n $running_int_jobs ]]; then
            int_job_count=$(echo "$running_int_jobs" | wc -l)
        else
            int_job_count=0
        fi
        if [[ $int_job_count -gt 0 ]]; then
            echo ""
            echo -e "${RED}WARNING: ${NC}There are ${int_job_count} interactive jobs running. Updating packages in the interactive setup could lead to checkpoint failures."
            prompt_user
        fi
    fi

    running_jobs=$("${float_executable}" list -f user="${user}" -f "status=Executing or status=Suspended or status=Suspending or status=Starting or status=Initializing"| awk '{print $4}' | grep -v -e '^$' -e 'NAME' | grep "${user}_${ide}_${published_port}" || true)

    # If there exists executing or suspended jobs that match the ID, warn user
    if [[ -n "$running_jobs" ]]; then
        job_count=$(echo "$running_jobs" | wc -l)    	
        echo -e "${RED}WARNING: ${NC}User ${RED}$user${NC} already has ${job_count} existing interactive jobs under the same ide ${RED}$ide${NC} and port ${RED}$published_port${NC}."

        prompt_user
    fi
}

# Adjust publish port if not set by user and by ide
determine_ports() {
    if [[ "$ide" == "rstudio" ]]; then
        publish="8787:8787"
    elif [[ "$ide" == "vscode" ]]; then
        publish="8989:8989"
    else
        publish="8888:8888"
    fi
    echo -e "${publish}"
}

determine_job_name() {
    # Set default job_name if not provided by user
    published_port=$(echo "$publish" | cut -d':' -f1)
    if [[ -z "$job_name" ]]; then
        # Extract the published port from the publish variable
        job_name="${user}_${ide}_${published_port}"
    # If there is a custom job name, we add identifiers to the end
    else
        job_name+=".${user}_${ide}_${published_port}"
    fi
    echo -e "${job_name}"
}

# Set tmate warning and check valid IDEs
give_tmate_warning () {
    RED='\033[0;31m'
    NC='\033[0m' # No Color

    valid_ides=("tmate" "jupyter" "jupyter-lab" "rstudio" "vscode")
    if [[ ! " ${valid_ides[*]} " =~ ${ide} ]]; then
        echo "Error: Invalid IDE specified. Please choose one of: ${valid_ides[*]}"
        exit 1
    fi

    # If ide is tmate, warn user about initial package setup
    if [ "${ide}" == "tmate" ]; then
        echo ""
        echo -e "${RED}NOTICE:${NC} tmate sessions are primarily designed for initial package configuration.\nFor regular development work, we recommend utilizing a more advanced Integrated Development Environment (IDE)\nvia the -ide option, if you have previously set up an alternative IDE."

        prompt_user
    fi
}

# Submit job
submit_interactive_job() {
    # Submit the job and retrieve job ID
    # Execute or echo the full command
    "${script_dir}/float_wrapper.sh" \
        --float-executable "${float_executable}" \
        --core "${core}" \
        --gateway "${gateway}" \
        --image "${image}" \
        --mem "${mem}" \
        --opcenter "${opcenter}" \
        --publish "${publish}" \
        --securityGroup "${securityGroup}" \
        --vmPolicy "${vm_policy}" \
        --dataVolumeOption "${dataVolume_params// /;};${volumeMount_params// /;}" \
        --imageVolSize "${image_vol_size}" \
        --rootVolSize "${root_vol_size}" \
        --host-script "${host_script}" \
        --job-script "${script_dir}/bind_mount.sh" \
        --migratePolicy "[disable=true,evadeOOM=false]" \
        --dryrun "${dryrun}" \
        --env-parameters "${env_parameters// /;}" \
        --extra-parameters "${extra_parameters// /;}" \
        --ide "${ide}" \
        --job-name "${job_name}" \
        --verbose
}
#################################################

# --- MAIN SECTION ---

# Check required parameters (regardless of batch or interactive)
check_params
login
script_dir=$(find_script_dir)
host_script="${script_dir}/host_init.sh"

# Mount bucket(s) with provided mount options
dataVolume_params=$(mount_buckets)
# Mount volume(s)
volumeMount_params=$(mount_volumes)
# Set env varaibles
env_parameters=$(set_env_parameters)
# Determine VM Policy
# In batch mode, vmPolicy is spotOnly by default
# In interactive mode, vmPolicy is onDemand by default
vm_policy=$(determine_vm_policy)

# Start batch mode if batch_mode is true
if [[ "$batch_mode" == "true" ]]; then
    echo "Starting batch mode..."

    # Submit job
    submit_each_line_with_float "${job_script}"
# Start interactive mode if interactive_mode is true
elif [[ "$interactive_mode" == "true" ]]; then
    echo "Starting interactive mode..."

    # Additional helper functions
    publish=$(determine_ports)
    determine_running_jobs
    job_name=$(determine_job_name)
    give_tmate_warning

    submit_interactive_job
fi
