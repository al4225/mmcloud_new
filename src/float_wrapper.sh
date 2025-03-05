#!/usr/bin/env bash
# Gao Wang and MemVerge Inc.

set -o errexit -o nounset -o pipefail

# For interactive jobs - get gateway IP of job
get_public_ip() {
    local jobid="$1"
    local IP_ADDRESS=""
    while [[ -z "$IP_ADDRESS" ]]; do
        IP_ADDRESS=$("$float_executable" show -j "$jobid" | grep -A 1 portMappings | tail -n 1 | awk '{print $4}' || true)
        if [[ -n "$IP_ADDRESS" ]]; then
            echo "$IP_ADDRESS"
        else
            sleep 1s
        fi
    done
}

# For interactive jobs - get tmate url
get_tmate_session() {
    local jobid="$1"
    local tmate_session=""
    local ssh=""
    local ssh_tmate=""

    echo "[$(date)]: Waiting for the job to execute and retrieve tmate web session (~5min)..."
    while true; do
        url=$("$float_executable" log -j "$jobid" cat stdout.autosave | grep "web session:" | head -n 1 || true)  
        if [[ -n "$url" ]]; then
            tmate_session=$(echo "$url" | awk '{print $3}')
            echo "To access the server, copy this URL in a browser: $tmate_session"
            echo "To access the server, copy this URL in a browser: $tmate_session" > "${jobid}_tmate_session.log"

            ssh=$("$float_executable" log -j "$jobid" cat stdout.autosave | grep "ssh session:" | head -n 1 || true)
            ssh_tmate=$(echo "$ssh" | awk '{print $3,$4}')
            echo "SSH session: $ssh_tmate"
            echo "SSH session: $ssh_tmate" >> "${jobid}_tmate_session.log"
            break
        else
            sleep 60
            echo "[$(date)]: Still waiting for the job to execute..."
        fi
    done
}

# For interactive jobs - get jupyter link and token
get_jupyter_token() {
    local jobid="$1"
    local ip_address="$2"
    local token=""
    local new_url=""

    echo "[$(date)]: Waiting for the job to execute and retrieve Jupyter token (~10min)..."
    while true; do
        url=$("$float_executable" log -j "$jobid" cat stderr.autosave | grep token= | head -n 1 || true)
        no_jupyter=$("$float_executable" log -j "$jobid" cat stdout.autosave | grep "JupyterLab is not available." | head -n 1 || true)

        if [[ $url == *token=* ]]; then
            token=$(echo "$url" | sed -E 's|.*http://[^/]+/(lab\?token=[a-zA-Z0-9]+).*|\1|')
            new_url="http://$ip_address/$token"
            echo "To access the server, copy this URL in a browser: $new_url"
            echo "To access the server, copy this URL in a browser: $new_url" > "${jobid}_jupyter.log"
            break
        elif [[ -n $no_jupyter ]]; then
            echo "[$(date)]: WARNING: No JupyterLab installed. Falling back to tmate session."
            get_tmate_session "$jobid"
            break
        else
            sleep 60
            echo "[$(date)]: Still waiting for the job to generate token..."
        fi
    done
}

dryrun=""
gateway=""
image_vol_size=""
root_vol_size=""
publish=""
dataVolumeOption=""
verbose=""
declare -a float_args=()

while (( "$#" )); do
    case "$1" in
        --float-executable) float_executable="$2"; shift 2;;
        # Basic parameters
        --core) core="$2"; shift 2;;
        --gateway) gateway="$2"; shift 2;;
        --image) image="$2"; shift 2;;
        --mem) mem="$2"; shift 2;;
        --opcenter) opcenter="$2"; shift 2;;
        --publish) publish="$2"; publish="$2"; shift 2;;
        --securityGroup) securityGroup="$2"; shift 2;;
        --vmPolicy) vm_policy="$2"; shift 2;;
        # Volume parameters
        --dataVolumeOption) dataVolumeOption="${2//;/ }"; shift 2;;
        --imageVolSize) image_vol_size="$2"; shift 2;;
        --rootVolSize) root_vol_size="$2"; shift 2;;
        #Script parameters
        --host-script) host_script="$2"; shift 2;;
        --job-script) job_script="$2"; shift 2;;
        # Miscellaneous parameters
        --dryrun) dryrun="$2"; shift 2;;
        --extra-parameters) extra_parameters="${2//;/ }"; shift 2;;
        --env-parameters) env_parameters="${2//;/ }"; shift 2;;
        --ide) ide="${2}"; shift 2;;
        --job-name) job_name="$2"; shift 2;;
        --verbose) verbose=true; shift;;
        *) echo "Unknown parameter passed: $1"; exit 1 ;;
    esac
done

float_args+=(
    "-a" "$opcenter"
    "-i" "$image" "-c" "$core" "-m" "$mem"
    "--vmPolicy" "$vm_policy"
    "--securityGroup" "$securityGroup"
    "--migratePolicy" "[disable=true,evadeOOM=false]"
    "--withRoot"
    "--allowList" "[r5*,r6*,r7*,m*]"
    "-j" "$job_script"
    "--hostInit" "${host_script}"
    "--dirMap" "/mnt/efs:/mnt/efs"
    "-n" "$job_name"
    "${dataVolumeOption}"
    "${env_parameters}"
)

# If image vol size and root vol size not empty, populate float args
if [[ -n  "${image_vol_size}" ]]; then
    float_args+=(
        "--imageVolSize" "${image_vol_size}"
    )
fi

if [[ -n  "${root_vol_size}" ]]; then
    float_args+=(
        "--rootVolSize" "${root_vol_size}"
    )
fi

if [[ -n "${gateway}" ]]; then
    float_args+=(
        "--gateway" "$gateway"
    )
fi

if [[ -n "${publish}" ]]; then
    float_args+=(
        "--publish" "${publish}"
    )
fi

float_args+=(
    "${extra_parameters}"
)

# Reparse arguments to separate them properly
IFS=' ' read -ra float_args_array <<< "${float_args[*]}"

if [[ -n "${verbose}" ]]; then
    echo ""
    echo "#-------------"
    echo "${float_executable} submit ${float_args_array[*]}"
    echo "#-------------"
fi

if [[ ${dryrun} == true ]]; then
    if [[ -n "${verbose}" ]]; then
        echo "Command not submitted because dryrun was requested."
    fi
    exit 0
else
    jobid=$(echo "yes" | "${float_executable}" submit "${float_args_array[@]}" | grep 'id:' | awk -F'id: ' '{print $2}' | awk '{print $1}' || true)
fi

if [[ -z "$jobid" ]]; then
    echo "Error returned from float submission command! Exiting..."
    exit 1
else
    echo ""
    echo "JOB ID: ${jobid}"

    # Wait for the job to execute and retrieve connection info
    case "$ide" in
        batch)
            exit 0
            ;;
        tmate)
            IP_ADDRESS=$(get_public_ip "$jobid")
            get_tmate_session "$jobid"
            ;;
        jupyter|jupyter-lab)
            IP_ADDRESS=$(get_public_ip "$jobid")
            get_jupyter_token "$jobid" "$IP_ADDRESS"
            ;;
        rstudio)
            IP_ADDRESS=$(get_public_ip "$jobid")
            echo "To access RStudio Server, navigate to http://$IP_ADDRESS in your web browser."
            echo "Please give the instance about 5 minutes to start RStudio"
            echo "RStudio Server URL: http://$IP_ADDRESS" > "${jobid}_rstudio.log"
            ;;
        vscode)
            IP_ADDRESS=$(get_public_ip "$jobid")
            echo "To access code-server, navigate to http://$IP_ADDRESS in your web browser."
            echo "Please give the instance about 5 minutes to start vscode"
            echo "code-server URL: http://$IP_ADDRESS" > "${jobid}_code-server.log"
            ;;
        *)
            echo "Unrecognized IDE specified: $ide"
            ;;
    esac

    # Output suspend command
    suspend_command="$float_executable suspend -j $jobid"
    echo "Suspend your environment when you do not need it by running:"
    echo "$suspend_command"
    echo "$suspend_command" >> "${jobid}_${ide}.log"

fi
