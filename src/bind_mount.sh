#!/bin/bash
username=$(whoami)
cd "/home/${username}"

link_paths() {
    efs_path=$1
    local_path=$2

    # To link .bashrc and .profile, need to remove the original (not for oem_packages)
    rm "${local_path}/.bashrc" "${local_path}/.profile"
    ln -s "${efs_path}/.bashrc" "${local_path}/.bashrc"
    ln -s "${efs_path}/.profile" "${local_path}/.profile"

    # conda config files
    ln -s "${efs_path}/.condarc" "${local_path}/.condarc"
    ln -s "${efs_path}/.mambarc" "${local_path}/.mambarc"

    # jupyter config files
    ln -s "${efs_path}/.jupyter" "${local_path}/.jupyter"

    # code-server config files
    mkdir -p "${local_path}/.local/share"
    ln -s "${efs_path}/.local/share/code-server" "${local_path}/.local/share/code-server"

    # other config files
    ln -s "${efs_path}/.config" "${local_path}/.config"

    # Software folders
    ln -s "${efs_path}/.pixi" "${local_path}/.pixi"
    ln -s "${efs_path}/micromamba" "${local_path}/micromamba"

    # Only link these directories when not in oem_packages mode
    if [[ ${MODE} != "oem_packages" ]]; then
        # Ipython folder
        ln -s "${efs_path}/.ipython" "${local_path}/.ipython"

        # Cache
        ln -s "${efs_path}/.cache" "${local_path}/.cache"

        # Git repositories
        ln -s "${efs_path}/ghq" "${local_path}/ghq"

        # After installing pixi, it adds the local dir to the PATH through the .bashrc
        # Because we do not want multiple $HOME to front of PATH
        # We need to make a new .bashrc and .profile every time.
        echo -e "Making new .bashrc...\n"
        tee "${efs_path}/.bashrc" << EOF
source \$HOME/.set_paths
unset PYTHONPATH
EOF
        echo -e "Making new .profile...\n"
        tee "${efs_path}/.profile" << EOF
# if running bash
if [ -n "\$BASH_VERSION" ]; then
# include .bashrc if it exists
if [ -f "\$HOME/.bashrc" ]; then
. "\$HOME/.bashrc"
fi
fi
EOF
    fi

    # ln -s ${efs_path}/.mamba ${local_path}/.mamba
    # ln -s ${efs_path}/.conda ${local_path}/.conda

}

set_paths() {
    efs_path=$1

    # Create a PATH script - does not need to be saved in EFS
    # (new every time for easy editing)
    tee "${HOME}/.set_paths" << EOF
export PATH="${HOME}/bin:${efs_path}/.pixi/bin:/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin"
EOF

    source "${efs_path}/.bashrc"
    echo "PATH: ${PATH}"
}

# Link necessary dirs and files
# The efs_path, or first parameter in link_paths and set_paths, is the location of their .bashrc
if [[ ${MODE} == "oem_admin" ]] || [[ ${MODE} == "oem_packages" ]]; then
    # For updating shared packages
    link_paths /mnt/efs/shared "/home/${username}"
    set_paths /mnt/efs/shared
elif [[ ${MODE} == "mount_packages" ]]; then
    # Can NOT access shared packages, but can see user packages. Can install user packages on EFS
    link_paths "/mnt/efs/${FLOAT_USER}" "/home/${username}"
    set_paths "/mnt/efs/${FLOAT_USER}"
else
    echo -e "ERROR: invalid mode specified - must be one of oem_admin, oem_packages or mount_packages"
    exit 1
fi

# Run entrypoint if given
if [[ -n "$ENTRYPOINT" ]]; then
    curl -fsSL "${ENTRYPOINT}" | bash
else
# Else run original VMUI check
  is_available() {
    command -v "$1" &> /dev/null
  }

  # Function to start the terminal server
  start_terminal_server() {
    echo "Starting terminal server ..."
    tmate -F
  }

  # Check if VMUI variable is set
  if [[ -z "${VMUI}" ]]; then
    echo "No UI specified."
    start_terminal_server
    exit 0
  fi

  # Check the value of VMUI and start the corresponding UI
  case "${VMUI}" in
    batch )
        echo "Running batch job, no IDE will be started" ;;
    jupyter|jupyter-lab)
      if is_available jupyter-lab; then
        echo "[$(date)]: JupyterLab is available. Starting JupyterLab ..."
        while true; do
            export JUPYTER_CONFIG_DIR=$HOME/.jupyter
            jupyter-lab
            # Check if jupyter-lab exited with a non-zero exit code
            if [ $? -ne 0 ]; then
                echo "[$(date)]: Jupyter Lab crashed, restarting..."
            else
                echo "[$(date)]: Jupyter Lab exited normally."
                break
            fi
            # Optionally, add a short sleep to avoid immediate retries
            sleep 15s
        done
      else
        echo "JupyterLab is not available."
        start_terminal_server
      fi
      ;;
    rstudio)
      if is_available rserver; then
        echo "RStudio is available. Starting RStudio ..."
        rserver --config-file=${HOME}/.config/rstudio/rserver.conf
      else
        echo "RStudio is not available."
        start_terminal_server
      fi
      ;;
    vscode)
      if is_available code-server; then
        echo "VS Code is available via code-server. Starting ..."
        code-server
      else
        echo "VS Code is not available."
        start_terminal_server
      fi
      ;;
    nvim)
      if is_available nvim; then
        echo "Nvim is available. Starting Nvim ..."
        nvim
      else
        echo "Nvim is not available."
        start_terminal_server
      fi
      ;;
    *)
      echo "Unknown UI specified: ${VMUI}."
      start_terminal_server
      ;;
  esac
fi