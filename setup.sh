#!/bin/bash

# Setup script for the nano merger project.
# Environment variables are prefixed with "NM_".

action() {
    local shell_is_zsh="$( [ -z "${ZSH_VERSION}" ] && echo "false" || echo "true" )"
    local this_file="$( ${shell_is_zsh} && echo "${(%):-%x}" || echo "${BASH_SOURCE[0]}" )"
    local this_dir="$( cd "$( dirname "${this_file}" )" && pwd )"


    #
    # global variables
    #

    export NM_BASE="${this_dir}"
    export NM_LOCAL_SCHEDULER="${NM_LOCAL_SCHEDULER:-true}"
    export NM_SCHEDULER_HOST="${NM_SCHEDULER_HOST:-naf-cms16.desy.de}"
    export NM_SCHEDULER_PORT="${NM_SCHEDULER_PORT:-8082}"
    export NM_WCLG_USE_CACHE="true"
    export NM_WLCG_CACHE_ROOT="${NM_WLCG_CACHE_ROOT:-/nfs/dust/cms/user/$(whoami)/nm_wlcg_cache}"

    # adjustments for remote jobs
    if [ "${NM_REMOTE_JOB}" = "1" ]; then
        export NM_WCLG_USE_CACHE="false"
        export NM_WLCG_CACHE_ROOT=""
    fi


    #
    # minimal software setup
    #

    source "/afs/desy.de/user/r/riegerma/public/law_sw/setup.sh" "" || return "$?"
    source "/cvmfs/grid.cern.ch/centos7-ui-160522/etc/profile.d/setup-c7-ui-python3-example.sh" "" || return "$?"
    source "/cvmfs/cms.cern.ch/cmsset_default.sh" "" || return "$?"

    # custom path
    export PATH="${NM_BASE}/bin:${PATH}"

    # custom pythonpath
    export PYTHONPATH="${NM_BASE}:${PYTHONPATH}"


    #
    # law setup
    #

    export LAW_HOME="${NM_BASE}/.law"
    export LAW_CONFIG_FILE="${NM_BASE}/law.cfg"

    if which law &> /dev/null; then
        # source law's bash completion scipt
        source "$( law completion )" ""

        # silently index
        law index -q
    fi
}
action "$@"
