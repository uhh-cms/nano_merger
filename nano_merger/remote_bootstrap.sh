#!/usr/bin/env bash

# Bootstrap file that is executed in remote jobs submitted by law to set up the environment.
# So-called render variables, denoted by "{{name}}", are replaced with variables configured in the
# remote workflow tasks, e.g. in HTCondorWorkflow.htcondor_job_config() upon job submission.

# Bootstrap function for standalone htcondor jobs.
bootstrap_htcondor_standalone() {
    # set env variables
    export NM_ON_HTCONDOR="1"
    export NM_REMOTE_JOB="1"
    export NM_BASE="{{nm_base}}"
    [ ! -z "{{vomsproxy_file}}" ] && export X509_USER_PROXY="${PWD}/{{vomsproxy_file}}"

    # source the default repo setup
    source "${NM_BASE}/setup.sh" "" || return "$?"

    return "0"
}

# job entry point
bootstrap_{{nm_bootstrap_name}} "$@"
