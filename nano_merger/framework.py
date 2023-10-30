# coding: utf-8

"""
Base tasks.
"""

import os
import math
import subprocess
import json

import luigi
import law


law.contrib.load("htcondor", "root", "tasks", "wlcg")


class Task(law.Task):

    def store_parts(self):
        return (self.task_family,)

    def local_target(self, *path, directory=False):
        parts = ("$NM_BASE", "data") + self.store_parts() + tuple(map(str, path))
        cls = law.LocalDirectoryTarget if directory else law.LocalFileTarget
        return cls(os.path.join(*parts))

    def remote_target(self, *path, fs="wlcg_fs"):
        parts = self.store_parts() + tuple(map(str, path))
        return law.wlcg.WLCGFileTarget(os.path.join(*parts), fs=fs)


class ConfigTask(Task):

    config = luigi.Parameter(
        default="Run2_2016_uhh",
        description="directory with config files to load; resolved relative to "
        "$NM_BASE/modules/NanoProd/NanoProd/crab; default: Run2_2016_uhh",
    )

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

        # determine the configuration file directory
        config_dir = self.config
        if not os.path.exists(os.path.expandvars(os.path.expanduser(config_dir))):
            config_dir = os.path.join("$NM_BASE/modules/NanoProd/NanoProd/crab", config_dir)
        config_dir = law.LocalDirectoryTarget(config_dir)

        # open and merge all configs
        data = {}
        for name in config_dir.listdir(pattern="*.yaml", type="f"):
            # skip unused files
            if name.startswith((".", "_")):
                continue
            data.update(config_dir.child(name, type="f").load(formatter="yaml"))

        # store global config and per dataset configs
        self.global_config = data["GLOBAL"]
        self.datasets_config = {name: cfg for name, cfg in data.items() if name != "GLOBAL"}

        # read the exclusion file
        self.exclude_data = {}

    @property
    def wlcg_fs_source(self):
        gc = self.global_config
        dc = self.dataset_config
        return f"wlcg_fs_{gc['era']}_{gc['nanoVersion']}_{dc['remoteBase']}"

    @property
    def wlcg_fs_target(self):
        gc = self.global_config
        return f"wlcg_fs_{gc['era']}_{gc['nanoVersion']}"

    def remote_target(self, *path):
        return super().remote_target(*path, fs=self.wlcg_fs_source)

    def store_parts(self):
        return super().store_parts() + (os.path.splitext(os.path.basename(self.config))[0],)


class DatasetTask(ConfigTask):

    dataset = luigi.Parameter(
        default="GluGluToHHTo2B2Tau",
        description="the dataset to process; default: GluGluToHHTo2B2Tau",
    )

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

        # load the dataset config
        self.dataset_config = self.datasets_config[self.dataset]

        # extract excluded lfns, if not exist use empty tuple to enable "if in check"
        self.exclude_lfns = set(self.dataset_config.get("ignore_miniAOD_LFNs", ()))

    def store_parts(self):
        return super().store_parts() + (self.dataset,)

    def get_das_info(self):
        # build the command
        das_key = self.dataset_config["miniAOD"]
        cmd = f"dasgoclient -query='file dataset={das_key}' -json"

        # run it
        self.publish_message(f"running '{cmd}'")
        code, out, _ = law.util.interruptable_popen(
            cmd,
            shell=True,
            executable="/bin/bash",
            stdout=subprocess.PIPE,
        )
        if code != 0:
            raise Exception("dasgoclient query failed")

        # parse it
        das_info = {"n_events": None, "dataset_id": None}
        n_events = []
        for dbs_info in json.loads(out.strip()):
            dbs_file = dbs_info["file"][0]
            # add event to count if valid and not excluded
            if dbs_file["is_file_valid"] and dbs_file["name"] not in self.exclude_lfns:
                n_events.append(dbs_file["nevents"])
            dataset_id = dbs_file["dataset_id"]

        # store info
        das_info["n_events"] = sum(n_events)
        das_info["dataset_id"] = dataset_id

        return das_info


class DatasetWrapperTask(ConfigTask, law.WrapperTask):

    datasets = law.CSVParameter(
        default=("*",),
        description="comma-separated list of datasets to run; default: all",
        brace_expand=True,
    )

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

        # resolve datasets
        datasets = []
        for dataset in self.datasets_config:
            if law.util.multi_match(dataset, self.datasets, mode=any):
                datasets.append(dataset)
        self.datasets = tuple(datasets)


class HTCondorWorkflow(law.htcondor.HTCondorWorkflow):

    transfer_logs = luigi.BoolParameter(
        default=True,
        significant=False,
        description="transfer job logs to the output directory; default: True",
    )
    max_runtime = law.DurationParameter(
        default=8.0,
        unit="h",
        significant=False,
        description="maximum runtime; default unit is hours; default: 8",
    )
    htcondor_memory = law.BytesParameter(
        default=law.NO_FLOAT,
        unit="MB",
        significant=False,
        description="requested memeory in MB; empty value leads to the cluster default setting; "
        "empty default",
    )
    htcondor_flavor = luigi.ChoiceParameter(
        default="naf",
        choices=("naf", "cern"),
        significant=False,
        description="the 'flavor' (i.e. configuration name) of the batch system; choices: "
        "naf,cern; default: naf",
    )

    exclude_params_branch = {"max_runtime", "htcondor_flavor"}

    def htcondor_output_directory(self):
        # the directory where submission meta data and logs should be stored
        return self.local_target(directory=True)

    def htcondor_bootstrap_file(self):
        # each job can define a bootstrap file that is executed prior to the actual job
        # in order to setup software and environment variables
        return law.JobInputFile(
            os.path.expandvars("$NM_BASE/nano_merger/remote_bootstrap.sh"),
            share=True,
            render_job=True,
        )

    def htcondor_job_config(self, config, job_num, branches):
        vomsproxy_file = law.wlcg.get_vomsproxy_file()
        if not law.wlcg.check_vomsproxy_validity(proxy_file=vomsproxy_file):
            raise Exception("voms proxy not valid, submission aborted")
        config.input_files["vomsproxy_file"] = law.JobInputFile(
            vomsproxy_file,
            share=True,
            render=False,
        )

        # some htcondor setups requires a "log" config, but we can safely set it to /dev/null
        # if you are interested in the logs of the batch system itself, set a meaningful value here
        config.custom_content.append(("log", "/dev/null"))

        # use cc7 at CERN (https://batchdocs.web.cern.ch/local/submit.html)
        if self.htcondor_flavor == "cern":
            config.custom_content.append(("requirements", '(OpSysAndVer =?= "CentOS7")'))

        # same on naf with case insensitive comparison (https://confluence.desy.de/display/IS/BIRD)
        if self.htcondor_flavor == "naf":
            config.custom_content.append(("requirements", '(OpSysAndVer == "CentOS7")'))

        # max runtime
        max_runtime = int(math.floor(self.max_runtime * 3600)) - 1
        config.custom_content.append(("+MaxRuntime", max_runtime))
        config.custom_content.append(("+RequestRuntime", max_runtime))

        # request memory
        if self.htcondor_memory > 0:
            config.custom_content.append(("Request_Memory", self.htcondor_memory))

        # render variables
        config.render_variables["nm_bootstrap_name"] = "htcondor_standalone"
        config.render_variables["nm_base"] = os.environ["NM_BASE"]

        return config

    def htcondor_use_local_scheduler(self):
        # remote jobs should not communicate with ther central scheduler but with a local one
        return True
