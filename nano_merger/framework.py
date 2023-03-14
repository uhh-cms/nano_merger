# coding: utf-8

"""
Base tasks.
"""

import os
import math

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
        default="samples_2017_uhh.yaml",
        description="config file to load; resolved relative to "
        "$NM_BASE/modules/Framework/config; default: samples_2017_uhh.yaml",
    )

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

        # load the config
        config = self.config
        if not os.path.exists(os.path.expandvars(os.path.expanduser(config))):
            config = os.path.join("$NM_BASE/modules/Framework/config", config)
        data = law.LocalFileTarget(config).load(formatter="yaml")

        # store global config and per dataset configs
        self.global_config = data["GLOBAL"]
        self.datasets_config = {name: cfg for name, cfg in data.items() if name != "GLOBAL"}

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

    def store_parts(self):
        return super().store_parts() + (self.dataset,)

    def htcondor_destination_info(self, info):
        info.append(self.dataset)
        return info


class DatasetWrapperTask(ConfigTask, law.WrapperTask):

    datasets = law.CSVParameter(
        default=("*",),
        description="comma-separated list of datasets to run; default: all",
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
        default=2.0,
        unit="h",
        significant=False,
        description="maximum runtime; default unit is hours; default: 2",
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
        # include the voms proxy
        voms_proxy_file = law.wlcg.get_voms_proxy_file()
        if not law.wlcg.check_voms_proxy_validity(proxy_file=voms_proxy_file):
            raise Exception("voms proxy not valid, submission aborted")
        config.input_files["voms_proxy_file"] = law.JobInputFile(
            voms_proxy_file,
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
        config.custom_content.append(("+MaxRuntime", int(math.floor(self.max_runtime * 3600)) - 1))

        # render variables
        config.render_variables["nm_bootstrap_name"] = "htcondor_standalone"
        config.render_variables["nm_base"] = os.environ["NM_BASE"]

        return config

    def htcondor_use_local_scheduler(self):
        # remote jobs should not communicate with ther central scheduler but with a local one
        return True
