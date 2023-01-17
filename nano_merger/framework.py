# coding: utf-8

"""
Base tasks.
"""

import os

import luigi
import law


law.contrib.load("htcondor", "root", "tasks", "wlcg")


class Task(law.Task):

    def store_parts(self):
        return (self.task_family,)

    def local_target(self, *path):
        parts = ("$NM_BASE", "data") + self.store_parts() + tuple(map(str, path))
        return law.LocalFileTarget(os.path.join(*parts))

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
