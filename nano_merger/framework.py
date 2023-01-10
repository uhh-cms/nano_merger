# coding: utf-8

"""
Base tasks.
"""

import os

import law


law.contrib.load("htcondor", "tasks", "wlcg")


class Task(law.Task):

    def local_parts(self):
        return ("$NM_BASE", "data", self.task_family)

    def local_target(self, *path):
        parts = self.local_parts() + tuple(map(str, path))
        return law.LocalFileTarget(os.path.join(*parts))

    def remote_parts(self):
        return (self.task_family,)

    def remote_target(self, *path, fs="wlcg_fs"):
        parts = self.remote_parts() + tuple(map(str, path))
        return law.wlcg.WLCGFileTarget(os.path.join(*parts), fs=fs)
