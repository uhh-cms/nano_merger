# coding: utf-8

import law

from nano_merger.framework import Task


class GatherFiles(Task):

    def output(self):
        return self.local_target("files.json")

    def run(self):
        files = [
            {
                "path": "foo",
                "size": 123,
            },
            {
                "path": "bar",
                "size": 456,
            },
        ]
        self.output().dump(files, formatter="json")


class ComputeMergingFactor(Task):

    def requires(self):
        return GatherFiles.req(self)

    def output(self):
        return self.remote_target("factors.json")

    def run(self):
        self.output().dump({"n": 15}, formatter="json")


class MergeFiles(law.tasks.ForestMerge):
    pass
