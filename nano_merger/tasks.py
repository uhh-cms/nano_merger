# coding: utf-8

import law
from nano_merger.util import find_child_files, calculate_mergin_factor
from nano_merger.framework import DatasetTask


class GatherFiles(DatasetTask):

    def output(self):
        return self.local_target("files.json")

    def run(self):

        files = find_child_files(
            config_global=self.global_config,
            dataset=self.dataset,
            config_dataset=self.dataset_config)

        from IPython import embed
        embed()
        self.output().dump(files, formatter="json")


class ComputeMergingFactor(DatasetTask):

    def requires(self):
        return GatherFiles.req(self)

    def output(self):
        return self.remote_target("factors.json")

    def run(self):
        target_size = 512
        unit = "mb"

        merging_factor = calculate_mergin_factor(
            files=self.requires().output().load(),
            target_size=target_size,
            unit=unit)

        self.output().dump({"n": merging_factor}, formatter="json")


class MergeFiles(DatasetTask, law.tasks.ForestMerge):
    merge_factor = 8

    def merge_workflow_requires(self):
        req = self.dep_PrepareMLEvents.req(self, _exclude={"branches"})

        # if the merging stats exist, allow the forest to be cached
        self._cache_forest = req.merging_stats_exist

        return req

    def merge_requires(self, start_leaf, end_leaf):
        return [
            self.dep_PrepareMLEvents.req(self, branch=i)
            for i in range(start_leaf, end_leaf)
        ]

    def merge_output(self):

        k = self.ml_model_inst.folds
        return self.target(f"mlevents_f{self.fold}of{k}.parquet")

    def merge(self, inputs, output):
        law.root.hadd_task(self, inputs, output)
