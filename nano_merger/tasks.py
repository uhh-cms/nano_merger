# coding: utf-8

import law
from nano_merger.util import find_child_files, calculate_mergin_factor
from nano_merger.framework import DatasetTask


class GatherFiles(DatasetTask):

    def output(self):
        return self.remote_target("files.json")

    def run(self):
        files = find_child_files(
            config_global=self.global_config,
            dataset=self.dataset,
            config_dataset=self.dataset_config)

        self.output().dump(files, indent=4, formatter="json")


class ComputeMergingFactor(DatasetTask):

    # TODO: make merged_size
    # merged_size = law.BytesParameter(default=512.0, unit="MB")

    def requires(self):
        return GatherFiles.req(self)

    def output(self):
        return self.remote_target("factors.json")

    def run(self):
        target_size = 512
        unit = "mb"

        merging_factor = calculate_mergin_factor(
            files=self.input().load(formatter="json"),
            target_size=target_size,
            unit=unit,
        )

        self.output().dump({"merging_factor": merging_factor}, indent=4, formatter="json")


class MergeFiles(DatasetTask, law.tasks.ForestMerge):
    # TODO: execute on htcondor

    merge_factor = 3

    def merge_workflow_requires(self):
        return {
            "files": GatherFiles.req(self),
            "factor": ComputeMergingFactor.req(self),
        }

    def trace_merge_workflow_inputs(self, inputs):
        return len(inputs["files"].load(formatter="json"))

    def merge_requires(self, start_leaf, end_leaf):
        return {
            "files": GatherFiles.req(self),
            "factor": ComputeMergingFactor.req(self),
        }

    def trace_merge_inputs(self, inputs):
        # get file info
        files = inputs["files"].load(formatter="json")[slice(*self.leaf_range)]

        # map to wlcg file targets
        targets = [
            law.wlcg.WLCGFileTarget(f["path"], fs=f"wlcg_fs_{f['remotebase']}")
            for f in files
        ]

        return targets

    def merge_output(self):
        # TODO: define dynamic outputs once our inputs exist
        return law.SiblingFileCollection([
            self.local_target("merged_0.root"),
            # self.local_target("merged_1.root"),
        ])

    def merge(self, inputs, output):
        law.root.hadd_task(self, inputs, output)
