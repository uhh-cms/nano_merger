# coding: utf-8

import math
import random
import uuid

import law

from nano_merger.framework import DatasetTask, DatasetWrapperTask
from nano_merger.util import find_child_files, calculate_merging_factor


class GatherFiles(DatasetTask):

    def output(self):
        return self.remote_target("files.json")

    def run(self):
        files = find_child_files(
            config_global=self.global_config,
            dataset=self.dataset,
            config_dataset=self.dataset_config,
        )

        self.output().dump(files, indent=4, formatter="json")


class ComputeMergingFactor(DatasetTask):

    target_size = law.BytesParameter(
        default=512.0,
        unit="MB",
        description="the target size of the merged file; default unit is MB; default: 512MB",
    )

    def requires(self):
        return GatherFiles.req(self)

    def output(self):
        return self.remote_target(f"factor_{self.target_size}MB.json")

    def run(self):
        files = self.input().load(formatter="json")
        merging_factor = calculate_merging_factor(files=files, target_size_mb=self.target_size)

        self.output().dump({"merging_factor": merging_factor}, indent=4, formatter="json")

        self.publish_message(
            f"dataset {self.dataset}\n"
            f"  inputs : {len(files)}\n"
            f"  merging: {merging_factor}\n"
            f"  outputs: {int(math.ceil(len(files) / merging_factor))}",
        )


class ComputeMergingFactorWrapper(DatasetWrapperTask):

    def requires(self):
        return [
            ComputeMergingFactor.req(self, dataset=dataset)
            for dataset in self.datasets
        ]


class MergeFiles(DatasetTask, law.tasks.ForestMerge):
    # TODO: execute on htcondor

    target_size = ComputeMergingFactor.target_size

    # number of files to merge into a single output
    merge_factor = 8

    @classmethod
    def _req_tree(cls, inst, *args, **kwargs):
        new_inst = super()._req_tree(inst, *args, **kwargs)

        # forward cache values
        new_inst._input_files_from_json = inst._input_files_from_json
        new_inst._merge_factor_from_json = inst._merge_factor_from_json

        return new_inst

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

        # cache for merging info
        self._input_files_from_json = None
        self._merge_factor_from_json = None

    def get_input_files_from_json(self, inp):
        if self._input_files_from_json is None:
            self._input_files_from_json = inp.load(formatter="json")
        return self._input_files_from_json

    def get_merge_factor_from_json(self, inp):
        if self._merge_factor_from_json is None:
            self._merge_factor_from_json = inp.load(formatter="json")["merging_factor"]
        return self._merge_factor_from_json

    def create_lfn(self, index):
        # random, yet deterministic uuidv4 hash
        seed_src = (
            self.dataset_config["miniAOD"],
            self.dataset_config["timestamp"],
            self.target_size,
            index,
        )
        seed = int(law.util.create_hash(seed_src), base=16)
        rnd = random.Random()
        rnd.seed(seed)
        _hash = str(uuid.UUID(int=rnd.getrandbits(128), version=4)).upper()

        # determine lfn parts
        full_dataset, campaign, tier = self.dataset_config["miniAOD"].split("/")[1:]
        is_mc = tier.endswith("SIM")
        sim = "SIM" if is_mc else ""
        kind = "mc" if is_mc else "data"
        main_campaign, sub_campaign = campaign.split("-", 1)

        return f"/store/{kind}/{main_campaign}/{full_dataset}/NANOAOD{sim}/{sub_campaign}/0/{_hash}.root"

    def store_parts(self):
        return (f"merged_to_{self.target_size}MB",)

    def merge_workflow_requires(self):
        return {
            "files": GatherFiles.req(self),
            "factor": ComputeMergingFactor.req(self),
        }

    def trace_merge_workflow_inputs(self, inputs):
        return len(self.get_input_files_from_json(inputs["files"]))

    def merge_requires(self, start_leaf, end_leaf):
        return {
            "files": GatherFiles.req(self),
            "factor": ComputeMergingFactor.req(self),
        }

    def trace_merge_inputs(self, inputs):
        # get file info
        files = self.get_input_files_from_json(inputs["files"])[slice(*self.leaf_range)]

        # map to wlcg file targets
        targets = [
            law.wlcg.WLCGFileTarget(f["path"], fs=f"wlcg_fs_{f['remote_base']}")
            for f in files
        ]

        return targets

    def merge_output(self):
        # return a dummy output as long as the requirements are incomplete
        reqs = self.merge_workflow_requires()
        if not all(req.complete() for req in reqs.values()):
            output = self.local_target("DUMMY_UNTIL_INPUTS_EXIST")
            return self._mark_merge_output_placeholder(output)

        # determine the number of outputs
        n_inputs = len(self.get_input_files_from_json(reqs["files"].output()))
        merge_factor = self.get_merge_factor_from_json(reqs["factor"].output())
        n_outputs = int(math.ceil(n_inputs / merge_factor))

        # define the collection
        return law.SiblingFileCollection([
            self.remote_target(self.create_lfn(i).lstrip("/"))
            for i in range(n_outputs)
        ])

    def merge(self, inputs, output):
        law.root.hadd_task(self, inputs, output)


class MergeFilesWrapper(DatasetWrapperTask):

    def requires(self):
        return [
            MergeFiles.req(self, dataset=dataset)
            for dataset in self.datasets
        ]
