# coding: utf-8

import os
import math
import random
import uuid
import re

import law

from nano_merger.framework import DatasetTask, DatasetWrapperTask, HTCondorWorkflow


# file size ratios to scale sum of files with default nano root compression to zstd
compression_ratios = {
    6: 1.24,
    7: 1.16,
    8: 1.13,
    9: 1.08,
}


class GatherFiles(DatasetTask):

    def output(self):
        return self.remote_target("files.json")

    def run(self):
        # define the base directory that is queried
        remote_base = self.dataset_config["remoteBase"]
        remote_dir = law.wlcg.WLCGDirectoryTarget(
            self.dataset_config["miniAOD"].split("/")[1],
            fs=f"wlcg_fs_{remote_base}",
        )

        # loop through timestamps, corresponding to the normal and the recovery jobs
        assert self.dataset_config["timestamps"]
        files = []
        for recovery_index, timestamp in enumerate(self.dataset_config["timestamps"]):
            # skip empty timestamp, pointing to empty submissions
            if not timestamp:
                continue

            # build the submission directory and contained timestamp directory
            submission_dir = f"crab_{self.dataset}"
            if recovery_index:
                submission_dir = f"{submission_dir}_recovery_{recovery_index}"
            timestamp_dir = remote_dir.child(os.path.join(submission_dir, timestamp), type="d")

            # loop through numbered directories
            for num in timestamp_dir.listdir(pattern="0*", type="d"):
                numbered_dir = timestamp_dir.child(num, type="d")

                # loop through root files
                for name in numbered_dir.listdir(pattern="*.root", type="f"):
                    root_file = numbered_dir.child(name, type="f")
                    files.append({
                        "path": root_file.path,
                        "size": root_file.stat().st_size,
                        "remote_base": remote_base,
                    })

        # save the file info
        self.output().dump(files, indent=4, formatter="json")


class ComputeMergingFactor(DatasetTask):

    target_size = law.BytesParameter(
        default=512.0,
        unit="MB",
        description="the target size of the merged file; default unit is MB; default: 512MB",
    )

    compression = 7

    def requires(self):
        return GatherFiles.req(self)

    def output(self):
        return self.remote_target(f"factor_{self.target_size}MB.json")

    def run(self):
        # compute the merging factor
        files = self.input().load(formatter="json")
        size_files = sum([file["size"] for file in files]) / (1024 ** 2)
        size_files *= compression_ratios[self.compression]
        merging_factor = int(math.ceil(len(files) / math.ceil(size_files / self.target_size)))

        # save the file
        self.output().dump({"merging_factor": merging_factor}, indent=4, formatter="json")

        # some useful logs
        self.publish_message(
            f"dataset {self.dataset}\n"
            f"  inputs : {len(files)}\n"
            f"  merging: {merging_factor}\n"
            f"  outputs: {int(math.ceil(len(files) / merging_factor))}",
        )


class ComputeMergingFactorWrapper(DatasetWrapperTask):

    target_size = ComputeMergingFactor.target_size

    def requires(self):
        return [
            ComputeMergingFactor.req(self, dataset=dataset)
            for dataset in self.datasets
        ]


class MergeFiles(DatasetTask, law.tasks.ForestMerge, HTCondorWorkflow):

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
            self.dataset_config["timestamps"],
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
        # replace the "miniAOD" part in the main campaign
        main_campaign = re.sub(r"MiniAODv\d+", f"NanoAODv{self.global_config['nanoVersion']}", main_campaign)

        return f"/store/{kind}/{main_campaign}/{full_dataset}/NANOAOD{sim}/{sub_campaign}/0/{_hash}.root"

    def store_parts(self):
        return (f"merged_{self.target_size}MB",)

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
        # default cwd
        cwd = law.LocalDirectoryTarget(is_tmp=True)
        cwd.touch()

        # fetch files first into the cwd
        with self.publish_step("fetching inputs ...", runtime=True):
            def fetch(inp):
                inp.copy_to_local(cwd.child(inp.unique_basename, type="f"), cache=False)
                return inp.unique_basename

            def callback(i):
                self.publish_message(f"fetch file {i + 1} / {len(inputs)}")

            bases = law.util.map_verbose(fetch, inputs, every=5, callback=callback)

        # start merging into the localized output
        with output.localize("w", cache=False) as tmp_out:
            level = ComputeMergingFactor.compression if self.is_root else 1
            with self.publish_step(f"merging with ZSTD={level} ...", runtime=True):
                cmd = law.util.quote_cmd([
                    "repack_root_file",
                    "-c", f"ZSTD={level}",
                    "-s", "branch",
                    "-o", tmp_out.path,
                    *bases,
                ])
                code = law.util.interruptable_popen(cmd, shell=True, executable="/bin/bash", cwd=cwd.path)[0]
                if code != 0:
                    raise Exception(f"repack_root_file failed with exit code {code}")

            # print the size
            output_size = law.util.human_bytes(tmp_out.stat().st_size, fmt=True)
            self.publish_message(f"merged file size: {output_size}")


class MergeFilesWrapper(DatasetWrapperTask):

    target_size = ComputeMergingFactor.target_size

    def requires(self):
        return [
            MergeFiles.req(self, dataset=dataset)
            for dataset in self.datasets
        ]
