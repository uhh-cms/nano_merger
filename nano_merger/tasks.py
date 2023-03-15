# coding: utf-8

import os
import math
import random
import uuid
import re
import urllib

import law

from nano_merger.framework import DatasetTask, DatasetWrapperTask, HTCondorWorkflow


# file size ratios to scale sum of files with default nano root compression to zstd/zlib
compression_ratios = {
    ("ZSTD", 6): 1.24,
    ("ZSTD", 7): 1.16,
    ("ZSTD", 8): 1.13,
    ("ZSTD", 9): 1.08,
    ("ZLIB", 6): 1.31,
    ("ZLIB", 7): 1.29,
    ("ZLIB", 8): 1.27,
    ("ZLIB", 9): 1.26,
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
        default=2048.0,
        unit="MB",
        description="the target size of the merged file; default unit is MB; default: 2048MB",
    )

    compression = ("ZLIB", 9)

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

    def lfn_target(self, *path, fs="wlcg_fs"):
        parts = (f"merged_{self.target_size}MB",) + tuple(map(str, path))
        return law.wlcg.WLCGFileTarget(os.path.join(*parts), fs=fs)

    def htcondor_output_postfix(self):
        postfix = super().htcondor_output_postfix()
        return f"{postfix}_{self.target_size}MB"

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
        main_campaign = re.sub(r"MiniAODv\d+", f"NanoAOD{self.global_config['nanoVersion']}", main_campaign)

        return f"/store/{kind}/{main_campaign}/{full_dataset}/NANOAOD{sim}/{sub_campaign}/0/{_hash}.root"

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
            {
                "events": self.lfn_target(lfn),
                "stats": self.lfn_target(f"{lfn[:-5]}.json"),
            }
            for lfn in (
                self.create_lfn(i).lstrip("/") for i in range(n_outputs)
            )
        ])

    def merge(self, inputs, output):
        # default cwd
        cwd = law.LocalDirectoryTarget(is_tmp=True)
        cwd.touch()

        # fetch event files first into the cwd
        with self.publish_step("fetching inputs ...", runtime=True):
            def fetch(inp):
                inp.copy_to_local(cwd.child(inp.unique_basename, type="f"), cache=False)
                return inp.unique_basename

            def callback(i):
                self.publish_message(f"fetch file {i + 1} / {len(inputs)}")

            bases = law.util.map_verbose(
                fetch,
                inputs if self.is_leaf() else [inp["events"] for inp in inputs],
                every=5,
                callback=callback,
            )

        # start merging events into the localized output
        with output["events"].localize("w", cache=False) as tmp_out:
            compr, level = ComputeMergingFactor.compression
            if not self.is_root:
                level = 1
            with self.publish_step(f"merging with {compr}={level} ...", runtime=True):
                cmd = law.util.quote_cmd([
                    "repack_root_file",
                    "-c", f"{compr}={level}",
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

            # determine the number of events at leaf stage, merge input json for all other stages
            if self.is_leaf():
                # read number of events
                law.contrib.load("root")
                with tmp_out.load(formatter="root") as tfile:
                    tree = tfile.Get("Events")
                    n_events = int(tree.GetEntries()) if tree else 0
            else:
                # merge json files
                n_events = sum(inp["stats"].load(formatter="json")["n_events"] for inp in inputs)

        # save the stats
        output["stats"].dump({"n_events": n_events}, indent=4, formatter="json")


class MergeFilesWrapper(DatasetWrapperTask):

    target_size = ComputeMergingFactor.target_size

    def requires(self):
        return [
            MergeFiles.req(self, dataset=dataset)
            for dataset in self.datasets
        ]


class ValidateEvents(DatasetTask):

    def requires(self):
        return GatherFiles.req(self)

    def output(self):
        return self.local_target("stats.json")

    def run(self):
        # create a list of input targets
        targets = [
            law.wlcg.WLCGFileTarget(
                data["path"],
                fs=f"wlcg_fs_{data['remote_base']}",
            )
            for data in self.input().load(formatter="json")
        ]

        # create a list of local paths pointing to the /pnfs mount
        paths = [
            urllib.parse.urlparse(target.uri()).path
            for target in targets
        ]

        # open all files and extract the number of events
        law.contrib.load("root")
        progress = self.create_progress_callback(len(paths))
        n_events = []
        with self.publish_step(f"opening {len(paths)} files ..."):
            for i, path in enumerate(paths):
                with law.LocalFileTarget(path).load(formatter="root") as tfile:
                    tree = tfile.Get("Events")
                    _n_events = int(tree.GetEntries()) if tree else 0
                n_events.append(_n_events)
                self.publish_message(f"events in / after file {i}: {_n_events} / {sum(n_events)}")
                progress(i)
        n_events_total = sum(n_events)

        # compare with DAS
        das_info = self.get_das_info()
        if n_events_total != das_info["n_events"]:
            raise Exception(
                f"number of merged events ({n_events_total}) of dataset {self.dataset} does not " +
                f"match DAS info ({das_info['n_events']})",
            )

        # save the results
        data = {
            "n_events": n_events_total,
            "per_file": dict(zip(paths, n_events)),
        }
        self.output().dump(data, formatter="json")


class ValidateEventsWrapper(DatasetWrapperTask):

    def requires(self):
        return [
            ValidateEvents.req(self, dataset=dataset)
            for dataset in self.datasets
        ]


class CreateEntry(DatasetTask):

    target_size = ComputeMergingFactor.target_size

    def requires(self):
        return MergeFiles.req(self)

    def output(self):
        return self.local_target(f"entry_{self.target_size}MB.txt")

    def run(self):
        col = self.input()

        # get the number of files and events
        n_files = len(col)
        n_events = sum(inp["stats"].load(formatter="json")["n_events"] for inp in col.targets)

        # get das info
        das_info = self.get_das_info()

        # compare number of events
        if n_events != das_info["n_events"]:
            raise Exception(
                f"number of merged events ({n_events}) of dataset {self.dataset} does not " +
                f"match DAS info ({das_info['n_events']})",
            )

        #
        # start creating the entry
        #

        # DAS-like nano key
        full_dataset, campaign, tier = self.dataset_config["miniAOD"].split("/")[1:]
        campaign = re.sub(r"MiniAODv\d+", f"NanoAOD{self.global_config['nanoVersion']}", campaign)
        tier = re.sub(r"MINI", "NANO", tier)
        nano_key = f"/{full_dataset}/{campaign}/{tier}"

        # extra information
        is_data_extra = ""
        aux_extra = ""
        if self.dataset_config["sampleType"] == "data":
            era = re.match(r"^.+_Run\d{4}([^-]+)-.+$", self.dataset).group(1)
            is_data_extra = """
    is_data=True,"""
            aux_extra = f"""
    aux={{
        "era": "{era}",
    }},"""

        # the actual entry
        entry = f"""
cpn.add_dataset(
    name="THE_NAME",
    id={das_info['dataset_id']},{is_data_extra}
    processes=[procs.THE_PROCESS],
    keys=[
        "{nano_key}",  # noqa
    ],
    n_files={n_files},
    n_events={n_events},{aux_extra}
)
"""

        # print and save it
        self.publish_message(entry)

        self.output().dump(entry, formatter="text")


class CreateEntryWrapper(DatasetWrapperTask):

    target_size = ComputeMergingFactor.target_size

    def requires(self):
        return [
            CreateEntry.req(self, dataset=dataset)
            for dataset in self.datasets
        ]
