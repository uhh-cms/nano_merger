# coding: utf-8

import os
import math
import random
import uuid
import re
import urllib
from collections import defaultdict

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


class CrabOutputUser(DatasetTask):

    def get_crab_outputs(self, **kwargs):
        # define the base directory that is queried
        remote_dir = law.wlcg.WLCGDirectoryTarget(
            self.dataset_config["miniAOD"].split("/")[1],
            fs=self.wlcg_fs_source,
        )

        outputs = []

        # loop through timestamps, corresponding to the normal and the recovery jobs
        assert self.dataset_config["timestamps"]
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

                # loop through directory content and loop
                for name in numbered_dir.listdir(**kwargs):
                    outputs.append(numbered_dir.child(name, type=kwargs.get("type")))

        return outputs


class UnpackFiles(CrabOutputUser):

    def output(self):
        return self.remote_target("mapping.json")

    def run(self):
        # mapping from tar file name to contained root files
        contents = defaultdict(list)

        # loop through crab outputs
        crab_outputs = self.get_crab_outputs(pattern="*.tar", type="f")

        def unpack_and_upload(target):
            # unpack into tmp dir
            tmp = law.LocalDirectoryTarget(is_tmp=True)
            tmp.touch()
            target.load(tmp, formatter="tar")

            # loop through unpacked root files
            for name in tmp.listdir(pattern="*.root", type="f"):
                # fill mapping
                contents[target.basename].append(name)

                # upload
                target.sibling(name, type="f").copy_from_local(tmp.child(name, type="f"))

        def callback(i):
            self.publish_message(f"handle file {i + 1} / {len(crab_outputs)}")

        law.util.map_verbose(
            unpack_and_upload,
            crab_outputs,
            every=5,
            callback=callback,
        )

        # save the file info
        self.output().dump(contents, indent=4, formatter="json")


class GatherFiles(CrabOutputUser):

    def requires(self):
        return UnpackFiles.req(self)

    def output(self):
        return self.remote_target("files.json")

    def run(self):
        # collect files
        files = []
        for target in self.get_crab_outputs(pattern="*.root", type="f"):
            files.append({
                "path": target.path,
                "size": target.stat().st_size,
                "remote_base": self.dataset_config["remoteBase"],
            })

        # save the file info
        self.output().dump(files, indent=4, formatter="json")


class ComputeMergingFactor(DatasetTask):

    target_size = law.BytesParameter(
        default=2048.0,
        unit="MB",
        description="the target size of the merged file; default unit is MB; default: 2048MB",
    )

    # compression = ("ZSTD", 9)
    compression = ("ZSTD", 1)

    def requires(self):
        return GatherFiles.req(self)

    def output(self):
        return self.remote_target(f"factor_{self.target_size}MB.json")

    def run(self):
        # compute the merging factor
        files = self.input().load(formatter="json")
        size_files = sum([file["size"] for file in files]) / (1024 ** 2)
        size_files *= compression_ratios.get(self.compression, 1.0)
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


class MergeFiles(DatasetTask, law.LocalWorkflow, HTCondorWorkflow):

    target_size = ComputeMergingFactor.target_size

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

        # don't cache by default, but only if requirements exist
        self._cache_branches = False

    def create_branch_map(self):
        reqs = self.workflow_requires()
        if not reqs["factor"].complete():
            return [None]

        self._cache_branches = True
        n_files = len(reqs["files"].output().load(formatter="json"))
        merging_factor = reqs["factor"].output().load(formatter="json")["merging_factor"]

        return list(law.util.iter_chunks(n_files, merging_factor))

    def workflow_requires(self):
        return {
            "files": GatherFiles.req(self),
            "factor": ComputeMergingFactor.req(self),
        }

    def requires(self):
        return {
            "files": GatherFiles.req(self),
            "factor": ComputeMergingFactor.req(self),
        }

    def output(self):
        lfn = self.create_lfn(self.branch).lstrip("/")
        size_str = f"merged_{self.target_size}MB"
        lfn_target = lambda lfn: law.wlcg.WLCGFileTarget(os.path.join(size_str, lfn), fs=self.wlcg_fs_target)

        return {
            "events": lfn_target(lfn),
            "stats": lfn_target(f"{lfn[:-5]}.json"),
        }

    def run(self):
        # define inputs
        inputs = [
            law.wlcg.WLCGFileTarget(f["path"], fs=self.wlcg_fs_source)
            for i, f in enumerate(self.input()["files"].load(formatter="json"))
            if i in self.branch_data
        ]

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
                inputs,
                every=5,
                callback=callback,
            )

        # start merging events into the localized output
        outputs = self.output()
        with outputs["events"].localize("w", cache=False) as tmp_out:
            compr, level = ComputeMergingFactor.compression
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

            # determine the number of events
            law.contrib.load("root")
            with tmp_out.load(formatter="root") as tfile:
                tree = tfile.Get("Events")
                n_events = int(tree.GetEntries()) if tree else 0

        # save the stats
        outputs["stats"].dump({"n_events": n_events}, indent=4, formatter="json")

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

    def htcondor_output_postfix(self):
        postfix1 = super().htcondor_output_postfix()
        postfix2 = f"{self.target_size}MB"
        return f"{postfix1}_{postfix2}" if postfix1 else postfix2

    def htcondor_destination_info(self, info):
        return info + [self.dataset]


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
            law.wlcg.WLCGFileTarget(data["path"], fs=self.wlcg_fs_source)
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
            for i, path in enumerate(paths, start=1):
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
                f"number of events ({n_events_total}) of dataset {self.dataset} does not " +
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
        col = self.input()["collection"]

        # get the number of files and events
        n_files = len(col)
        n_events = sum(
            inp["stats"].load(formatter="json")["n_events"]
            for inp in col.targets.values()
        )

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
