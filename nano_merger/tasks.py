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

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

        # define the remote directory
        self.remote_dir = law.wlcg.WLCGDirectoryTarget(
            self.dataset_config["miniAOD"].split("/")[1],
            fs=self.wlcg_fs_source,
        )

    def get_crab_outputs(self, **kwargs):
        # define the base directory that is queried

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
            timestamp_dir = self.remote_dir.child(os.path.join(submission_dir, timestamp), type="d")

            # loop through numbered directories
            # numbers = timestamp_dir.listdir(pattern="0*", type="d")
            # def get_outputs(num):
            for num in timestamp_dir.listdir(pattern="0*", type="d"):

                numbered_dir = timestamp_dir.child(num, type="d")

                # loop through directory content and loop
                for name in numbered_dir.listdir(**kwargs):
                    outputs.append(numbered_dir.child(name, type=kwargs.get("type")))
            # def callback(i):
            #     self.publish_message(f"handle dir {i + 1} / {len(numbers)}")

        # law.util.map_verbose(
        #     get_outputs,
        #     numbers,
        #     every=1,
        #     callback=callback,
        # )
        # return sorted(outputs, key=lambda x: x.path)
        return outputs

class GatherCrabOutputs(CrabOutputUser):

    def output(self):
        return self.remote_target("outputs.json")
    
    def run(self):
        # collect files
        outputs = [x.path for x in self.get_crab_outputs(pattern="*.tar", type="f")]

        # save the file info
        self.output().dump(sorted(outputs), indent=4, formatter="json")

class UnpackFiles(CrabOutputUser, law.LocalWorkflow, HTCondorWorkflow):

    cache_branch_map_default = False

    @law.dynamic_workflow_condition
    def workflow_condition(self):
        # declare that the branch map can be built if the workflow requirement exists
        # note: self.input() refers to the outputs of tasks defined in workflow_requires()
        return self.input()["outputs"].exists()

    def workflow_requires(self):
        reqs = super().workflow_requires()
        reqs.update({
            "outputs": GatherCrabOutputs.req(self),
        })
        return reqs

    def requires(self):
        return {
            "outputs": GatherCrabOutputs.req(self),
        }

    @workflow_condition.create_branch_map
    def create_branch_map(self):
        # reqs = self.workflow_requires()
        # if not reqs["outputs"].complete():
        #     return [None]

        self._cache_branch_map = True
        all_craboutputs = self.input()["outputs"].load(formatter="json")

        return list(law.util.iter_chunks(all_craboutputs, 20))
    
    @workflow_condition.output
    def output(self):
        return self.remote_target(f"mapping_{self.branch}.json")

    def run(self):
        # mapping from tar file name to contained root files
        contents = defaultdict(list)

        # loop through crab outputs
        # crab_outputs = self.get_crab_outputs(pattern="*.tar", type="f")
        crab_outputs = self.branch_map[self.branch]

        def unpack_and_upload(target):
            if not isinstance(target, law.wlcg.WLCGFileTarget):
                target = self.remote_dir.child(target, type="f")
            # unpack into tmp dir
            tmp = law.LocalDirectoryTarget(is_tmp=True)
            tmp.touch()
            target.load(tmp, formatter="tar")

            # loop through unpacked root files
            for name in tmp.listdir(pattern="*.root", type="f"):
                # fill mapping
                contents[target.basename].append(name)

                try:
                    # upload
                    output_at_target = target.sibling(name, type="f")
                    output_at_target.copy_from_local(tmp.child(name, type="f"))
                    self.publish_message(f"uploaded {output_at_target.path} with {target.stat().st_size} bytes")
                    
                    if not output_at_target.exists():
                        raise Exception(f"upload failed for {output_at_target.path}")

                except Exception as e:
                    print(e)

        def callback(i):
            self.publish_message(f"handle file {i + 1} / {len(crab_outputs)}")

        law.util.map_verbose(
            unpack_and_upload,
            crab_outputs,
            every=1,
            callback=callback,
        )

        # save the file info
        self.output().dump(contents, indent=4, formatter="json")
    
    def htcondor_output_postfix(self):
        postfix1 = super().htcondor_output_postfix()
        postfix2 = "unpacking"
        return f"{postfix1}_{postfix2}" if postfix1 else postfix2

    def htcondor_destination_info(self, info):
        info["dataset"] = self.dataset
        return info


class CheckJobOutputs(CrabOutputUser, law.LocalWorkflow, HTCondorWorkflow):

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self._dbs_interface = None

    crab_log_dir = law.Parameter(
        description="directory with crab log files; resolved relative to "
        "$NM_BASE",
        # type=str,
    )

    def create_branch_map(self):
        return [self.crab_log_dir]

    # @law.dynamic_workflow_condition
    # def workflow_condition(self):
    #     # declare that the branch map can be built if the workflow requirement exists
    #     # note: self.input() refers to the outputs of tasks defined in workflow_requires()
    #     return self.input()["outputs"].exists()

    def workflow_requires(self):
        reqs = super().workflow_requires()
        reqs.update({
            "crab_outputs": GatherCrabOutputs.req(self),
            "unpacked_mappings": UnpackFiles.req(self, _exclude={"branches"}),
        })
        return reqs

    def requires(self):
        return {
            "crab_outputs": GatherCrabOutputs.req(self),
            "unpacked_mappings": UnpackFiles.req(self, _exclude={"branch"}),
        }

    @property
    def dbs_interface(self):
        if not self._dbs_interface:
            import sys
            nano_prod_path = os.path.expandvars(os.path.join("$NM_BASE", "modules", "NanoProd"))
            if nano_prod_path not in sys.path:
                sys.path.append(nano_prod_path)

            import wlcg_dbs_interface as dbs_helper
            self._dbs_interface = dbs_helper.WLCGInterface()
        return self._dbs_interface

    # @workflow_condition.output
    def output(self):
        return {
            "completion": self.remote_target("check_complete.txt"),
            "not_unpacked": self.remote_target("not_unpacked_ids.json"),
            "broken": self.remote_target("broken_files.json"),
        }

    def run(self):
        # from IPython import embed
        # embed()
        crab_log_dir = self.branch_map[self.branch]
        
        log_dir_base = law.LocalDirectoryTarget(os.path.join("$NM_BASE", crab_log_dir, self.dataset))
        if not log_dir_base.exists():
            raise Exception(f"crab log directory {log_dir_base.abspath} does not exist")

        crab_outputs = self.input()["crab_outputs"].load(formatter="json")
        # this will need some rethinking if there is actually more than one timestamp
        unpacked_file_mappings = list(self.input()["unpacked_mappings"].collection.targets.values())
        # first merge all mappings for easier lookup
        all_mappings = dict()
        for mapping in unpacked_file_mappings:
            all_mappings.update(mapping.load(formatter="json"))

        # get complete list of lfns from interface
        valid_files = self.dbs_interface.load_valid_file_list(self.dataset_config["miniAOD"])

        foo = law.contrib.load("root")
        ROOT = foo.import_ROOT()
        broken_files = defaultdict(dict)
        for recovery_index, timestamp in enumerate(self.dataset_config["timestamps"]):
            if not timestamp:
                continue
            submission_dir = f"crab_{self.dataset}"
            if recovery_index:
                submission_dir = f"{submission_dir}_recovery_{recovery_index}"
            local_log_dir = log_dir_base.child(os.path.join(submission_dir, "local"), type="d")

            if not local_log_dir.exists():
                raise Exception(f"local log directory {local_log_dir.abspath} does not exist")

            crab_mapping_file = local_log_dir.child("job_input_files.json", type="f")
            crab_mapping = crab_mapping_file.load(formatter="json")

            # check whether all outputs were unpacked (needs to be updated to account for different timestamps)
            unpacked_job_ids = set(int(re.match(r".*_(\d+).tar", file_name).group(1)) for file_name in all_mappings.keys())
            crab_job_ids = set([int(x) for x in crab_mapping.keys()])
            not_unpacked_ids = crab_job_ids.symmetric_difference(unpacked_job_ids)

            if len(not_unpacked_ids) > 0:
                # self.publish_message(f"found {len(not_unpacked_ids)} job ids that were not unpacked")
                self.output()["not_unpacked"].dump(list(not_unpacked_ids), indent=4, formatter="json")
                raise Exception(f"found {len(not_unpacked_ids)} job ids that were not unpacked")

            # filter crab outputs for this timestamp
            timestamp_outputs = list(filter(lambda x: timestamp in x, crab_outputs))

            self.publish_message(f"found {len(timestamp_outputs)} crab outputs for timestamp {timestamp}")

            # loop through crab outputs and load unpacked files from mappings
            # for crab_output in timestamp_outputs:
            def compare_counts_per_lfn(crab_output):
                # get the corresponding tar file
                base_dir = os.path.dirname(crab_output)
                file_name = os.path.basename(crab_output)
                processed_events = 0
                for x in all_mappings[file_name]:
                    tmp = os.path.join(base_dir, x)
                    target = self.remote_dir.child(tmp, type="f")
                    with target.load(formatter="root") as tfile:
                        if not all((tfile.IsOpen(), not tfile.IsZombie(), not tfile.TestBit(ROOT.TFile.kRecovered))):
                            raise Exception(f"file {target.abspath} is not open or zombie or recovered")
                        tree = tfile.Get("Events")
                        processed_events += int(tree.GetEntries()) if tree else 0

                job_id = int(re.match(r".*_(\d+).tar", file_name).group(1))

                these_lfns = crab_mapping[str(job_id)]
                if any("logical_file_name" in x.keys() for x in valid_files):
                    # access keys if information is obtained from dbs api
                    lfn_key = "logical_file_name"
                    count_key = "event_count"
                else:
                    # access keys if information is obtained from dasgoclient
                    lfn_key = "name"
                    count_key = "nevents"
                lfns_info = list(filter(lambda x: x[lfn_key] in these_lfns, valid_files))

                original_count = sum([x[count_key] for x in lfns_info])

                if not processed_events == original_count:
                    self.publish_message(f"job {job_id} has {processed_events} events, but should have {original_count}")
                    broken_files[timestamp].update({job_id: {
                        "processed": processed_events,
                        "original": original_count,
                        "lfns": these_lfns,
                    }})

            def callback(i):
                self.publish_message(f"handle file {i + 1} / {len(timestamp_outputs)}")

            law.util.map_verbose(
                compare_counts_per_lfn,
                timestamp_outputs,
                every=5,
                callback=callback,
            )

        if len(broken_files) > 0:
            broken_output = self.output()["broken"]
            broken_output.dump(broken_files, indent=4, formatter="json")
            self.publish_message(f"saved broken files to {broken_output.abspath}")

            raise Exception(f"found {len(broken_files)} broken jobs")

        # if we end up here, everything worked, so touch all files
        for key in self.output().keys():

            self.output()[key].touch()


class GatherFiles(CrabOutputUser):

    def workflow_requires(self):
        reqs = super().workflow_requires()
        reqs.update({
            "unpack": UnpackFiles.req(self),
        })
        return reqs

    def requires(self):
        return {
            "unpack": UnpackFiles.req(self),
        }

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

class TestGatherFiles(GatherFiles):

    def workflow_requires(self):
        reqs = super().workflow_requires()
        reqs.update({
            "unpack": UnpackFiles.req(self),
            "crab_outputs": GatherCrabOutputs.req(self),
        })
        return reqs

    def requires(self):
        return {
            "unpack": UnpackFiles.req(self),
            "crab_outputs": GatherCrabOutputs.req(self),
        }
    
    def run(self):

        unpacked_file_mappings = list(self.input()["unpack"].collection.targets.values())
        crab_outputs = self.input()["crab_outputs"].load(formatter="json")
        
        # first merge all mappings for easier lookup
        all_mappings = dict()
        for mapping in unpacked_file_mappings:
            all_mappings.update(mapping.load(formatter="json"))
        
        # now loop through crab outputs and load unpacked files from mappings
        files = []
        for output in crab_outputs:
            this_dir = os.path.dirname(output)
            this_base = os.path.basename(output)
            for x in all_mappings[this_base]:
                tmp = os.path.join(this_dir, x)
                target = self.remote_dir.child(tmp, type="f")
                files.append({
                    "path": target.path,
                    "size": target.stat().st_size,
                    "remote_base": self.dataset_config["remoteBase"],
                })
        
        self.publish_message(f"extracted {len(files)} files from mappings")
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

    cache_branch_map_default = False

    def create_branch_map(self):
        reqs = self.workflow_requires()
        if not reqs["factor"].complete():
            return [None]

        self._cache_branch_map = True
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
        info["dataset"] = self.dataset
        return info


class MergeFilesWrapper(DatasetWrapperTask):

    target_size = ComputeMergingFactor.target_size

    def requires(self):
        return [
            MergeFiles.req(self, dataset=dataset)
            for dataset in self.datasets
        ]


class ValidateEvents(DatasetTask):

    def requires(self):
        return TestGatherFiles.req(self)

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
        foo = law.contrib.load("root")
        ROOT = foo.import_ROOT()
        progress = self.create_progress_callback(len(paths))
        n_events = []
        with self.publish_step(f"opening {len(paths)} files ..."):
            for i, path in enumerate(paths, start=1):
                with law.LocalFileTarget(path).load(formatter="root") as tfile:
                    if not all((tfile.IsOpen(), not tfile.IsZombie(), not tfile.TestBit(ROOT.TFile.kRecovered))):
                        raise Exception(f"file {path} is not open or zombie or recovered")
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
        return {
        "factor": ComputeMergingFactor.req(self),
        "merged_files": MergeFiles.req(self),
        }

    def output(self):
        return self.local_target(f"entry_{self.target_size}MB.txt")

    def run(self):
        inputs = self.input()

        merging_factor = inputs["factor"]
        col = inputs["merged_files"]["collection"]


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
            era = re.match(r"^.+_Run\d{4}([^-]+).*$", self.dataset).group(1)
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
