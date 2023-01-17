# coding: utf-8

import os
import math

import law


def find_child_files(config_global, dataset, config_dataset):
    remote_base = config_dataset["remoteBase"]
    store_parts = (
        config_dataset["miniAOD"].split(os.sep)[1],
        f"crab_{dataset}",
        config_dataset["timestamp"],
    )

    base_wlcg = law.wlcg.WLCGDirectoryTarget("/", fs=f"wlcg_fs_{remote_base}")

    # get 0* dirs, if file number exceds 1000 files
    numbered_dirs = base_wlcg.child(os.path.join(*store_parts),
                                    type="d").listdir(pattern="0*", type="d")

    files_infos = []
    for numbered_dir in numbered_dirs:
        # append numbered dir
        numbered_dir = os.path.join(*store_parts, numbered_dir)

        child_wlcg = base_wlcg.child(numbered_dir, type="d")
        root_files = child_wlcg.listdir(pattern="*.root", type="f")
        for root_file in root_files:
            root_wlcg = base_wlcg.child(os.path.join(child_wlcg.path, root_file), type="f")

            size = root_wlcg.stat().st_size
            path = root_wlcg.path
            files_infos.append({"path": path, "size": size, "remote_base": remote_base})
    return files_infos


def calculate_merging_factor(files, target_size_mb=512.0):
    num_files = len(files)
    size_files = sum([file["size"] for file in files]) / (1024 ** 2)
    merging_factor = int(math.ceil(num_files / math.ceil(size_files / target_size_mb)))
    return merging_factor
