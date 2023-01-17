import os
import law
import math


class GetFiles:
    def __init__(self, global_config, dataset, dataset_config):
        """
        Args:
            global_config (dict): Description
            dataset (str): name of the dataset that needs to be merged
            dataset_config (dict)
        """
        self.config_global = global_config  # dir you want to merge files
        self.config_dataset = dataset_config  # comes from config
        self.dataset_name = dataset

        self.remote_base = self.config_dataset["remoteBase"]
        self.base_dir = self.base_wlcg()
        self.store_parts = (
            self.config_dataset["miniAOD"].split(os.sep)[1],
            f"crab_{self.dataset_name}",
            self.config_dataset["timestamp"],
        )

    def base_wlcg(self):
        """Create WLCGTargetFile. All files using this object as parent.
        Returns:
            WLCGTargetFile: WLCGTargetFile with remotebase as path.
        """
        file_base_location = "wlcg_fs_" + self.remote_base
        base_dir = law.wlcg.WLCGDirectoryTarget("/", fs=file_base_location)
        return base_dir

    def join_path(self, parts):
        """Joints str to a path using the OS separator

        Args:
            parts (str): any path string

        Returns:
            str: path
        """
        return os.path.join(*parts)

    def search_child_files(self):
        """Search all root files within self.base_dir and returns the paths, relative
        to self.base_dir.

        Returns:
            list[str]: List with all paths relative to self.base_dir
        """
        child_wlcg = self.base_dir.child(self.join_path(self.store_parts), type="d")

        num_dirs = child_wlcg.listdir(pattern="0*", type="d")

        all_file_paths = []
        for num_dir in num_dirs:
            files = child_wlcg.child(num_dir, type="d").listdir(pattern="*.root", type="f")
            for file in files:
                file_path = self.join_path((child_wlcg.path, num_dir, file))
                all_file_paths.append(file_path)

        return all_file_paths

    def collect_file_information(self, child_file):
        """creates a dictionary with path, remotebase and file size information of a <child_file> from self.base_dir.

        Args:
            child_file (str): path of a child_file, relative to self.base_dir

        Returns:
            dict: dictionary with path, remotebase and file size of <child_file>
        """
        wlcg_dir = self.base_dir.child(child_file)
        size = wlcg_dir.stat().st_size
        path = wlcg_dir.path
        return {"path": path, "size": size, "remote_base": self.remote_base}

    def collect_files_information(self):
        """Creates a list with dictionaries holding path, remotebase and file size of all Child files of self.base_dir

        Returns:
            dict: with path, remotebase and file size of all child_files
        """
        child_files = self.search_child_files()
        return [self.collect_file_information(file) for file in child_files]


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
            files_infos.append({"path": path, "size": size, "remotebase": remote_base})
    return files_infos


def convert_bytes(byte_size, dst_unit="mb", precision=3):
    exponents_map = {"b": 0, "kb": 1, "mb": 2, "gb": 3, "tb": 4}
    if dst_unit not in exponents_map:
        raise ValueError("""Non SI unit detected choose from"
        ["bytes", "kb", "mb", "gb"]""")
    size = byte_size / 1024 ** exponents_map[dst_unit]
    return round(size, precision)


def calculate_mergin_factor(files, target_size=512, unit="mb", *, precision=3):
    num_files = len(files)
    size_files = convert_bytes(sum([file["size"] for file in files]), unit, precision)
    merging_factor = int(math.ceil(num_files / math.ceil(size_files / target_size)))
    return merging_factor
