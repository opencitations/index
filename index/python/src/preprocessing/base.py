import os.path
from abc import ABCMeta, abstractmethod
from os import sep, makedirs, walk
from os.path import exists, basename, isdir
import tarfile
from json import load, loads
import zstandard as zstd
import pathlib
import zipfile


class Preprocessing(metaclass=ABCMeta):
    """This is the interface for implementing preprocessors for specific datasources.
    It provides the signatures of the methods for preprocessing a dump"""

    def __init__(self, **params):
        """preprocessor constructor."""
        for key in params:
            setattr(self, key, params[key])

    def get_all_files(self, i_dir_or_compr, req_type):
        result = []
        targz_fd = None

        if isdir(i_dir_or_compr):

            for cur_dir, cur_subdir, cur_files in walk(i_dir_or_compr):
                for cur_file in cur_files:
                    if cur_file.endswith(req_type) and not basename(cur_file).startswith("."):
                        result.append(os.path.join(cur_dir, cur_file))
        elif i_dir_or_compr.endswith("tar.gz"):
            targz_fd = tarfile.open(i_dir_or_compr, "r:gz", encoding="utf-8")
            for cur_file in targz_fd:
                if cur_file.name.endswith(req_type) and not basename(cur_file.name).startswith("."):
                    result.append(cur_file)
        elif i_dir_or_compr.endswith("zip"):
            with zipfile.ZipFile(i_dir_or_compr, 'r') as zip_ref:
                dest_dir = i_dir_or_compr.split(".")[0] + "_decompr_zip_dir"
                if not exists(dest_dir):
                    makedirs(dest_dir)
                zip_ref.extractall(dest_dir)
            for cur_dir, cur_subdir, cur_files in walk(dest_dir):
                for cur_file in cur_files:
                    if cur_file.endswith(req_type) and not basename(cur_file).startswith("."):
                        result.append(cur_dir + sep + cur_file)

        elif i_dir_or_compr.endswith("zst"):
            input_file = pathlib.Path(i_dir_or_compr)
            dest_dir = i_dir_or_compr.split(".")[0] + "_decompr_zst_dir"
            with open(input_file, 'rb') as compressed:
                decomp = zstd.ZstdDecompressor()
                if not exists(dest_dir):
                    makedirs(dest_dir)
                output_path = pathlib.Path(dest_dir) / input_file.stem
                if not exists(output_path):
                    with open(output_path, 'wb') as destination:
                        decomp.copy_stream(compressed, destination)
            for cur_dir, cur_subdir, cur_files in walk(dest_dir):
                for cur_file in cur_files:
                    if cur_file.endswith(req_type) and not basename(cur_file).startswith("."):
                        result.append(cur_dir + sep + cur_file)
        else:
            print("It is not possible to process the input path.", i_dir_or_compr)
        return result, targz_fd

    def load_json(self, file, targz_fd, file_idx, len_all_files):
        result = None

        if targz_fd is None:
            print("Open file %s of %s" % (file_idx, len_all_files))
            with open(file, encoding="utf8") as f:
                result = load(f)

        else:
            print("Open file %s of %s (in tar.gz archive)" % (file_idx, len_all_files))
            cur_tar_file = targz_fd.extractfile(file)
            json_str = cur_tar_file.read()

            # In Python 3.5 it seems that, for some reason, the extractfile method returns an
            # object 'bytes' that cannot be managed by the function 'load' in the json package.
            # Thus, to avoid issues, in case an object having type 'bytes' is return, it is
            # transformed as a string before passing it to the function 'loads'. Please note
            # that Python 3.9 does not show this behaviour, and it works correctly without
            # any transformation.
            if type(json_str) is bytes:
                json_str = json_str.decode("utf-8")

            result = loads(json_str)

        return result

    @abstractmethod
    def split_input(self):
        """ ...
        """
        pass

    def filter(self, data):
        pass

    @abstractmethod
    def splitted_to_file(self, cur_n, data, type):
        pass