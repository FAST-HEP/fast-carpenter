import os
import pandas as pd


class Collector():
    def __init__(self, filename):
        self.filename = filename

    def collect(self, dataset_readers_list):

        if len(dataset_readers_list) == 0:
            return

        output = self._prepare_output(dataset_readers_list)
        output.to_hdf(self.filename, key='df', mode='w')

    def _prepare_output(self, dataset_readers_list):
        dataset_readers_list = [(d, [r.contents for r in readers]) for d, readers in dataset_readers_list if readers]
        if len(dataset_readers_list) == 0:
            return None

        return self._merge_dataframes(dataset_readers_list)

    def _merge_dataframes(self, dataset_readers_list):

        final_df = None
        all_dfs = []
        keys = []

        for dataset, readers in dataset_readers_list:
            dataset_df = readers[0]
            for df in readers[1:]:
                if df is None:
                    continue
                dataset_df = dataset_df.add(df, fill_value=0.)

            all_dfs.append(dataset_df)
            keys.append(dataset)

        final_df = pd.concat(all_dfs, keys=keys, names=['dataset'], sort=True)

        return final_df.reset_index()


class EventByEventDataframe(object):
    """
    Write out a pandas dataframe with event-level values
    """

    def __init__(self, name, out_dir, collections, mask=None, flatten=True):

        self.name = name
        self.out_dir = out_dir
        self.mask = mask
        self.collections = collections
        self.contents = None
        self.df = None
        self.flatten = flatten

    def event(self, chunk):
        variables = chunk.tree.pandas.df(self.collections, flatten=self.flatten)

        variables["hashed_filename"] = hash(chunk.config.inputPaths[0])

        self.df = variables

        if self.contents is None:
            self.contents = self.df
        else:
            self.contents = pd.concat([self.contents, self.df])

        return True

    def collector(self):

        outfilename = "df_" + self.name + ".hd5"
        outfilename = os.path.join(self.out_dir, outfilename)
        return Collector(outfilename)

    def merge(self, rhs):
        if rhs.contents is None or len(rhs.contents) == 0:
            return
        if self.contents is None:
            self.contents = rhs.contents
            return
        self.contents = pd.concat([self.contents, rhs.contents])
