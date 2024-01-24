from typing import Dict, Iterable, Union

import pandas as pd
from cachetools import TTLCache, cached
from pandas import DataFrame

from bioos.config import Config
from bioos.errors import ConflictError, NotFoundError
from bioos.utils.common_tools import SingletonType


class DataModelResource(metaclass=SingletonType):

    def __init__(self, workspace_id: str):
        self.workspace_id = workspace_id

    def __repr__(self):
        return f"DataModelInfo:\n{self._entities_with_cache()}"

    @cached(cache=TTLCache(maxsize=100, ttl=1))
    def _entities_with_cache(self) -> pd.DataFrame:
        return self.list()

    def list(self) -> pd.DataFrame:
        """Returns all 'normal' data_models with .

        :return: table of 'normal' data models
        :rtype: DataFrame
        """
        models = Config.service().list_data_models({
            'WorkspaceID':
            self.workspace_id,
        }).get("Items")
        df = pd.DataFrame.from_records(models)
        return df[df.Type == "normal"].reset_index(drop=True)

    def write(self, sources: Dict[str, DataFrame], force: bool = True):
        """Writes the given data to the remote 'normal' data_model .

        *Example*:
        ::

            import pandas as pd
            ws = bioos.workspace("foo")
            data = pd.DataFrame({"aaa": "bbb", "ccc": "ddd"})
            ws.data_models.write(sources = data, force = False)

        :param sources: data_model content or a batch of data_model content
        :type sources: Dict[str, DataFrame]
        :param force: Whether to cover the same name data_model
        :type force: bool
        """
        if not force:
            entities = self.list()
            all_normal_models_set = set()
            for _, entity in entities.iterrows():
                all_normal_models_set.add(entity.Name)
            duplicate_models_set = all_normal_models_set.intersection(
                set(sources.keys()))
            if len(duplicate_models_set) > 0:
                raise ConflictError(
                    "sources", f"{duplicate_models_set} already exists, "
                    f"pls use force=True to overwrite")

        for name, data in sources.items():
            Config.service().create_data_model({
                'WorkspaceID': self.workspace_id,
                'Name': name,
                'Headers': list(data.head()),
                'Rows': data.values.tolist(),
            })

    def read(
        self,
        sources: Union[str, Iterable[str],
                       None] = None) -> Dict[str, DataFrame]:
        """Reads the data from the remote 'normal' data_models .

        return all data_models if `sources` not set

        *Example*:
        ::

            ws = bioos.workspace("foo")
            ws.data_models.read(sources = "bar", force = False) #output: {"bar": DataFrame}

        :param sources: name of data_model to read
        :type sources: Union[str, Iterable[str]]
        :return: Reading result
        :rtype: Dict[str, DataFrame]
        """
        if sources is not None:
            sources = {sources} if isinstance(sources, str) else set(sources)

        entities = self.list()
        all_normal_models = {}
        for _, entity in entities.iterrows():
            all_normal_models[entity.Name] = entity.ID
        # return all data_models if empty
        if not sources:
            models_to_find = all_normal_models.keys()
        else:
            models_to_find = sources.intersection(set(
                all_normal_models.keys()))

        if len(models_to_find) == 0:
            raise NotFoundError("sources", sources)

        models_res = {}
        for model in models_to_find:
            content = Config.service().list_data_model_rows({
                'WorkspaceID':
                self.workspace_id,
                'ID':
                all_normal_models[model],
                'PageSize':
                0,
            })
            if content and content["TotalCount"] > 0:
                res_df = pd.DataFrame.from_records(content['Rows'])
                res_df.columns = content['Headers']
                models_res[model] = res_df
        return models_res

    def delete(self, target: str):
        """Deletes a remote 'normal' data_model for given name.

        *Example*:
        ::

            ws = bioos.workspace("foo")
            ws.data_models.delete(target = "bar")

        :param target: name of data_model to delete
        :type target: str
        """
        entities = self.list()

        entity_row = entities[entities["Name"] == target]
        if entity_row.empty:
            raise NotFoundError("target", target)

        ids = Config.service().list_data_model_row_ids({
            'WorkspaceID':
            self.workspace_id,
            'ID':
            entity_row.ID.iloc[0],
        })

        Config.service().delete_data_model_rows_and_headers({
            'WorkspaceID':
            self.workspace_id,
            'ID':
            entity_row.ID.iloc[0],
            'RowIDs':
            ids["RowIDs"]
        })
