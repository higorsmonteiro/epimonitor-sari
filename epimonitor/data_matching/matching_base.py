# -*- coding: utf-8 -*- 

import os
import ujson as json
import numpy as np
import pandas as pd
import recordlinkage

import epimonitor.data_matching.utils as matching_utils

class MatchingBase:
    '''
        Base class with general settings and definitions for the tasks of deduplication and probabilistic 
        linkage over databases.

        Args:
        -----
            left_df:
                pandas.DataFrame.
            right_df:
                pandas.DataFrame.
            left_id:
                String. Unique ID for the left-hand dataframe.
            right_id:
                String. Unique ID for the right-hand dataframe.
            env_folder:
                String. Optional path to the folder pairs' storage.

        Attributes:
        -----------
            comparison_matrix:
                pandas.DataFrame.
    '''
    def __init__(self, left_df, right_df=None, left_id=None, right_id=None, env_folder=None) -> None:
        self.linkage_vars = None
        self.candidate_pairs = None
        self.name_ranks = None
        self.compare_cl, self._comparison_matrix = None, None

        # -- solve init for the left dataframe
        self.left_df, self.left_id = left_df.copy(), left_id
        if self.left_id is None or self.left_id not in self.left_df.columns:
            raise Exception("Must provide an existing field as a unique identifier.")
        self.left_df = self.left_df.set_index(self.left_id)
        
        # -- solve init for the right dataframe, if the case
        self.right_df = None
        if right_df is not None:
            self.right_df, self.right_id = right_df.copy(), right_id
            if self.right_id is None or self.right_id not in self.right_df.columns:
                raise Exception("Must provide an existing field as a unique identifier.")
            self.right_df = self.right_df.set_index(self.right_id)
            
        # -- select working folder
        self.env_folder = env_folder
        if self.env_folder is not None and not os.path.isdir(self.env_folder):
            os.mkdir(self.env_folder)

    # ------------------------------------------
    # --------------- Attributes ---------------
    
    @property
    def comparison_matrix(self):
        if self._comparison_matrix is not None:
            return self._comparison_matrix

    @comparison_matrix.setter
    def comparison_matrix(self):
        raise Exception("Not possible to change this attribute from outside.")
    
    # ---------------------------------------------------------
    # --------------- Matching general settings ---------------
    
    def set_linkage_variables(self, linkage_vars, string_method="damerau_levenshtein"):
        '''
            Define the variables to be compared and the method of comparison.
            
            Args:
            -----
                linkage_vars:
                    List of tuples. Each element of the list must be at least a tuple of size 3.
                    The 1st and 2nd position of the tuple are the names of one field of the
                    left-hand dataframe and one field of the the right-hand dataframe, respectively. 
                    This determines which fields must be compared. For deduplication, only the first
                    position is considered. 
                    
                    The 3rd position defines how those fields will be compared ('exact', 'string', 
                    'numerical', ...).

                    4th position is the label of the comparison.   
        '''
        self.linkage_vars = linkage_vars
        self.compare_cl = recordlinkage.Compare()
        for elem in self.linkage_vars:
            left_field, right_field, cmethod, label = elem[0], elem[1], elem[2], elem[3]
            if cmethod=='exact':
                self.compare_cl.exact(left_field, right_field, label=label)
            elif cmethod=='string':
                self.compare_cl.string(left_field, right_field, label=label, method=string_method, threshold=None)
            elif cmethod=='numerical':
                pass
            elif cmethod=='date':
                pass
            else:
                pass
        return self
    
    def get_rank_names(self, rank_colnames=['rank_primeiro_nome', 'rank_primeiro_nome_mae']):
        '''
            Get the data with the ranking of first names. 

            The data is expected from the data parsed to the class. The names of the columns
            is provided externally.
        '''
        self.name_ranks = self.left_df[rank_colnames]
    
    def set_linkage_old(self, compare_rules, string_method="damerau_levenshtein"):
        '''
            Description.

            Args:
            -----
                compare_rules:
                    Dictionary. Keys of the dictionary represent the linkage variables to be used
                    in the comparison. Each key holds a list of values containing at least a single
                    element. The first element should be always the type of comparison to be 
                    performed for the given field: {'exact', 'string'}. For 'string' comparison,
                    the second element of the list should be the threshold (0.0 to 1.0) for the
                    comparison method. For 'exact' comparison, no further values are needed.
                **kwargs:
                    Aside from the 'threshold' argument, arguments are the same as the comparison
                    methods from recordlinkage.Compare class. 
            Return:
                None.
        '''
        self.linkage_vars = list(compare_rules.keys())

        # -- settings for comparison between fields
        self.compare_cl = recordlinkage.Compare()
        for key, values in compare_rules.items():
            if values[0]=="exact":
                self.compare_cl.exact(key, key, label=key)
            elif values[0]=="string":
                self.compare_cl.string(key, key, label=key, threshold=values[1], method=string_method)
            else:
                pass
        return self
    
    def define_pairs(self):
        return self

    def perform_linkage(self):
        return self

    # ----------------------------------------------------------
    # ---------------------- INPUT/OUTPUT ----------------------
    
    def export_comparison_matrix(self, env_folder, fname):
        if self._comparison_matrix is not None:
            self._comparison_matrix.to_parquet(os.path.join(env_folder, f"{fname}.parquet"))

    def save_files(self, pairs, aggr_df, output_folder=None, left_cols=None, right_cols=None, overwrite=False, batchsize=100):
        '''
            Description.

            Args:
            -----
                pairs:
                    List. List of 2-tuples containing the pair of IDs representing
                    the pairs.
                aggr_df:
                    pandas.DataFrame.
                output_folder:
                    String.
                left_cols:
                    List. Default None. Columns to be used when saving the information of
                    a given record from the left database.
                right_cols:
                    List. Default None. Columns to be used when saving the information of
                    a given record from the right database.
                overwrite:
                    Boolean. If False it will not override the existing files of 
                    classified pairs. 
        '''
        # -- create output folder to save the files.
        if self.env_folder is None and output_folder is None:
            raise Exception("No working folder was provided.")
        
        output = self.env_folder
        if output is None:
            output = output_folder
        annotation_folder = os.path.join(output, "annotation_files")
        if not os.path.isdir(annotation_folder):
            os.mkdir(annotation_folder)
        
        # -- permission to overwrite existent files.
        if not overwrite:
            raise Exception("Overwrite of annotation files not allowed.")
        
        # -- create annotation data
        json_list = matching_utils.format_annotation(pairs, self.left_df, self.right_df, aggr_df, left_cols, right_cols, batchsize=5000)
        batches = [ json_list[x:x+batchsize] for x in range(0, len(json_list), batchsize) ]
        for index, current_batch in enumerate(batches):
            pairs_json = {'pairs': current_batch }
            with open(os.path.join(output, f"PAIRS_{index}.json"), "w") as f:
                json.dump(pairs_json, f, indent=4)


    def save_pairs(self, positive_pairs, potential_pairs, negative_pairs,  
                   left_cols=None, right_cols=None, agg_df=None, 
                   overwrite=False, negative_max=1000):
        '''
            Save pairs (negative, positive and potential pairs) considering a 
            format for further annotation.

            Args:
            -----
                positive_pairs:
                    List. List of 2-tuples containing the pair of IDs representing
                    the pairs classified as positive.
                potential_pairs:
                    List. List of 2-tuples containing the pair of IDs representing
                    the pairs classified as potential pairs (to be classified manually).
                negative_pairs:
                    List. List of 2-tuples containing the pair of IDs representing
                    the pairs classified as negative.
                left_cols:
                    List. Default None. Columns to be used when saving the information of
                    a given record from the left database.
                right_cols:
                    List. Default None. Columns to be used when saving the information of
                    a given record from the right database.
                agg_df:
                    pandas.DataFrame. Default None. 
                overwrite:
                    Boolean. If False it will not override the existing files of 
                    classified pairs. 
                negative_max:
                    Integer. Default None. Maximum number of negative pairs to be stored.
            Return:
            -------
                None.
        '''
        if self.env_folder is None:
            raise Exception("No working folder was provided.")

        annotation_folder = os.path.join(self.env_folder, "annotation_files")
        if not os.path.isdir(annotation_folder):
            os.mkdir(annotation_folder)
        if not overwrite:
            raise Exception("Overwrite of annotation files not allowed.")
        
        
        json_list = matching_utils.create_json_pairs(self.left_df, self.right_df, left_cols, right_cols, 
                                                     positive_pairs, classification="positive", agg_df=agg_df)
        pairs_json = {"pairs": json_list}
        with open(os.path.join(self.env_folder, "annotation_files", "POSITIVE_PAIRS.json"), "w") as f:
            json.dump(pairs_json, f, indent=4)
        
        json_list = matching_utils.create_json_pairs(self.left_df, self.right_df, left_cols, right_cols, 
                                                     potential_pairs, classification="potential", agg_df=agg_df)
        pairs_json = {"pairs": json_list}
        with open(os.path.join(self.env_folder, "annotation_files", "POTENTIAL_PAIRS.json"), "w") as f:
            json.dump(pairs_json, f, indent=4)
    
        json_list = matching_utils.create_json_pairs(self.left_df, self.right_df, left_cols, right_cols, 
                                                     negative_pairs, classification="negative", rec_max=negative_max, agg_df=agg_df)
        pairs_json = {"pairs": json_list}
        with open(os.path.join(self.env_folder, "annotation_files", "NEGATIVE_PAIRS.json"), "w") as f:
            json.dump(pairs_json, f, indent=4)
        

    def load_pairs(self, annotation_folder="annotation_files"):
        '''
        
        '''
        annotation_folder = os.path.join(self.env_folder, annotation_folder)
        if not os.path.isdir(annotation_folder):
            raise Exception("Annotation folder does not exist.")

        with open(os.path.join(annotation_folder, "POSITIVE_PAIRS.json"), "r", encoding="latin") as f:
            positive_p = json.load(f)
        with open(os.path.join(annotation_folder, "POTENTIAL_PAIRS.json"), "r", encoding="latin") as f:
            potential_p = json.load(f)
        
        pairs = positive_p["pairs"] + potential_p["pairs"]

        # -- create dataframe
        df_pairs = {
            "left_id": [ pair["identifiers"]["a"] for pair in pairs ],
            "right_id": [ pair["identifiers"]["b"] for pair in pairs ],
            "classification": [ pair["classification"] for pair in pairs ],
        }
        df_pairs = pd.DataFrame(df_pairs)
        return df_pairs

    '''
        ---------------------------------------------------
        ------------ SUMMARY AND VISUALIZATION ------------
        ---------------------------------------------------
    '''
    def show_pair(self, pairs, left_cols=None, right_cols=None, random_state=None):
        '''
            Show a random pair of records from the list 'pairs' obtained by the matching.

            Args:
            -----
                pairs:
                    List.
                left_df:
                    pandas.DataFrame.
                right_df:
                    pandas.DataFrame. Default None.
                left_cols:
                    List. Default None.
                right_cols:
                    List. Default None.
                random_state:
                    Integer. Default None.
            Return:
            -------
                display_df:
                    pandas.DataFrame.
        '''
        display_df = matching_utils.show_pair(pairs=pairs, left_df=self.left_df, right_df=self.right_df, 
                                              left_cols=left_cols, right_cols=right_cols, random_state=random_state)
        return display_df

    def score_summary(self, score_arr, bins, range_certain, range_potential, scale="linear"):
        '''
            Plot a distribution of the scores resulted from the data matching process.

            Args:
            -----
                score_arr:
                    np.array or pd.Series. List of scores for each pair compared during
                    the matching process. When a pandas Series is parsed, the index is 
                    expected to be a MultiIndex containing the IDs of the records compared.
                bins:
                    list. Custom histogram bins.
                range_certain:
                    list. List of size two containing the lower and upper bound of the score
                    range of the records considered to be matched for certain.
                range_potential:
                    list. List of size two containing the lower and upper bound of the score
                    range of the records considered to be potential matching.
                scale:
                    String. {'linear', 'log', ...}. Scale of the y-axis.

            Return:
            -------
        '''
        return matching_utils.score_summary(score_arr, bins, range_certain, range_potential, scale)