import os
import json
import numpy as np
import pandas as pd
import recordlinkage
from recordlinkage.index import SortedNeighbourhood

from epimonitor.data_matching import MatchingBase

class Deduple(MatchingBase):

    def define_pairs(self, blocking_var, window=1):
        '''
            After setting the properties of the linkage, blocking is defined to generate the pairs 
            for comparison.

            Args:
            -----
                blocking_var:
                    String. Name of the field regarding the blocking variable for the linkage.
                window:
                    Odd Integer. Window parameter for the sorted neighborhood blocking algorithm. 
                    window equal one means exact blocking.
        '''
        indexer = recordlinkage.Index()
        indexer.add(SortedNeighbourhood(blocking_var, blocking_var, window=window))
        self.candidate_pairs = indexer.index(self.left_df)
        # -- sort the order of each pair with respect to the ID.
        self.candidate_pairs = pd.MultiIndex.from_tuples( list({*map(tuple, map(sorted, list(self.candidate_pairs)))}), names=[f"{self.left_id}_1", f"{self.left_id}_2"] )
        print(f"Number of pairs: {len(self.candidate_pairs)}")
        return self
    
    # TO DO: DEDUPLE OVER CHUNKSIZES (FOR VERY LARGE DATASETS) -> Could be done from outside
    def perform_linkage(self, blocking_var, window=1, output_fname="feature_pairs", threshold=None):
        '''
            After setting the properties of the linkage, blocking is defined and the linkage is performed.

            Args:
            -----
                blocking_var:
                    String. Name of the field regarding the blocking variable for the linkage.
                window:
                    Odd Integer. Window parameter for the sorted neighborhood blocking algorithm. 
                    window equal one means exact blocking.
                chunksize: NOT IMPLEMENTED.
                    Integer. Size of each partition of the right (BOTH!) database for partitioned linkage 
                    (used for large databases).
        '''

        # --> set blocking rule and create pairs for comparison
        if self.candidate_pairs is None:
            indexer = recordlinkage.Index()
            indexer.add(SortedNeighbourhood(blocking_var, blocking_var, window=window))
            self.candidate_pairs = indexer.index(self.left_df)
            print(f"Number of pairs: {len(self.candidate_pairs)}")
        if len(self.candidate_pairs):
            self._comparison_matrix = self.compare_cl.compute(self.candidate_pairs, self.left_df)
        else:
            return self

        # --> Save scores for all pairs
        if self.env_folder is not None:
            self._comparison_matrix.to_parquet(os.path.join(self.env_folder, f"{output_fname}.parquet"))

        # --> All field scores less than 'threshold' are reduced to zero.
        if threshold is not None and threshold<=1.0:
            self._comparison_matrix[self._comparison_matrix<threshold] = 0.0

class PLinkage(MatchingBase):

    def define_pairs(self, blocking_var, window=1):
        pass
    
    # TO DO: LINKAGE OVER CHUNKSIZES (FOR VERY LARGE DATASETS)
    def perform_linkage(self, blocking_var, window=1, output_fname="feature_pairs", threshold=None):
        '''
            After setting the properties of the linkage, blocking is defined and the linkage is performed.

            Args:
            -----
                blocking_var:
                    String. Name of the field regarding the blocking variable for the linkage.
                window:
                    Odd Integer. Window parameter for the sorted neighborhood blocking algorithm. 
                    window equal one means exact blocking.
                chunksize: NOT IMPLEMENTED.
                    Integer. Size of each partition of the right (BOTH!) database for partitioned linkage 
                    (used for large databases).
        '''
        
        # --> set blocking rule and create pairs for comparison
        indexer = recordlinkage.Index()
        indexer.add(SortedNeighbourhood(blocking_var, blocking_var, window=window))
        candidate_links = indexer.index(self.left_df, self.right_df)
        print(f"Number of pairs: {len(candidate_links)}")
        if len(candidate_links):
            self._comparison_matrix = self.compare_cl.compute(candidate_links, self.left_df, self.right_df)
        else:
            return self

        # --> Save scores for all pairs
        if self.env_folder is not None:
            self._comparison_matrix.to_parquet(os.path.join(self.env_folder, f"{output_fname}.parquet"))

        # --> All field scores less than 'threshold' are reduced to zero.
        if threshold is not None and threshold<=1.0:
            self._comparison_matrix[self._comparison_matrix<threshold] = 0.0