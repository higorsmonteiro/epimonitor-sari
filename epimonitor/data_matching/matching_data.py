import os
import json
import numpy as np
import pandas as pd
import recordlinkage
from recordlinkage.index import SortedNeighbourhood

from epimonitor.data_matching import MatchingBase
import epimonitor.data_matching.utils as utils

class Deduple(MatchingBase):

    def define_pairs(self, blocking_var, window=1):
        '''
            After setting the properties of the linkage, blocking is defined to generate the pairs 
            for comparison.

            Args:
            -----
                blocking_var:
                    String. Name of the field representing the blocking variable.
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
    
    def perform_linkage(self, threshold=None, number_of_blocks=1, verbose=True):
        '''
            Perform the comparison calculations.

            Args:
            -----
                threshold:
                    Float.
                number_of_blocks:
                    Integer. Number of partitions on the list of pairs to be compared. To be used
                    for larger datasets.
        '''
        
        if len(self.candidate_pairs) and number_of_blocks==1:
            # -- one round of calculation for the complete list of pairs.
            self._comparison_matrix = self.compare_cl.compute(self.candidate_pairs, self.left_df)
        elif len(self.candidate_pairs) and number_of_blocks>1:
            # -- calculation by dividing the list of pairs into 'n' batches.
            splitted_list = utils.split_list(list(self.candidate_pairs), number_of_blocks)
            
            self._comparison_matrix = []
            for index, subset_candidate_pairs in enumerate(splitted_list):
                subset_candidate_pairs = pd.MultiIndex.from_tuples( list({*map(tuple, map(sorted, list(subset_candidate_pairs)))}), names=[f"{self.left_id}_1", f"{self.left_id}_2"] )
                if verbose:
                    print(f"Matching subset batch {index+1}/{len(splitted_list)} of size {subset_candidate_pairs.shape[0]} ...")
                self._comparison_matrix.append( self.compare_cl.compute(subset_candidate_pairs, self.left_df) )
            self._comparison_matrix = pd.concat(self._comparison_matrix)
            if verbose:
                print('Done.')
        else:
            return self

        # -- all scores less than 'threshold' are reduced to zero.
        if threshold is not None and threshold<=1.0:
            self._comparison_matrix[self._comparison_matrix<threshold] = 0.0

        self.get_rank_names()
        self._comparison_matrix = self.comparison_matrix.merge(self.name_ranks, left_on=[f"{self.left_id}_1"], right_index=True, how="left").fillna(7)

        return self

class PLinkage(MatchingBase):

    def define_pairs(self, left_blocking_var, right_blocking_var, window=1):
        '''
            After setting the properties of the linkage, blocking is defined to generate the pairs for 
            comparison.

            Args:
            -----
                left_blocking_var:
                    String. Name of the field representing the blocking field for the left-hand dataframe.
                right_blocking_var:
                    String. Name of the field representing the blocking field for the right-hand dataframe.
                window:
                    Odd Integer. Window parameter for the sorted neighborhood blocking algorithm. 
                    window equal one means exact blocking.
        '''
        indexer = recordlinkage.Index()
        indexer.add(SortedNeighbourhood(left_blocking_var, right_blocking_var, window=window))
        self.candidate_pairs = indexer.index(self.left_df, self.right_df)
        # -- sort the order of each pair with respect to the ID. (THIS SORTING IS DANGEROUS)
        self.candidate_pairs = pd.MultiIndex.from_tuples(list(self.candidate_pairs), names=[f"{self.left_id}", f"{self.right_id}"] )
        print(f"Number of pairs: {len(self.candidate_pairs)}")
        return self
    
    # TO DO: LINKAGE OVER CHUNKSIZES (FOR VERY LARGE DATASETS)
    def perform_linkage_vOLD(self, blocking_var, window=1, output_fname="feature_pairs", threshold=None, split_n=1):
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

        #self.get_rank_names()
        #self._comparison_matrix = self.comparison_matrix.merge(self.name_ranks, left_on=[f"{self.left_id}_1"], right_index=True, how="left").fillna(7)

    def perform_linkage(self, threshold=None, number_of_blocks=1, verbose=True):
        '''
            Perform the comparison calculations.

            Args:
            -----
                threshold:
                    Float.
                number_of_blocks:
                    Integer. Number of partitions on the list of pairs to be compared. To be used
                    for larger datasets.
        '''
        
        if len(self.candidate_pairs) and number_of_blocks==1:
            # -- one round of calculation for the complete list of pairs.
            self._comparison_matrix = self.compare_cl.compute(self.candidate_pairs, self.left_df, self.right_df)
        elif len(self.candidate_pairs) and number_of_blocks>1:
            # -- calculation by dividing the list of pairs into 'n' batches.
            splitted_list = utils.split_list(list(self.candidate_pairs), number_of_blocks)
            
            self._comparison_matrix = []
            for index, subset_candidate_pairs in enumerate(splitted_list):
                # -- expected format: labeled multiindex.
                #subset_candidate_pairs = pd.MultiIndex.from_tuples( list({*map(tuple, map(sorted, list(subset_candidate_pairs)))}), names=[f"{self.right_id}", f"{self.left_id}"] )
                subset_candidate_pairs = pd.MultiIndex.from_tuples( list(subset_candidate_pairs), names=[f"{self.left_id}", f"{self.right_id}"] )
                if verbose:
                    print(f"Matching subset batch {index+1}/{len(splitted_list)} of size {subset_candidate_pairs.shape[0]} ...")
                # THERE IS AN ORDERING ISSUE HERE
                self._comparison_matrix.append( self.compare_cl.compute(subset_candidate_pairs, self.left_df, self.right_df) )
            self._comparison_matrix = pd.concat(self._comparison_matrix)
            if verbose:
                print('Done.')
        else:
            return self

        # -- all scores less than 'threshold' are reduced to zero.
        if threshold is not None and threshold<=1.0:
            self._comparison_matrix[self._comparison_matrix<threshold] = 0.0

        self.get_rank_names()
        self._comparison_matrix = self.comparison_matrix.merge(self.name_ranks, left_on=[f"{self.left_id}"], right_index=True, how="left").fillna(7) # 7 or 0??

        return self