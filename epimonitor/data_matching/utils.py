# -*- coding: utf-8 -*- 

import random
import numpy as np
import pandas as pd
from tqdm import tqdm
import matplotlib.pyplot as plt
from collections import defaultdict

'''
    -------------------------------------------------
    ---------- SUMMARIES AND VISUALIZATION ----------
    ------------------------------------------------- 
'''

def split_list(lst, nparts):
    '''
        Stack Overflow link:
        https://stackoverflow.com/questions/2130016/splitting-a-list-into-n-parts-of-approximately-equal-length/37414115#37414115
    '''
    n = len(lst)
    return [lst[i * (n // nparts) + min(i, n % nparts):(i+1) * (n // nparts) + min(i+1, n % nparts)] for i in range(nparts)]

def score_summary(score_arr, bins, range_certain, range_potential, scale="linear"):
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
            info:
                Dictionary. Main information on the pairs obtained and their scores.
    '''
    fig, ax = plt.subplots(1, figsize=(7,4.8))
    freq, bins = np.histogram(score_arr, bins=bins)
    ax.bar(bins[:-1], freq, facecolor="royalblue", edgecolor="white", width=1.0, linewidth=3.0, align='edge')

    
    cc = "#353535"
    ax.spines['top'].set_color('none')
    ax.spines['right'].set_color('none')
    ax.spines["left"].set_color(cc)
    ax.spines["bottom"].set_color(cc)
    ax.spines['left'].set_position(('outward', 7))
    ax.spines['bottom'].set_position(('outward', 7))

    ax.spines["left"].set_linewidth(1.5)
    ax.spines["bottom"].set_linewidth(1.5)
    ax.spines["top"].set_linewidth(1.5)
    ax.spines["right"].set_linewidth(1.5)

    ax.tick_params(width=1.5, labelsize=11, colors=cc)
    ax.set_ylabel("Número de pares", weight="bold", fontsize=14, labelpad=8, color=cc)
    ax.set_xlabel("Score", weight="bold", fontsize=13, color=cc)
    ax.grid(alpha=0.2)

    ax.fill_between(range_potential, y1=max(freq)+10, color="orange", alpha=0.2)
    ax.fill_between(range_certain, y1=max(freq)+10, color="dodgerblue", alpha=0.2)

    ncertain = score_arr[(score_arr>=range_certain[0]) & (score_arr<range_certain[1])].shape[0]
    npotential = score_arr[(score_arr>=range_potential[0]) & (score_arr<range_potential[1])].shape[0]
    ndiff = score_arr[(score_arr<range_potential[0])].shape[0]

    ax.set_xticks(bins)
    ax.set_xticklabels([f"{n:.1f}" for n in bins], rotation=45)
    ax.set_yscale(scale)
    ax.xaxis.label.set_color("#5b5b5b")
    ax.yaxis.label.set_color("#5b5b5b")
    info = {"FIG": fig, "AXIS": ax, "FREQUENCY AND BINS": (freq, bins), 
            "# IGUAIS": ncertain, "# POTENCIAIS": npotential, "# DIFERENTES": ndiff }
    return info

def show_pair(pairs, left_df, right_df=None, 
              left_cols=None, right_cols=None, random_state=None):
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
    if random_state is not None:
        random.seed(random_state)

    pair = random.choice(pairs)
    left_index, right_index = pair[0], pair[1]

    temp_left = left_df
    temp_right = right_df

    # --> Verify for columns
    if left_df is not None and left_cols is None:
        raise ValueError("Subset of columns' names must be parsed.")
    if right_df is not None and right_cols is None:
        raise ValueError("Subset of columns' names must be parsed.")

    if temp_right is None:
        display_df = pd.concat( [temp_left[left_cols].loc[left_index], temp_left[left_cols].loc[right_index]], axis=1 )
    else:
        display_df = pd.concat( [temp_left[left_cols].loc[left_index], temp_right[right_cols].loc[right_index]], axis=1 )
    return display_df 


'''
    -------------------------------------------------
    ----------------- OPERATIONAL -------------------
    ------------------------------------------------- 
'''

def format_annotation(list_of_pairs, left_df, right_df, agg_df, left_cols, right_cols, batchsize=5000):
    '''
    
    '''
    count = 0
    object_list = []

    # -- for memory efficiency purposes, process the list of pairs by batches
    splitted_list_of_pairs = [ list_of_pairs[x:x+batchsize] for x in range(0, len(list_of_pairs), batchsize) ]
    for current_list_of_pairs in splitted_list_of_pairs:
        
        # -- separate the left and right indexes 
        left_indexes = [ pair[0] for pair in current_list_of_pairs ]
        right_indexes = [ pair[1] for pair in current_list_of_pairs ]
        #classification = [ pair[2] for pair in current_list_of_pairs ] # maybe not needed
        
        # -- signal for linkage
        if right_df is not None:
            left_records, right_records = left_df[left_cols].loc[left_indexes].to_dict(orient='records'), right_df[right_cols].loc[right_indexes].to_dict(orient='records')
        # -- signal for deduplication
        else:
            left_records, right_records = left_df[left_cols].loc[left_indexes].to_dict(orient='records'), left_df[left_cols].loc[right_indexes].to_dict(orient='records')

        agg = None 
        if agg_df is not None:
            agg = agg_df.loc[current_list_of_pairs].to_dict(orient='records')

        for subindex in range(len(current_list_of_pairs)):
            count+=1
            pair_element = {"cod": count,
                            "classification": '',
                            "a": left_records[subindex], "b": right_records[subindex], 
                            "identifiers": {"a": left_indexes[subindex], "b": right_indexes[subindex]},
                            "agg": agg[subindex] }
            
            object_list.append(pair_element)
    return object_list

def create_json_pairs(left_df, right_df, left_cols, right_cols, list_of_pairs, 
                      agg_df=None, classification="", rec_max=None):
    '''
        Description.

        Args:
        -----
            left_df:
                pandas.DataFrame.
            right_df:
                pandas.DataFrame.
            left_cols:
                List.
            right_cols:
                List.
            list_of_pairs:
                List.
            agg_df:
                pandas.DataFrame.
            classification:
                String.
            rec_max:
                Integer. Default None.
        Return:
        -------
            object_list:
                List.
    ''' 
    count = 0
    object_list = []
    for row in list_of_pairs:
        count+=1
        pair = row
    
        left_pair = left_df[left_cols].loc[pair[0]].to_dict()
        if right_df is not None: # -- signal for linkage
            right_pair = right_df[right_cols].loc[pair[1]].to_dict()
        else: # -- signal for deduplication
            right_pair = left_df[left_cols].loc[pair[1]].to_dict()

        agg = None 
        if agg_df is not None:
            agg = agg_df.loc[(pair[0], pair[1])].to_dict()
            
        pair_element = {"cod": count,
                        "classification": classification,
                        "a": left_pair, "b": right_pair, 
                        "identifiers": {"a": pair[0], "b": pair[1]},
                        "agg": agg }
        
        object_list.append(pair_element)
        if rec_max is not None and count==rec_max:
            break

    return object_list


def find_root(index, ptr):
    dummy = index
    while ptr[dummy]>=0:
        dummy = ptr[dummy]
    return dummy

# --> Deduplication
def deduple_grouping(pairs):
    '''
        Perform grouping of matched records into a final schema file, identifying unique individuals.

        After deduplication, we use 'pairs'(a list of tuples corresponding to each pair of
        matched records), to create a hash/dictionary structure associating a given record to all its matched
        records (same person). Dictionary contains a list of matched records.

        Args:
        -----
            pairs:
                pandas.DataFrame. A dataframe containing at least two columns representing
                the matched pairs of unique records: "left" and "right".  
        Return:
        -------
            matched_records:
                collections.defaultdict. 
    '''
    left_nots = pairs["left_id"].tolist()
    right_nots = pairs["right_id"].tolist()

    # --> Define data structure of trees to aggregate several matched files through transitive relations. 
    # ----> Unique records in 'pairs'
    unique_nots = np.unique(left_nots+right_nots) 
    # ----> Tree positions of each unique record of 'pairs' (based on the union/find algorithms)
    ptr = np.zeros(unique_nots.shape[0], int) - 1
    # ----> Associate each record to its position in 'ptr' (hash)
    ptr_index = dict( zip(unique_nots, np.arange(0, unique_nots.shape[0], 1)) )

    # --> Aggregate matched records associating each unique person to a root index. 
    for index in tqdm(range(len(left_nots))):
        left = left_nots[index]
        right = right_nots[index]
        left_index = ptr_index[left]
        right_index = ptr_index[right]
    
        left_root = find_root(left_index, ptr)
        right_root = find_root(right_index, ptr)
    
        if left_root==right_root:
            continue
    
        bigger_root, bigger_index = left_root, left_index
        smaller_root, smaller_index = right_root, right_index
        if ptr[right_root]<ptr[left_root]:
            bigger_root, bigger_index = right_root, right_index
            smaller_root, smaller_index = left_root, left_index
    
        ptr[bigger_root] += ptr[smaller_root]
        ptr[smaller_root] = bigger_root
        
    matched_records = defaultdict(lambda: [])
    for index in range(len(ptr)):
        local_not = unique_nots[index]
        root_not = unique_nots[find_root(index, ptr)]
        if root_not!=local_not:
            matched_records[root_not].append(local_not)

    return matched_records

# --> Linkage
def linkage_grouping(pairs):
    '''
        Perform grouping of matched records into a final schema file, identifying unique individuals.

        After the linkage between two databases, we use 'pairs' (containing the ID of the matched 
        records) to create a hash/dictionary structure associating a given record in one database 
        to all its matched records in the other. Dictionary contains a list of matched records.

        Args:
        -----
            pairs:
                pandas.DataFrame. A dataframe containing at least two columns representing
                the matched pairs of unique records: "left" and "right".  
        Return:
        -------
            result:
                collections.defaultdict. 
    '''
    pairs_t = list(pairs.groupby("left")["right"].value_counts().index)
    result = {}
    for k, v in pairs_t:
        result.setdefault(k, []).append(v)
    return result 