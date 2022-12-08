from typing import List
from collections import deque
from pandas import DataFrame
from pyspark import SparkContext, SparkConf
from scipy.stats import entropy
from catagories import *
from copy import deepcopy
from operator import add
import pandas as pd
import time
import pyspark


class Node:

    def __init__(self, id: str, level: int):
        self.id: str = id
        self.children = dict()  # split attribute value : child
        self.level = level
        self.split_attr = -1
        self.is_leaf = False
        self.prediction = -1

    def __str__(self):
        dashes = '|---' * self.level
        res = f"{str(dashes)}{self.id} \n"
        for child in self.children.values():
            res = res + str(child)
        return res


def build_subtree_in_memory(df: DataFrame, root: Node, available_attributes: List, max_level: int):
    _df = df[df[9] == root.id]
    if len(_df) == 0:
        return

    flag = True
    for _ in available_attributes:
        if _:
            flag = False
            break
    if flag:
        root.is_leaf = True
        root.prediction = int(_df.mode(axis=0).at[0, 8])
        return

    if len(_df) < 100 or root.level >= max_level:
        root.is_leaf = True
        root.prediction = int(_df.mode(axis=0).at[0, 8])
        return

    arrival_delay_count = [0] * len(time_level_map)
    for i, _ in enumerate(time_level_map.values()):
        arrival_delay_count[i] = len(_df[_df[catagory_map['arrival_delay']] == _])
    arrival_delay_prob = [_ / sum(arrival_delay_count) for _ in arrival_delay_count]
    h_y = entropy(arrival_delay_prob, base=2)

    if h_y < 1:
        root.is_leaf = True
        root.prediction = int(_df.mode(axis=0).at[0, 8])
        return

    conditional_entropies = [float('inf')] * (len(catagory_map) - 1)
    for k, v in catagory_map.items():  # iterate through all the possible attributes
        if v > 7:
            continue
        if available_attributes[v]:
            h_y_V = []
            for val in catagories[v]:  # iterate through all the possible values for an attribute
                # X = v
                _df_ = _df[_df[v] == val]
                if len(_df_) == 0:
                    continue
                # compute H(Y|X=v)
                conditional_arrival_delay_count = [0] * len(time_level_map)
                for i, _ in enumerate(time_level_map.values()):
                    conditional_arrival_delay_count[i] = len(_df_[_df_[catagory_map['arrival_delay']] == _])
                conditional_arrival_delay_prob = [_ / sum(conditional_arrival_delay_count) for _ in
                                                  conditional_arrival_delay_count]
                h_y_v = entropy(conditional_arrival_delay_prob, base=2)
                h_y_V.append((h_y_v, len(_df_) / len(_df)))
            # compute H(Y|X)
            h_y_x = 0
            for a, b in h_y_V:
                h_y_x += a * b
            conditional_entropies[v] = h_y_x
    max_ig, split_attr = float('-inf'), -1
    for i in range(len(conditional_entropies)):
        if h_y - conditional_entropies[i] > max_ig:
            max_ig = h_y - conditional_entropies[i]
            split_attr = i
    root.split_attr = split_attr
    available_attributes[split_attr] = False
    for idx, val in enumerate(catagories[split_attr]):
        new_id = root.id + '#' + str(idx)
        for _ in range(len(df)):
            if df.at[_, split_attr] == val and df.at[_, 9] == root.id:
                df.at[_, 9] = new_id
        root.children[val] = Node(new_id, root.level + 1)
        root.children[val].split_attr = split_attr
        build_subtree_in_memory(df, root.children[val], deepcopy(available_attributes), max_level)


def find_best_split(rdd, available_attributes):
    # arrival_delay_cnt = rdd.map(lambda x: (x[8], 1)).groupByKey().mapValues(len).map(lambda x: x[1]).collect()
    arrival_delay_cnt = rdd.map(lambda x: (x[8], 1)).reduceByKey(add).map(lambda x: x[1]).collect()
    arrival_delay_prob = [_ / sum(arrival_delay_cnt) for _ in arrival_delay_cnt]
    h_y = entropy(arrival_delay_prob, base=2)
    if h_y < 1:
        return -1
    total_count = rdd.count()
    splits = dict()
    for attr, values in catagories.items():
        if attr < len(available_attributes) and available_attributes[attr]:
            h_y_v = []
            for value in values:
                rdd_value = rdd.filter(lambda x: x[attr] == value)
                # value_cnt = rdd_value.map(lambda x: (x[8], 1)).groupByKey().mapValues(len).map(lambda x: x[1]).collect()
                value_cnt = rdd_value.map(lambda x: (x[8], 1)).reduceByKey(add).map(lambda x: x[1]).collect()
                value_prob = [_ / sum(value_cnt) for _ in value_cnt]
                h_y_v.append((rdd_value.count() / total_count, entropy(value_prob, base=2)))
            h_y_x = 0
            for k, v in h_y_v:
                h_y_x += k * v
            splits[attr] = h_y_x
    best_split, best_ig = -1, float('-inf')
    for attr, entro in splits.items():
        if h_y - entro > best_ig:
            best_ig = h_y - entro
            best_split = attr
    return best_split


def get_mode(rdd):
    return sorted(rdd.map(lambda x: (x[8], 1)).reduceByKey(add).collect(), key=lambda x: x[1])[-1][0]


def build_tree(rdd, root: Node, max_level: int):
    queue = deque()
    level = 0
    attributes = [True] * (len(catagory_map) - 1)
    # queue.append((root, attributes))
    rdd.persist(pyspark.StorageLevel.MEMORY_AND_DISK)
    queue.append((root, attributes, rdd))
    while queue:
        size = len(queue)
        level += 1
        for _ in range(size):
            # cur, available_attributes = queue.popleft()
            cur, available_attributes, cur_rdd = queue.popleft()
            cnt = cur_rdd.count()
            # print(f"current rdd size: {cnt}")
            if 0 < cnt <= 5000:
                df = pd.DataFrame(cur_rdd.collect())
                build_subtree_in_memory(df, cur, deepcopy(available_attributes), max_level)
            elif cnt > 5000:
                split_attr = find_best_split(cur_rdd, available_attributes)
                if split_attr == -1:
                    cur.is_leaf = True
                    cur.prediction = get_mode(cur_rdd)
                    continue
                available_attributes[split_attr] = False
                for value in catagories[split_attr]:
                    new_id = cur.id + '#' + str(value)
                    child = Node(new_id, level)
                    child.split_attr = split_attr
                    cur.children[value] = child
                    # rdd = rdd.map(
                    #     lambda x: x[:9] + [new_id] if x[-1] == cur.id and x[split_attr] == value else x).persist()
                    new_rdd = cur_rdd.filter(lambda x: x[split_attr] == value).map(
                        lambda x: x[:9] + [new_id]).persist(pyspark.StorageLevel.MEMORY_AND_DISK)
                    # print(f"attribute {split_attr}, value {value}, number ", new_rdd.count())
                    # queue.append((child, deepcopy(available_attributes)))
                    if new_rdd.count() > 0:
                        queue.append((child, deepcopy(available_attributes), new_rdd))
                    else:
                        new_rdd.unpersist()
            cur_rdd.unpersist()


def find_prediction(model, data: List):
    if not data or len(data) != 7:
        return -1
    cur = model
    while cur and not cur.is_leaf:
        cur = cur.children[cur.split_attribute]
    if not cur:
        return -1
    else:
        return cur.prediction


if __name__ == '__main__':
    appName = "FlightDelayPrediction"
    conf = SparkConf() \
            .setMaster("local[11]") \
            .set("spark.executor.memory", "16g") \
            .set("spark.driver.memory", "16g") \
            .set("spark.memory.offHeap.enabled",True) \
            .set("spark.memory.offHeap.size","16g") \
            .setAppName(appName)

    sc = SparkContext(conf=conf)
    start = time.time()
    model = Node('root', 0)
    available_attributes = [True] * (len(catagory_map) - 1)
    data_path = './data/train.csv'
    # data_path = './data/half.csv'
    origin_data = sc.textFile(data_path).map(lambda line: line.split(',')).map(
        lambda x: [eval(item) for item in x] + ['root'])
    build_tree(origin_data, model, 4)
    end = time.time()
    total_time = end - start
    print(f"Total time: {total_time // 60} min {total_time % 60} s")
    print(model)
    sc.stop()
