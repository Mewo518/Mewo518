

import ray

# 初始化Ray
ray.init(address="auto")

# 定义一个用于处理数据的任务函数
@ray.remote(num_cpus=3)  # 每个节点只使用一个CPU核心来处理数据
def process_data(data):
    # 在这里处理数据，可以是任何需要的操作
    # 这里只是简单地打印数据
    # print("Processing data on node", ray.get_runtime_context().node_id, ":", data)
    no=0
    for num in data:
        no += num
    res = [ray.get_runtime_context().node_id,data,no]
    return res

if __name__ == "__main__":
    # 模拟一个数据集，这里使用简单的整数列表
    data_list = [1, 2, 3, 4, 5, 6, 7, 8, 9, 10]

    print("data_sets:",data_list)
    # 将数据集拆分为多个子集，并将每个子集分发到不同的节点并启动任务
    num_nodes = 5
    data_chunks = [data_list[i:i + len(data_list) // num_nodes] for i in range(0, len(data_list), len(data_list) // num_nodes)]

    # 将每个子集分发到不同的节点并启动任务
    tasks = [process_data.remote(chunk) for chunk in data_chunks]

    # 等待所有任务完成
    res = ray.get(tasks)
    print(res)

    nodes_info = ray.nodes()

    # 打印每个节点的信息
    for node_info in nodes_info:
        print("Node ID:", node_info["NodeID"])
        print("Node IP:", node_info["NodeManagerAddress"])
        print("Node Port:", node_info["NodeManagerPort"])
        print("Number of CPUs:", node_info["Resources"]["CPU"])
        print("Number of GPUs:", node_info["Resources"]["GPU"])

    # 关闭Ray
    ray.shutdown()



