import hashlib
import bisect

class ConsistentHash:
    def __init__(self, nodes=None, total_replicas=100):
        """
        初始化一致性哈希环
        :param nodes: 初始物理节点列表（如 ["192.168.1.100:8080", "192.168.1.101:8080"]）
        :param total_replicas: 总虚拟节点数（固定200，适配1/2/3节点场景）
        """
        self.total_replicas = total_replicas  # 总虚拟节点数（核心：保证少节点时总数足够）
        self.ring = []  # 有序存储虚拟节点的哈希值
        self.node_map = {}  # 虚拟节点哈希值 -> 物理节点的映射
        self.nodes = set()  # 已添加的物理节点集合

        # 初始化节点
        if nodes:
            for node in nodes:
                self.add_node(node)

    def _hash(self, data):
        """
        计算SHA-256哈希值（输出整数），统一接收字节串输入
        :param data: 字节串（bytes）
        :return: 哈希值（整数）
        """
        sha256 = hashlib.sha256()
        sha256.update(data)
        return int(sha256.hexdigest(), 16)

    def _gen_virtual_node_key(self, node, replica_idx):
        """
        生成虚拟节点的唯一标识（用于哈希），兼顾打散和可追溯
        :param node: 物理节点标识（字符串）
        :param replica_idx: 虚拟节点序号（整数）
        :return: 虚拟节点标识字节串（bytes）
        """
        # 第一步：先哈希物理节点ID，得到固定长度的字节串（二次打散第一步）
        node_hash_bytes = hashlib.sha256(node.encode('utf-8')).digest()
        # 第二步：拼接虚拟节点序号，生成唯一虚拟节点标识
        virtual_node_key = node_hash_bytes + str(replica_idx).encode('utf-8')
        return virtual_node_key

    def add_node(self, node):
        """添加物理节点，并生成对应的虚拟节点"""
        if node in self.nodes:
            print(f"节点 {node} 已存在，跳过添加")
            return
        self.nodes.add(node)

        # 计算当前节点数，分配每个物理节点的虚拟节点数（向上取整）
        node_count = len(self.nodes)
        replicas_per_node = (self.total_replicas + node_count - 1) // node_count  # 向上取整

        # 为当前节点生成指定数量的虚拟节点
        for idx in range(replicas_per_node):
            # 生成虚拟节点唯一标识并计算哈希
            virtual_key = self._gen_virtual_node_key(node, idx)
            virtual_hash = self._hash(virtual_key)

            # 避免哈希冲突（极端情况）
            if virtual_hash in self.node_map:
                print(f"虚拟节点哈希冲突：{virtual_hash}，重新生成序号 {idx+1000}")
                virtual_key = self._gen_virtual_node_key(node, idx + 1000)
                virtual_hash = self._hash(virtual_key)

            # 将虚拟节点加入环和映射表
            bisect.insort(self.ring, virtual_hash)  # 有序插入，避免后续排序
            self.node_map[virtual_hash] = node

        # 打印分布校验结果（便于调试）
        self._check_distribution()

    def remove_node(self, node):
        """移除物理节点（可选，完善功能）"""
        if node not in self.nodes:
            print(f"节点 {node} 不存在，跳过移除")
            return
        self.nodes.remove(node)

        # 找到该节点的所有虚拟节点并删除
        to_remove = [h for h, n in self.node_map.items() if n == node]
        for h in to_remove:
            del self.node_map[h]
            self.ring.remove(h)

    def get_node(self, key):
        """
        根据数据键找到对应的物理节点（顺时针最近原则）
        :param key: 数据键（字符串，如 "user_10086"）
        :return: 物理节点标识（字符串）
        """
        if not self.ring:
            raise ValueError("哈希环为空，请先添加节点")

        # 计算数据键的哈希值
        key_hash = self._hash(key.encode('utf-8'))

        # 二分查找第一个≥key_hash的虚拟节点哈希
        idx = bisect.bisect_left(self.ring, key_hash)
        # 若idx超出范围（key_hash大于所有虚拟节点哈希），取第一个节点
        idx = idx % len(self.ring)

        return self.node_map[self.ring[idx]]

    def _check_distribution(self):
        """校验各物理节点的环覆盖占比（调试用）"""
        if not self.ring:
            return
        node_coverage = {node: 0 for node in self.nodes}
        total_ring_size = 2 ** 256  # SHA-256哈希空间总大小

        # 计算每个虚拟节点区间的长度，并归属到对应物理节点
        prev_hash = 0
        for current_hash in self.ring:
            interval = current_hash - prev_hash
            node = self.node_map[current_hash]
            node_coverage[node] += interval
            prev_hash = current_hash
        # 处理最后一段：从最大哈希值到哈希环末尾（2^256）
        node_coverage[self.node_map[self.ring[0]]] += total_ring_size - prev_hash

        # 打印占比（保留2位小数）
        print("\n===== 节点分布校验 =====")
        for node, coverage in node_coverage.items():
            ratio = (coverage / total_ring_size) * 100
            print(f"节点 {node} 覆盖环占比：{ratio:.2f}%")
        print("=======================\n")


# -------------------------- 测试验证 --------------------------
if __name__ == "__main__":
    # 测试1：1个节点
    print("=== 测试1：1个物理节点 ===")
    ch1 = ConsistentHash(nodes=["192.168.1.100:8080"])
    # 测试10个数据键映射
    for i in range(10):
        key = f"user_{i}"
        print(f"键 {key} → 节点 {ch1.get_node(key)}")

    # 测试2：2个节点
    print("\n=== 测试2：2个物理节点 ===")
    ch2 = ConsistentHash(nodes=["192.168.1.100:8080", "192.168.1.101:8080"])
    # 统计1000条数据的分布
    count1, count2 = 0, 0
    for i in range(1000):
        node = ch2.get_node(f"user_{i}")
        if node == "192.168.1.100:8080":
            count1 += 1
        else:
            count2 += 1
    print(f"1000条数据分布：100节点={count1}条，101节点={count2}条（占比 {count1/10:.1f}% / {count2/10:.1f}%）")

    # 测试3：3个节点
    print("\n=== 测试3：3个物理节点 ===")
    ch3 = ConsistentHash(nodes=["192.168.1.100:8080", "192.168.1.101:8080", "192.168.1.102:8080"])
    # 统计1000条数据的分布
    count1, count2, count3 = 0, 0, 0
    for i in range(1000):
        node = ch3.get_node(f"user_{i}")
        if node == "192.168.1.100:8080":
            count1 += 1
        elif node == "192.168.1.101:8080":
            count2 += 1
        else:
            count3 += 1
    print(f"1000条数据分布：100节点={count1}条，101节点={count2}条，102节点={count3}条")
    print(f"占比：{count1/10:.1f}% / {count2/10:.1f}% / {count3/10:.1f}%")
