import json
import time
import random
import hashlib
import uuid
import threading
import socket
from typing import Dict, List, Optional
import requests
import bisect

# --- 全局配置 ---
# 移除 EMQX 相关配置
# 新增 TCP 服务器配置
TCP_HOST = "0.0.0.0"  # 监听所有网络接口
TCP_PORT_BASE = 6000  # TCP 端口基数，每个节点将使用 TCP_PORT_BASE + 节点编号

# Gossip协议配置
GOSSIP_INTERVAL_SEC = 2
HEARTBEAT_INTERVAL_SEC = 5
NODE_TIMEOUT_SEC = 15

# 一致性哈希配置
VIRTUAL_NODES = 4
RING_SIZE = 2 ** 3

# --- 工具函数 ---
def hash_key(key: str) -> int:
    md5 = hashlib.md5(key.encode("utf-8"))
    return int(md5.hexdigest(), 16) % RING_SIZE

# --- 核心节点类 ---
class GossipNode:
    def __init__(self, node_id: str, http_port: int, seed_nodes: Optional[List[str]] = None):
        self.node_id = node_id
        self.http_port = http_port
        self.http_url = f"http://localhost:{http_port}"

        self.members: Dict[str, Dict] = {
            self.node_id: {"url": self.http_url, "last_seen": time.time()}
        }
        self.members_lock = threading.Lock()

        self.virtual_node_hashes: List[int] = []
        self.virtual_node_map: Dict[int, str] = {}
        self.hash_ring_lock = threading.Lock()

        self.message_store: Dict[str, Dict] = {}
        self.message_lock = threading.Lock()

        self.seed_nodes = seed_nodes or []

        # 初始化一致性哈希环
        self._update_hash_ring()

    # --- 集群成员管理 ---
    def merge_members(self, incoming_members: Dict[str, Dict]):
        print("merging members")

        current_time = time.time()
        updated = False
        print("merging members unlock")
        for node_id, info in incoming_members.items():
            if not info or "url" not in info:
                continue
            if node_id not in self.members:
                print(f"[{self.node_id}] 发现新节点：{node_id}（URL：{info['url']}）")
                updated = True
            self.members[node_id] = {
                "url": info["url"],
                "last_seen": current_time
            }
        self._clean_timeout_members()
        if updated:
            self._update_hash_ring()
        print("merged members end")

    def _clean_timeout_members(self):
        current_time = time.time()
        timeout_nodes = []

        with self.members_lock:
            for node_id, info in self.members.items():
                if node_id == self.node_id:
                    continue
                if current_time - info["last_seen"] > NODE_TIMEOUT_SEC:
                    timeout_nodes.append(node_id)
            for node_id in timeout_nodes:
                print(f"[{self.node_id}] 节点 {node_id} 超时，从集群中移除")
                del self.members[node_id]

        if timeout_nodes:
            self._update_hash_ring()


    def get_node_url(self, node_id: str) -> Optional[str]:
        with self.members_lock:
            return self.members.get(node_id, {}).get("url")

    # --- 一致性哈希环管理 ---
    def _update_hash_ring(self):
        with self.hash_ring_lock:
            self.virtual_node_hashes.clear()
            self.virtual_node_map.clear()

            with self.members_lock:
                for node_id in self.members.keys():
                    for i in range(VIRTUAL_NODES):
                        virtual_node_key = f"{node_id}#{i}"
                        virtual_hash = hash_key(virtual_node_key)
                        self.virtual_node_hashes.append(virtual_hash)
                        self.virtual_node_map[virtual_hash] = node_id

            self.virtual_node_hashes.sort()

        print(f"[{self.node_id}] 一致性哈希环更新完成，当前虚拟节点数：{len(self.virtual_node_hashes)},node 映射{self.virtual_node_map}")

    def get_responsible_node(self, message_id: str) -> Optional[str]:
        if not message_id:
            return None

        message_hash = hash_key(message_id)

        with self.hash_ring_lock:
            if not self.virtual_node_hashes:
                return None

            idx = bisect.bisect_left(self.virtual_node_hashes, message_hash)
            if idx == len(self.virtual_node_hashes):
                idx = 0

            virtual_hash = self.virtual_node_hashes[idx]
            return self.virtual_node_map[virtual_hash]

    def should_process(self, message_id: str) -> bool:
        responsible_node = self.get_responsible_node(message_id)
        return responsible_node == self.node_id

    # --- Gossip协议核心逻辑 ---
    def _send_gossip(self):
        target_url = None
        with self.members_lock:
            peers = [info["url"] for node_id, info in self.members.items() if node_id != self.node_id]
        print("peers:", peers)
        if peers:
            target_url = random.choice(peers)
        elif self.seed_nodes:
            target_url = random.choice(self.seed_nodes)

        if not target_url:
            return

        try:
            with self.members_lock:
                gossip_msg = json.dumps({"members": self.members})

            response = requests.post(
                f"{target_url}/gossip",
                json=gossip_msg,
                headers={"Content-Type": "application/json"},
                timeout=6
            )
            if response.status_code != 200:
                print(f"[{self.node_id}] 向 {target_url} 发送Gossip失败，状态码：{response.status_code}")
        except requests.exceptions.RequestException as e:
            print(f"[{self.node_id}] 向 {target_url} 发送Gossip异常：{str(e)}")

    def _gossip_loop(self):
        last_gossip_sent = 0
        last_heartbeat = 0
        last_clean_timeout = 0

        while True:
            now = time.time()

            if now - last_gossip_sent > GOSSIP_INTERVAL_SEC:
                self._send_gossip()
                last_gossip_sent = now

            if now - last_heartbeat > HEARTBEAT_INTERVAL_SEC:
                with self.members_lock:
                    self.members[self.node_id]["last_seen"] = now
                last_heartbeat = now

            if now - last_clean_timeout > NODE_TIMEOUT_SEC / 3:
                self._clean_timeout_members()
                last_clean_timeout = now
            print("gossip 在执行中======================")
            time.sleep(1)

    # --- 节点启动与停止 ---
    def start(self):
        print(f"\n[{self.node_id}] 节点启动中...")

        gossip_thread = threading.Thread(target=self._gossip_loop, daemon=True)
        gossip_thread.start()
        print(f"[{self.node_id}] Gossip协议循环启动成功")

        # # 保持主线程运行
        # try:
        #     while True:
        #         time.sleep(1)
        # except KeyboardInterrupt:
        #     print(f"\n[{self.node_id}] 收到停止信号，正在关闭...")
        #     print(f"[{self.node_id}] 节点已关闭")

if __name__ == "__main__":
    import sys

    # 命令行参数：python gossip_node.py <node_id> <http_port> <tcp_port>
    # if len(sys.argv) != 4:
    #     print("用法：python gossip_node.py <节点ID> <HTTP端口> <TCP端口>")
    #     print("示例：python gossip_node.py node1 5001 6001")
    #     sys.exit(1)

    # node_id = sys.argv[1]
    # http_port = int(sys.argv[2])
    # tcp_port = int(sys.argv[3])

    node_id = "node1"
    http_port = 5001
    tcp_port = 8888

    # 种子节点：默认将第一个节点（node1，HTTP端口5001）作为种子
    seed_nodes = ["http://localhost:5001"] if http_port != 5001 else None

    node = GossipNode(
        node_id=node_id,
        http_port=http_port,
        seed_nodes=seed_nodes
    )
    node.start()
    print("=====")