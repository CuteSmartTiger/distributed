import json
import time
import random
import hashlib
import uuid
import threading
from typing import Dict, List, Optional, Tuple
import paho.mqtt.client as mqtt
import requests
from flask import Flask, request, jsonify
import bisect  # 用于一致性哈希的二分查找

# --- 全局配置 ---
# EMQX配置
EMQX_BROKER = "localhost"
EMQX_PORT = 1883
EMQX_TOPIC = "distributed/topic"

# Gossip协议配置
GOSSIP_INTERVAL_SEC = 2  # Gossip消息发送间隔
HEARTBEAT_INTERVAL_SEC = 5  # 心跳更新间隔
NODE_TIMEOUT_SEC = 15  # 节点超时时间（超过则认为下线）

# 一致性哈希配置
VIRTUAL_NODES = 10  # 每个真实节点对应的虚拟节点数（越多分布越均匀，迁移数据越少）
RING_SIZE = 2 ** 32  # 一致性哈希环的大小（2^32，模拟环形空间）

# --- 工具函数 ---
def hash_key(key: str) -> int:
    """计算字符串的哈希值，映射到 [0, RING_SIZE-1] 区间"""
    md5 = hashlib.md5(key.encode("utf-8"))
    return int(md5.hexdigest(), 16) % RING_SIZE

# --- 核心节点类 ---
class GossipNode:
    def __init__(self, node_id: str, http_port: int, seed_nodes: Optional[List[str]] = None):
        # 节点基础信息
        self.node_id = node_id
        self.http_port = http_port
        self.http_url = f"http://localhost:{http_port}"

        # 集群成员管理（线程安全）
        self.members: Dict[str, Dict] = {
            self.node_id: {"url": self.http_url, "last_seen": time.time()}
        }
        self.members_lock = threading.Lock()

        # 一致性哈希环（线程安全）
        self.virtual_node_hashes: List[int] = []  # 有序存储所有虚拟节点的哈希值
        self.virtual_node_map: Dict[int, str] = {}  # 虚拟节点哈希 -> 真实节点ID
        self.hash_ring_lock = threading.Lock()

        # 消息存储（存储当前节点处理过的消息，key: message_id, value: 消息内容）
        self.message_store: Dict[str, Dict] = {}
        self.message_lock = threading.Lock()

        # 种子节点
        self.seed_nodes = seed_nodes or []

        # MQTT客户端
        self.mqtt_client = mqtt.Client(client_id=f"node-{self.node_id}")
        self.mqtt_connected = False

        # 启动Flask HTTP服务器（接收Gossip消息和查询请求）
        self.app = self._init_flask_app()
        self._start_http_server_thread()

        # 初始化一致性哈希环（添加自身节点）
        self._update_hash_ring()

    def _init_flask_app(self) -> Flask:
        """初始化Flask应用（路由：/gossip 接收Gossip消息，/query/<id> 处理查询）"""
        app = Flask(f"GossipNode-{self.node_id}")

        # 1. 接收Gossip消息（更新集群成员）
        @app.route("/gossip", methods=["POST"])
        def handle_gossip():
            try:
                data = request.get_json()
                if not data or "members" not in data:
                    return jsonify({"error": "无效请求：缺少 members 字段"}), 400
                # 合并外部成员列表
                self._merge_members(data["members"])
                return jsonify({"status": "ok"}), 200
            except Exception as e:
                return jsonify({"error": str(e)}), 500

        # 2. 处理分布式查询（根据message_id查询数据）
        @app.route("/query/<message_id>", methods=["GET"])
        def handle_query(message_id: str):
            try:
                # 第一步：找到message_id对应的负责节点
                target_node_id = self._get_responsible_node(message_id)
                if not target_node_id:
                    return jsonify({"error": "集群中无可用节点"}), 503

                # 第二步：判断是否是当前节点负责
                if target_node_id == self.node_id:
                    # 自身负责，从本地存储查询
                    with self.message_lock:
                        message = self.message_store.get(message_id)
                    if message:
                        return jsonify({
                            "status": "success",
                            "data": message,
                            "handled_by": self.node_id
                        }), 200
                    else:
                        return jsonify({
                            "status": "fail",
                            "error": f"节点 {self.node_id} 未处理过该ID：{message_id}"
                        }), 404

                # 第三步：不是自身负责，转发到目标节点
                target_node_url = self._get_node_url(target_node_id)
                if not target_node_url:
                    return jsonify({"error": f"目标节点 {target_node_id} 不可达"}), 503

                # 转发查询请求
                response = requests.get(
                    f"{target_node_url}/query/{message_id}",
                    timeout=5
                )
                # 将目标节点的响应直接返回给客户端
                return (response.text, response.status_code, response.headers.items())

            except requests.exceptions.RequestException as e:
                return jsonify({"error": f"转发查询失败：{str(e)}"}), 502
            except Exception as e:
                return jsonify({"error": str(e)}), 500

        return app

    def _start_http_server_thread(self):
        """在独立线程中启动Flask服务器（避免阻塞主逻辑）"""
        def run_server():
            # use_reloader=False 避免重复启动，threaded=True 支持并发请求
            self.app.run(host="0.0.0.0", port=self.http_port, threaded=True, use_reloader=False)

        server_thread = threading.Thread(target=run_server, daemon=True)
        server_thread.start()
        time.sleep(1)  # 等待服务器启动完成
        print(f"[{self.node_id}] HTTP服务器启动成功，端口：{self.http_port}（路由：/gossip, /query/<id>）")

    # --- 集群成员管理 ---
    def _merge_members(self, incoming_members: Dict[str, Dict]):
        """合并外部成员列表到本地，更新后触发哈希环更新"""
        with self.members_lock:
            current_time = time.time()
            updated = False
            for node_id, info in incoming_members.items():
                # 过滤无效信息
                if not info or "url" not in info:
                    continue
                # 更新成员的最后活跃时间
                if node_id not in self.members:
                    print(f"[{self.node_id}] 发现新节点：{node_id}（URL：{info['url']}）")
                    updated = True
                self.members[node_id] = {
                    "url": info["url"],
                    "last_seen": current_time
                }
            # 清理超时节点
            self._clean_timeout_members()
            # 成员变化时更新哈希环
            if updated:
                self._update_hash_ring()

    def _clean_timeout_members(self):
        """清理超时的节点（超过 NODE_TIMEOUT_SEC 未活跃）"""
        current_time = time.time()
        timeout_nodes = []
        with self.members_lock:
            for node_id, info in self.members.items():
                if node_id == self.node_id:
                    continue  # 跳过自身
                if current_time - info["last_seen"] > NODE_TIMEOUT_SEC:
                    timeout_nodes.append(node_id)
            # 删除超时节点
            for node_id in timeout_nodes:
                print(f"[{self.node_id}] 节点 {node_id} 超时，从集群中移除")
                del self.members[node_id]
        # 节点删除后更新哈希环
        if timeout_nodes:
            self._update_hash_ring()

    def _get_node_url(self, node_id: str) -> Optional[str]:
        """根据节点ID获取其HTTP URL"""
        with self.members_lock:
            return self.members.get(node_id, {}).get("url")

    # --- 一致性哈希环管理 ---
    def _update_hash_ring(self):
        """根据当前成员列表更新一致性哈希环（包含虚拟节点）"""
        with self.hash_ring_lock:
            # 清空旧的哈希环
            self.virtual_node_hashes.clear()
            self.virtual_node_map.clear()

            # 为每个真实节点添加虚拟节点
            with self.members_lock:
                for node_id in self.members.keys():
                    for i in range(VIRTUAL_NODES):
                        # 虚拟节点命名：node_id#虚拟节点索引（避免冲突）
                        virtual_node_key = f"{node_id}#{i}"
                        virtual_hash = hash_key(virtual_node_key)
                        # 添加到哈希环（后续会排序）
                        self.virtual_node_hashes.append(virtual_hash)
                        # 映射虚拟节点到真实节点
                        self.virtual_node_map[virtual_hash] = node_id

            # 对虚拟节点哈希值排序（保证环形有序）
            self.virtual_node_hashes.sort()

        print(f"[{self.node_id}] 一致性哈希环更新完成，当前虚拟节点数：{len(self.virtual_node_hashes)}")

    def _get_responsible_node(self, message_id: str) -> Optional[str]:
        """找到message_id对应的负责节点（一致性哈希核心逻辑）"""
        if not message_id:
            return None

        # 计算message_id的哈希值
        message_hash = hash_key(message_id)

        with self.hash_ring_lock:
            # 哈希环为空（无可用节点）
            if not self.virtual_node_hashes:
                return None

            # 二分查找第一个大于等于message_hash的虚拟节点哈希
            idx = bisect.bisect_left(self.virtual_node_hashes, message_hash)

            # 如果idx等于环的长度，说明落在最后一个节点之后，取第一个节点（环形特性）
            if idx == len(self.virtual_node_hashes):
                idx = 0

            # 获取对应的虚拟节点哈希，再映射到真实节点ID
            virtual_hash = self.virtual_node_hashes[idx]
            return self.virtual_node_map[virtual_hash]

    def _should_process(self, message_id: str) -> bool:
        """判断当前节点是否应该处理该message_id（基于一致性哈希）"""
        responsible_node = self._get_responsible_node(message_id)
        return responsible_node == self.node_id

    # --- Gossip协议核心逻辑 ---
    def _send_gossip(self):
        """向随机一个集群节点发送Gossip消息（同步成员列表）"""
        # 选择发送目标：优先从已知成员中选，无则选种子节点
        target_url = None
        with self.members_lock:
            peers = [info["url"] for node_id, info in self.members.items() if node_id != self.node_id]
        if peers:
            target_url = random.choice(peers)
        elif self.seed_nodes:
            target_url = random.choice(self.seed_nodes)

        if not target_url:
            return  # 无目标节点，跳过发送

        try:
            # 构建Gossip消息（包含当前节点的成员列表）
            with self.members_lock:
                gossip_msg = json.dumps({"members": self.members})

            # 发送POST请求
            response = requests.post(
                f"{target_url}/gossip",
                data=gossip_msg,
                headers={"Content-Type": "application/json"},
                timeout=2
            )
            if response.status_code == 200:
                pass  # 发送成功，无需打印（避免日志冗余）
            else:
                print(f"[{self.node_id}] 向 {target_url} 发送Gossip失败，状态码：{response.status_code}")
        except requests.exceptions.RequestException as e:
            print(f"[{self.node_id}] 向 {target_url} 发送Gossip异常：{str(e)}")

    def _gossip_loop(self):
        """Gossip协议主循环（定期发送消息、更新心跳、清理超时）"""
        last_gossip_sent = 0
        last_heartbeat = 0
        last_clean_timeout = 0

        while True:
            now = time.time()

            # 1. 定期发送Gossip消息
            if now - last_gossip_sent > GOSSIP_INTERVAL_SEC:
                self._send_gossip()
                last_gossip_sent = now

            # 2. 定期更新自身心跳（避免被其他节点判定为超时）
            if now - last_heartbeat > HEARTBEAT_INTERVAL_SEC:
                with self.members_lock:
                    self.members[self.node_id]["last_seen"] = now
                last_heartbeat = now

            # 3. 定期清理超时节点
            if now - last_clean_timeout > NODE_TIMEOUT_SEC / 3:
                self._clean_timeout_members()
                last_clean_timeout = now

            time.sleep(1)  # 降低循环频率，减少资源消耗

    # --- MQTT消息处理 ---
    def _on_mqtt_connect(self, client, userdata, flags, rc):
        """MQTT连接成功回调"""
        if rc == 0:
            self.mqtt_connected = True
            print(f"[{self.node_id}] 连接EMQX成功（broker：{EMQX_BROKER}:{EMQX_PORT}）")
            # 订阅目标主题
            client.subscribe(EMQX_TOPIC)
            print(f"[{self.node_id}] 已订阅主题：{EMQX_TOPIC}")
        else:
            print(f"[{self.node_id}] 连接EMQX失败，状态码：{rc}")

    def _on_mqtt_message(self, client, userdata, msg):
        """MQTT接收消息回调"""
        try:
            # 解析消息体（要求包含id字段）
            payload = json.loads(msg.payload)
            message_id = payload.get("id")
            if not message_id:
                print(f"[{self.node_id}] 接收无效消息：缺少 id 字段（payload：{payload}）")
                return

            # 判断是否由当前节点处理
            if self._should_process(message_id):
                print(f"[{self.node_id}] 处理消息：id={message_id}，data={payload.get('data')}")
                # 存储消息到本地（用于后续查询）
                with self.message_lock:
                    self.message_store[message_id] = {
                        "payload": payload,
                        "processed_at": time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())
                    }
            else:
                # 不处理，仅打印日志（可选）
                responsible_node = self._get_responsible_node(message_id)
                print(f"[{self.node_id}] 跳过消息：id={message_id}（负责节点：{responsible_node}）")
        except json.JSONDecodeError:
            print(f"[{self.node_id}] 接收无效JSON消息：{msg.payload}")

    # --- 节点启动与停止 ---
    def start(self):
        """启动节点（Gossip循环 + MQTT循环）"""
        print(f"\n[{self.node_id}] 节点启动中...")

        # 启动Gossip协议循环
        gossip_thread = threading.Thread(target=self._gossip_loop, daemon=True)
        gossip_thread.start()
        print(f"[{self.node_id}] Gossip协议循环启动成功")

        # 连接MQTT并启动消息循环
        self.mqtt_client.on_connect = self._on_mqtt_connect
        self.mqtt_client.on_message = self._on_mqtt_message
        self.mqtt_client.connect(EMQX_BROKER, EMQX_PORT, 60)  # 60秒心跳超时

        # 阻塞式运行MQTT循环（直到手动停止）
        try:
            self.mqtt_client.loop_forever()
        except KeyboardInterrupt:
            print(f"\n[{self.node_id}] 收到停止信号，正在关闭...")
            self.mqtt_client.disconnect()
            print(f"[{self.node_id}] 节点已关闭")

if __name__ == "__main__":
    import sys

    # 命令行参数：python gossip_node.py <node_id> <http_port>
    if len(sys.argv) != 3:
        print("用法：python gossip_node.py <节点ID> <HTTP端口>")
        print("示例：python gossip_node.py node1 5001")
        sys.exit(1)

    node_id = sys.argv[1]
    http_port = int(sys.argv[2])

    # 种子节点：默认将第一个节点（node1，端口5001）作为种子
    seed_nodes = ["http://localhost:5001"] if http_port != 5001 else None

    # 创建并启动节点
    node = GossipNode(node_id=node_id, http_port=http_port, seed_nodes=seed_nodes)
    node.start()