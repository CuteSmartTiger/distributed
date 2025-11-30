import paho.mqtt.client as mqtt
import time
import json
import requests


from gossip_node import GossipNode


node_id = "node1"
http_port = 5001
tcp_port = 8888

# 种子节点：默认将第一个节点（node1，HTTP端口5001）作为种子
seed_nodes = [
    # "http://localhost:5001",
    "http://127.0.0.1:5002",
    "http://localhost:5003",
    # "http://localhost:5004"
]
node = GossipNode(
    node_id=node_id,
    http_port=http_port,
    seed_nodes=seed_nodes
)

node.start()


from flask import Flask, request, jsonify


app = Flask(f"GossipNode-{node_id}")

@app.route("/", methods=["GET"])
def handle_get_gossip():
    try:
        return jsonify({"status": "ok"}), 200
    except Exception as e:
        return jsonify({"error": str(e)}), 500

@app.route("/gossip", methods=["POST"])
def handle_gossip():
    try:
        data_json = request.get_json()
        data = json.loads(data_json)
        print("data",data)
        if not data or "members" not in data:
            return jsonify({"error": "无效请求：缺少 members 字段"}), 400
        node.merge_members(data["members"])
        return jsonify({"status": "ok"}), 200
    except Exception as e:
        print(e)
        return jsonify({"error": str(e)}), 500



@app.route("/query/<message_id>", methods=["GET"])
def handle_query(message_id: str):
    try:
        target_node_id = node.get_responsible_node(message_id)
        if not target_node_id:
            return jsonify({"error": "集群中无可用节点"}), 503

        if target_node_id == node_id:
            with node.message_lock:
                message = node.message_store.get(message_id)
            if message:
                return jsonify({
                    "status": "success",
                    "data": message,
                    "handled_by": node.node_id
                }), 200
            else:
                return jsonify({
                    "status": "fail",
                    "error": f"节点 {node.node_id} 未处理过该ID：{message_id}"
                }), 404

        target_node_url = node.get_node_url(target_node_id)
        if not target_node_url:
            return jsonify({"error": f"目标节点 {target_node_id} 不可达"}), 503

        response = requests.get(
            f"{target_node_url}/query/{message_id}",
            timeout=5
        )
        return (response.text, response.status_code, response.headers.items())

    except requests.exceptions.RequestException as e:
        return jsonify({"error": f"转发查询失败：{str(e)}"}), 502
    except Exception as e:
        return jsonify({"error": str(e)}), 500

import threading

def run_server():
    app.run(host="0.0.0.0", port=http_port, threaded=True, use_reloader=False)

server_thread = threading.Thread(target=run_server, daemon=True)
server_thread.start()
time.sleep(1)
print(f"[{node.node_id}] HTTP服务器启动成功，端口：{http_port}（路由：/gossip, /query/<id>）")




# -------------------------- 配置项 --------------------------
EMQX_BROKER = "localhost"  # EMQX 服务器地址
EMQX_PORT = 1883  # MQTT TCP 端口
MQTT_TOPIC = "sensor/temperature"  # 订阅的 Topic（支持通配符，如 sensor/#）
MQTT_CLIENT_ID = "python-consumer-{}".format(node_id)  # 消费者客户端 ID（需唯一）
# 认证信息（与 Producer 一致，若 EMQX 未配置则注释）
MQTT_USERNAME = "admin"
MQTT_PASSWORD = "admin"
QOS_LEVEL = 1  # 订阅的 QoS 级别（需与生产者匹配）


# -------------------------- 回调函数 --------------------------
# 连接成功回调：连接后立即订阅 Topic
def on_connect(client, userdata, flags, rc):
    if rc == 0:
        print(f"✅ 成功连接到 EMQX Broker (rc={rc})")
        # 订阅指定 Topic（可订阅多个，用列表传参：[(topic1, qos), (topic2, qos)]）
        client.subscribe(MQTT_TOPIC, qos=QOS_LEVEL)
        print(f"📥 已订阅 Topic: {MQTT_TOPIC} (QoS={QOS_LEVEL})")
    else:
        print(f"❌ 连接失败 (rc={rc})")




# 收到消息回调：核心处理逻辑（解析、存储、业务处理等）
def on_message(client, userdata, msg):
    payload = msg.payload.decode("utf-8")
    try:
        msg_json = json.loads(payload)
        if node.should_process(msg_json['producer_id']):
            print("当前节点需要处理消息",msg_json)
    except json.JSONDecodeError:
        print("   ⚠️  消息非 JSON 格式，跳过解析")


# 订阅成功回调（可选）
def on_subscribe(client, userdata, mid, granted_qos):
    print(f"✅ 订阅确认：Message ID={mid}，授予 QoS={granted_qos}")


from gossip_node import GossipNode

# -------------------------- 核心逻辑 --------------------------
def start_consumer():
    # 1. 创建 MQTT 客户端实例
    client = mqtt.Client(client_id=MQTT_CLIENT_ID, clean_session=True)

    # 2. 设置回调函数
    client.on_connect = on_connect
    client.on_message = on_message
    client.on_subscribe = on_subscribe

    # 3. 设置认证（若有）
    client.username_pw_set(MQTT_USERNAME, MQTT_PASSWORD)

    # 4. 连接 EMQX（阻塞连接）
    try:
        client.connect(EMQX_BROKER, EMQX_PORT, keepalive=60)
    except Exception as e:
        print(f"❌ 连接 EMQX 失败：{e}")
        return

    # 5. 启动客户端循环（阻塞模式，持续监听消息）
    # loop_forever()：阻塞主线程，直到调用 disconnect() 或异常退出
    # 替代方案：loop_start() + 无限循环（非阻塞，适合多线程）
    try:
        client.loop_forever()
    except KeyboardInterrupt:
        print("\n⚠️ 用户中断消费")
    finally:
        # 6. 断开连接（优雅退出）
        client.disconnect()
        print("🔌 已断开与 EMQX 的连接")


# -------------------------- 启动消费者 --------------------------
if __name__ == "__main__":
    print("🚀 启动 MQTT 消费者...")
    start_consumer()