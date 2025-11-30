import paho.mqtt.client as mqtt
import time
import json
import random

# -------------------------- 配置项 --------------------------
EMQX_BROKER = "localhost"
EMQX_PORT = 1883
MQTT_TOPIC = "sensor/temperature"
MQTT_CLIENT_ID = "python-producer-002"
MQTT_USERNAME = "admin"
MQTT_PASSWORD = "admin"
SEND_INTERVAL = 2  # 持续发送间隔（秒）
SEND_COUNT = 1000  # 批量发送次数（0 表示无限发送）


# -------------------------- 回调函数 --------------------------
def on_connect(client, userdata, flags, rc):
    if rc == 0:
        print(f"✅ 连接 EMQX 成功")
        client.connected_flag = True
    else:
        print(f"❌ 连接失败 (rc={rc})")
        client.connected_flag = False


# -------------------------- 持续发送逻辑 --------------------------
def continuous_send():
    # 初始化客户端
    client = mqtt.Client(client_id=MQTT_CLIENT_ID)
    client.username_pw_set(MQTT_USERNAME, MQTT_PASSWORD)
    client.on_connect = on_connect
    client.connected_flag = False  # 自定义连接状态标志

    # 连接 EMQX
    client.connect(EMQX_BROKER, EMQX_PORT, 60)
    client.loop_start()  # 启动后台循环（持续处理网络事件）

    # 等待连接成功
    while not client.connected_flag:
        time.sleep(0.1)

    # 开始发送消息
    count = 0
    try:
        while True:
            # 构造动态消息（模拟传感器数据）
            for i in range(50):
                message = {
                    "producer_id": f"temp-{i}",
                    "seq":count,
                    "temperature": round(random.uniform(20.0, 30.0), 1),  # 随机温度
                    "humidity": round(random.uniform(40.0, 70.0), 1),  # 随机湿度
                    "timestamp": time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())

                }
                # 转换为 JSON 字符串（推荐，便于解析）
                payload = json.dumps(message, ensure_ascii=False)

                # 发布消息
                result = client.publish(MQTT_TOPIC, payload, qos=1)
                result.wait_for_publish()

                count += 1
                print(f"📤 第 {count} 条消息发送成功：{payload}")
                time.sleep(SEND_INTERVAL)

            # 控制发送次数/间隔
            if SEND_COUNT > 0 and count >= SEND_COUNT:
                break
            time.sleep(SEND_INTERVAL)

    except KeyboardInterrupt:
        print("\n⚠️ 用户中断发送")
    finally:
        # 清理资源
        client.loop_stop()
        client.disconnect()
        print("🔌 已断开连接")


# -------------------------- 执行发送 --------------------------
if __name__ == "__main__":
    continuous_send()