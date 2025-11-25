import json
import time
import uuid
import paho.mqtt.client as mqtt

EMQX_BROKER = "localhost"
EMQX_PORT = 1883
EMQX_TOPIC = "distributed/topic"

def on_connect(client, userdata, flags, rc):
    print("连接EMQX成功！" if rc == 0 else f"连接失败，状态码：{rc}")

def main():
    client = mqtt.Client(client_id="publisher")
    client.on_connect = on_connect
    client.connect(EMQX_BROKER, EMQX_PORT, 60)

    try:
        count = 0
        while True:
            message_id = str(uuid.uuid4())  # 唯一ID
            message = {
                "id": message_id,
                "data": f"测试消息_{count}",
                "timestamp": time.strftime("%Y-%m-%d %H:%M:%S")
            }
            client.publish(EMQX_TOPIC, json.dumps(message))
            print(f"发布消息：id={message_id}，data={message['data']}")
            count += 1
            time.sleep(1)  # 每秒发布1条
    except KeyboardInterrupt:
        print("\n发布者停止，断开EMQX连接...")
        client.disconnect()

if __name__ == "__main__":
    main()