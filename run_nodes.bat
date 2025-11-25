@echo off
setlocal enabledelayedexpansion

echo [INFO] 停止之前的节点进程...
taskkill /F /IM python.exe /FI "COMMANDLINE eq *gossip_node.py*" >nul 2>&1
timeout /t 2 /nobreak >nul

:: 定义4个节点
set NODES[0]="node1 5001"
set NODES[1]="node2 5002"
set NODES[2]="node3 5003"
set NODES[3]="node4 5004"

echo [INFO] 启动新节点...
for /l %%i in (0,1,3) do (
    for /f "tokens=1,2 delims= " %%a in (!NODES[%%i]!) do (
        set NODE_ID=%%a
        set HTTP_PORT=%%b
    )
    echo [INFO] 启动 !NODE_ID! （HTTP端口：!HTTP_PORT!）
    start "!NODE_ID!" cmd /k "python gossip_node.py !NODE_ID! !HTTP_PORT! > !NODE_ID!.log 2>&1"
    timeout /t 2 /nobreak >nul
)

echo [INFO] 所有节点启动完成！日志文件：node1.log~node4.log
echo [INFO] 按任意键停止所有节点...
pause >nul

echo [INFO] 停止节点进程...
taskkill /F /IM python.exe /FI "COMMANDLINE eq *gossip_node.py*" >nul 2>&1
echo [INFO] 节点已全部停止。