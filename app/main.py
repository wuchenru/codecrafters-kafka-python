import socket

def main():
    print("Logs from your program will appear here!")
    
    server = socket.create_server(("localhost", 9092), reuse_port=True)
    
    # 只 accept 一次
    conn, addr = server.accept()
    
    # 读取请求内容（不解析即可）
    _ = conn.recv(1024)
    
    # 构造响应：message_size + correlation_id
    response = b'\x00\x00\x00\x00' + b'\x00\x00\x00\x07'
    conn.sendall(response)
    
    conn.close()

if __name__ == "__main__":
    main()
