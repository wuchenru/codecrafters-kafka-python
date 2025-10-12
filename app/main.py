import socket  # noqa: F401


def main():
    # You can use print statements as follows for debugging,
    # they'll be visible when running tests.
    print("Logs from your program will appear here!")

    # Uncomment this to pass the first stage
    #
    server = socket.create_server(("localhost", 9092), reuse_port=True)
    # server.accept() # wait for client
    conn, addr = server.accept()
    
    data = conn.recv(1024)  
    print(f"Received data: {data}")

    # based on the protocal of request messsage
    if len(data) >= 12:
        correlation_id_bytes = data[8:12]
    else:
        correlation_id_bytes = b'\x00\x00\x00\x00'
    
    response = b'\x00\x00\x00\x00' + correlation_id_bytes
    
    conn.sendall(response)
    
    conn.close()


if __name__ == "__main__":
    main()
