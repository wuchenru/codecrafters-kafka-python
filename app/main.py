import socket  # noqa: F401


def main():
    # You can use print statements as follows for debugging,
    # they'll be visible when running tests.
    print("Logs from your program will appear here!")

    # Uncomment this to pass the first stage
    #
    server = socket.create_server(("localhost", 9092), reuse_port=True)
    server.accept() # wait for client
    conn, addr = server.accept()
    
    data = conn.recv(1024)  
    
    response = b'\x00\x00\x00\x00' + b'\x00\x00\x00\x07'
    
    conn.sendall(response)
    
    conn.close()


if __name__ == "__main__":
    main()
