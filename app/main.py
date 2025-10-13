import socket  # noqa: F401
import struct

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

        request_api_version_bytes = data[6:8]
        (request_api_version, ) = struct.unpack('>h', request_api_version_bytes)
        print(f"request_api_version = {request_api_version}")
    else:
        correlation_id_bytes = b'\x00\x00\x00\x00'
        request_api_version = -1
    

    if 0 <= request_api_version <= 4:
        error_code = 0
    else:
        error_code = 35
    
    message_size = b'\x00\x00\x00\x00'
    error_code_bytes = struct.pack(">h", error_code)  # short = 2 bytes, big-endian
    response = message_size + correlation_id_bytes + error_code_bytes
    conn.sendall(response)
    
    conn.close()


if __name__ == "__main__":
    main()
