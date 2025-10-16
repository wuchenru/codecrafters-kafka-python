import socket  # noqa: F401
import struct

def encode_unsigned_varint(n):
    out = bytearray()
    while True:
        b = n & 0x7F
        n >>= 7
        if n == 0:
            out.append(b)
            break
        else:
            out.append(b | 0x80)
    return bytes(out)

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
        (request_api_version,) = struct.unpack('>h', request_api_version_bytes)
        print(f"request_api_version = {request_api_version}")
    else:
        correlation_id_bytes = b'\x00\x00\x00\x00'
        request_api_version = -1

    # error_code
    if 0 <= request_api_version <= 4:
        error_code = 0
    else:
        error_code = 35

    # ApiVersions Response v4 body
    error_code_bytes = struct.pack(">h", error_code)

    # ApiKeys array，1 个元素 ApiKey=18, MinVersion=0, MaxVersion=4
    api_key = 18
    min_version = 0
    max_version = 4
    api_key_entry = struct.pack(">hhh", api_key, min_version, max_version)
    api_keys_array = encode_unsigned_varint(1) + api_key_entry

    throttle_time_ms = struct.pack(">i", 0)

    response_body = error_code_bytes + api_keys_array + throttle_time_ms + b'\x00'  # tag buffer as required by v4

    # header + body
    header_and_body = correlation_id_bytes + response_body

    # message_size
    message_size_bytes = struct.pack(">i", len(header_and_body))

    response = message_size_bytes + header_and_body

    # 
    conn.sendall(response)
    conn.close()
    print("Response sent, connection closed.")

if __name__ == "__main__":
    main()
