import socket
import struct
import threading

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

def print_bytes_info(label, b):
    print(f"{label} (len={len(b)}):")
    print("  readable:", b)
    print("  hex:     ", b.hex())
    print()

def handle_client(conn, addr):
    print(f"Client connected: {addr}\n")
    while True:
        data = conn.recv(1024)
        if not data:
            print(f"Client {addr} disconnected.")
            break

        print_bytes_info("Received request", data)

        if len(data) < 14:
            print("Invalid or too short Kafka request")
            continue

        message_size = struct.unpack(">i", data[0:4])[0]
        api_key = struct.unpack(">h", data[4:6])[0]
        api_version = struct.unpack(">h", data[6:8])[0]
        correlation_id = struct.unpack(">i", data[8:12])[0]

        print(f"[Request Info]")
        print(f"  message_size = {message_size} bytes (rest of message)")
        print(f"  api_key = {api_key} (should be 18 for ApiVersions)")
        print(f"  api_version = {api_version}")
        print(f"  correlation_id = {correlation_id}")

        try:
            client_id_len = struct.unpack(">h", data[12:14])[0]
            client_id = data[14:14 + client_id_len].decode("utf-8", errors="ignore")
            print(f"  client_id_len = {client_id_len}")
            print(f"  client_id = {client_id}")
        except Exception as e:
            print(f"  (client_id decode failed: {e})")
        print()

        if len(data) >= 12:
            correlation_id_bytes = data[8:12]
            request_api_version_bytes = data[6:8]
            (request_api_version,) = struct.unpack('>h', request_api_version_bytes)
            print(f"request_api_version = {request_api_version}")
            print_bytes_info("CorrelationID bytes", correlation_id_bytes)
            print_bytes_info("Request API version bytes", request_api_version_bytes)
        else:
            correlation_id_bytes = b'\x00\x00\x00\x00'
            request_api_version = -1

        if 0 <= request_api_version <= 4:
            error_code = 0
        else:
            error_code = 35

        error_code_bytes = struct.pack(">h", error_code)
        print_bytes_info("ErrorCode bytes", error_code_bytes)

        # api_key = 18
        # min_version = 0
        # max_version = 4
        # api_key_entry = struct.pack(">hhh", api_key, min_version, max_version) + b'\x00'
        # print_bytes_info("Single ApiKey entry", api_key_entry)

        # api_keys_array = encode_unsigned_varint(2) + api_key_entry
        # print_bytes_info("ApiKeys array", api_keys_array)

        # # ApiVersions entry
        # api_key_18 = 18
        # min_version_18 = 0
        # max_version_18 = 4
        # api_key_entry_18 = struct.pack(">hhh", api_key_18, min_version_18, max_version_18) + b'\x00'

        # # DescribeTopicPartitions entry
        # api_key_75 = 75
        # min_version_75 = 0
        # max_version_75 = 0
        # api_key_entry_75 = struct.pack(">hhh", api_key_75, min_version_75, max_version_75) + b'\x00'

        api_entries = [
            (18, 0, 4, "ApiVersions"),
            (75, 0, 0, "DescribeTopicPartitions")
        ]

        api_keys_array = encode_unsigned_varint(len(api_entries) + 1)
        for k, min_v, max_v, name in api_entries:
            api_keys_array += struct.pack(">hhh", k, min_v, max_v) + b'\x00'

        # # Combine entries (array length = 2)
        # api_keys_array = encode_unsigned_varint(3) + api_key_entry_18 + api_key_entry_75
        print_bytes_info("ApiKeys array", api_keys_array)

        throttle_time_ms = struct.pack(">i", 0)
        print_bytes_info("ThrottleTimeMs bytes", throttle_time_ms)

        response_body = error_code_bytes + api_keys_array + throttle_time_ms + b'\x00'
        print_bytes_info("Response body", response_body)

        header_and_body = correlation_id_bytes + response_body
        print_bytes_info("Header + body", header_and_body)

        message_size_bytes = struct.pack(">i", len(header_and_body))
        print_bytes_info("Message size bytes", message_size_bytes)

        response = message_size_bytes + header_and_body

        print("\n[Response Info]")
        print(f"  Total response length = {len(response)} bytes")
        print(f"  message_size (prefix) = {len(header_and_body)} bytes")
        print(f"  correlation_id = {correlation_id}")
        print(f"  error_code = {error_code}")
        print(f"  api_keys_array_count = {len(api_entries)}")
        for k, min_v, max_v, name in api_entries:
            print(f"  api_key = {k} ({name}), min_version = {min_v}, max_version = {max_v}")

        conn.sendall(response)
        print("Response sent.\n")

    conn.close()
    print(f"Server closed connection: {addr}")

def main():
    print("Logs from your program will appear here!")
    server = socket.create_server(("localhost", 9092), reuse_port=True)
    print("Server listening on port 9092...\n")
    while True:
        conn, addr = server.accept()
        t = threading.Thread(target=handle_client, args=(conn, addr), daemon=True)
        t.start()

if __name__ == "__main__":
    main()
