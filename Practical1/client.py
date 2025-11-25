# CLIENT
import socket
import os

SERVER_HOST = '127.0.0.1'
SERVER_PORT = 65432
BUFFER_SIZE = 4096
FILE_TO_SEND = "test_file.txt" 

def start_client():
    # Create socket
    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    print(f"[*] Connecting to {SERVER_HOST}:{SERVER_PORT}...")
    s.connect((SERVER_HOST, SERVER_PORT))
    print("[+] Connected.")
    
    # Get file info
    filesize = os.path.getsize(FILE_TO_SEND)
    filename = os.path.basename(FILE_TO_SEND)
    
    # File info
    header = f"{filename}|{filesize}"
    s.send(header.encode())
    
    print(f"[*] File: {filename}, Size: {filesize} bytes")

    # Send file data
    bytes_sent = 0
    with open(FILE_TO_SEND, "rb") as f:
        while True:
            bytes_read = f.read(BUFFER_SIZE)
            if not bytes_read:
                break
            
            s.sendall(bytes_read)
            bytes_sent += len(bytes_read)
            print(f"\rSent {bytes_sent}/{filesize} bytes", end="")

    # Close connection
    print(f"\n[+] Transfering '{filename}' completed.")
    s.close()

start_client()