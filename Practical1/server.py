# SERVER
import socket
import os

SERVER_HOST = '127.0.0.1'  
SERVER_PORT = 65432       
BUFFER_SIZE = 4096         

def start_server():
    # Create socket
    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    s.bind((SERVER_HOST, SERVER_PORT))
    s.listen(5)
    print(f"[*] Listening on {SERVER_HOST}:{SERVER_PORT}")
    
    # Accept connection
    client_socket, address = s.accept()
    print(f"[*] Client connected from {address}")

    # File info
    received = client_socket.recv(BUFFER_SIZE).decode()
    filename, filesize_str = received.split('|')
    filesize = int(filesize_str)
    filename = os.path.basename(filename)
    
    print(f"[*] File: {filename}, Size: {filesize} bytes")

    # Receive file data
    bytes_received = 0
    with open("received_" + filename, "wb") as f:
        while bytes_received < filesize:
            remaining_bytes = filesize - bytes_received
            
            bytes_to_read = min(BUFFER_SIZE, remaining_bytes)
            bytes_read = client_socket.recv(bytes_to_read)
            
            if not bytes_read:
                break
            
            f.write(bytes_read)
            bytes_received += len(bytes_read)
            print(f"\rReceived {bytes_received}/{filesize} bytes", end="")
            
    # Close connection
    print(f"\n[+] Sending '{filename}' completed.")
    client_socket.close()
    s.close()

start_server()