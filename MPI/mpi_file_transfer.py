import os
from mpi4py import MPI

BUFFER_SIZE = 4096
FILE = "test_file.txt" 

if not os.path.exists(FILE):
    with open(FILE, "w") as f:
        f.write("This is a test file for MPI transfer.\n")

comm = MPI.COMM_WORLD
rank = comm.Get_rank()
size = comm.Get_size()

if size < 2:
    if rank == 0:
        print("Error: Need at least 2 processes (Sender and Receiver) to run this program.")
        print("Use: mpirun -np 2 python mpi_file_transfer.py")
    exit()

SENDER_RANK = 1
RECEIVER_RANK = 0

def mpi_sender():
    try:
        # Get file info
        filesize = os.path.getsize(FILE)
        filename = os.path.basename(FILE)
        
        print(f"[*] Sender (Rank {rank}): Preparing to send '{filename}', Size: {filesize} bytes")

        # Send file info to the receiver
        file_info = f"{filename}|{filesize}"
        comm.send(file_info.encode(), dest=RECEIVER_RANK, tag=1)
        
        # Send file data
        bytes_sent = 0
        with open(FILE, "rb") as f:
            while True:
                bytes_read = f.read(BUFFER_SIZE)
                if not bytes_read:
                    break
                comm.send(bytes_read, dest=RECEIVER_RANK, tag=2) 
                bytes_sent += len(bytes_read)
                print(f"\rSender (Rank {rank}): Sent {bytes_sent}/{filesize} bytes", end="")

        comm.send(b'', dest=RECEIVER_RANK, tag=2)
        print(f"\n[+] Sender (Rank {rank}): Transfering '{filename}' completed.")

    except FileNotFoundError:
        print(f"\n[!] Sender (Rank {rank}): File not found: {FILE}")

def mpi_receiver():
    print(f"[*] Receiver (Rank {rank}): Waiting for file info from Rank {SENDER_RANK}...")
    
    # Receive file info
    file_info_encoded = comm.recv(source=SENDER_RANK, tag=1)
    received = file_info_encoded.decode()   
    filename, filesize_str = received.split('|')
    filesize = int(filesize_str)
    filename = os.path.basename(filename)   
    print(f"[*] Receiver (Rank {rank}): File: {filename}, Size: {filesize} bytes")

    # Receive file data
    bytes_received = 0
    with open("received_" + filename, "wb") as f:
        while bytes_received < filesize:
            bytes_read = comm.recv(source=SENDER_RANK, tag=2)       
            if not bytes_read:
                break
            
            f.write(bytes_read)
            bytes_received += len(bytes_read)
            print(f"\rReceiver (Rank {rank}): Received {bytes_received}/{filesize} bytes", end="")

    # Verify final size
    if bytes_received == filesize:
        print(f"\n[+] Receiver (Rank {rank}): Sending '{filename}' completed successfully.")
    else:
        print(f"\n[!] Receiver (Rank {rank}): Error: Received {bytes_received} bytes, expected {filesize} bytes.")

if rank == SENDER_RANK:
    mpi_sender()
elif rank == RECEIVER_RANK:
    mpi_receiver()
else:
    print(f"Rank {rank} is idle.")
