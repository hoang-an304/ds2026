import grpc
import os
import file_transfer_pb2 as pb2
import file_transfer_pb2_grpc as pb2_grpc

SERVER_HOST = 'localhost'
SERVER_PORT = 65432
BUFFER_SIZE = 4096
FILE_TO_SEND = "test_file.txt" 

def file_chunk_generator(filename):
    # send metadata
    yield pb2.FileChunk(metadata=pb2.FileMetadata(filename=os.path.basename(filename)))

    # send data
    bytes_sent = 0
    filesize = os.path.getsize(filename) 
    with open(filename, 'rb') as f:
        while True:
            chunk_data = f.read(BUFFER_SIZE)
            if not chunk_data:
                break
            bytes_sent += len(chunk_data)
            yield pb2.FileChunk(chunk_data=chunk_data)
            print(f"\rSent {bytes_sent}/{filesize} bytes", end="")
            
def run_client():
    channel = grpc.insecure_channel(f'{SERVER_HOST}:{SERVER_PORT}')
    # create a client stub
    stub = pb2_grpc.FileTransferServiceStub(channel) 
    print(f"[*] Preparing to send file: {FILE_TO_SEND}")

    # call rpc
    try:
        status = stub.UploadFile(file_chunk_generator(FILE_TO_SEND))
        print(f"\n[+] Transfer result: Success={status.success}, Message: {status.message}")        
    except grpc.RpcError as e:
        print(f"\n[-] RPC Error: {e.details()}")

if __name__ == '__main__':
    if not os.path.exists(FILE_TO_SEND):
        with open(FILE_TO_SEND, "w") as f:
            f.write("Đây là nội dung file test cho RPC.\n")
            f.write("Nó cần phải được gửi qua gRPC service.")
    
    run_client()