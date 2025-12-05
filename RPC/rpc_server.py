import grpc
from concurrent import futures
import os
import file_transfer_pb2 as pb2
import file_transfer_pb2_grpc as pb2_grpc

class FileTransferServicer(pb2_grpc.FileTransferServiceServicer):
    def UploadFile(self, request_iterator, context):
        # receive metadata
        first_chunk = next(request_iterator)
        if first_chunk.HasField("metadata"):
            filename = first_chunk.metadata.filename
            save_filename = "received_" + os.path.basename(filename)
            print(f"[*] Starting to receive file: {filename}")
        else:
            return pb2.UploadStatus(success=False, message="Missing file metadata")

        # receive data
        try:
            bytes_received = 0
            with open(save_filename, 'wb') as f:
                for chunk in request_iterator:
                    if chunk.HasField("chunk_data"):
                        f.write(chunk.chunk_data)
                        bytes_received += len(chunk.chunk_data)
           
            print(f"[+] File '{filename}' received successfully. Total size: {bytes_received} bytes")
            return pb2.UploadStatus(success=True, message=f"File received: {save_filename}")

        except Exception as e:
            print(f"[-] Error receiving file: {e}")
            return pb2.UploadStatus(success=False, message=f"Server error: {e}")

def server():
    # create rpc server
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
    pb2_grpc.add_FileTransferServiceServicer_to_server(
        FileTransferServicer(), server
    )
    server.add_insecure_port('[::]:65432')
    server.start()
    print("[*] RPC Server listening on port 65432")
    server.wait_for_termination()

if __name__ == '__main__':
    server()