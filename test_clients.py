import socket
import sys
import threading

host = "127.0.0.1"
port = int(sys.argv[1]) if len(sys.argv) > 1 else 9000
barrier = threading.Barrier(3)
results = [None] * 3


def worker(index):
    with socket.create_connection((host, port), timeout=5) as connection:
        connection.settimeout(5)
        barrier.wait(timeout=5)
        payload = (f"Клиент {index}: привет, сервер!\n" * 1000).encode("utf-8")
        connection.sendall(payload)
        received = bytearray()
        while len(received) < len(payload):
            block = connection.recv(4096)
            if not block:
                break
            received.extend(block)
        results[index - 1] = (len(received), received == payload)


threads = [threading.Thread(target=worker, args=(index,)) for index in range(1, 4)]
for thread in threads:
    thread.start()
for thread in threads:
    thread.join()
for index, result in enumerate(results, 1):
    size, matches = result
    print(f"Клиент {index}: получено {size} байт, эхо {'совпало' if matches else 'НЕ совпало'}")
if not all(result[1] for result in results):
    sys.exit(1)
