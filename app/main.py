# Uncomment this to pass the first stage
import socket
from dataclasses import dataclass

@dataclass
class BaseString:
    raw: str

    def strip_control_chars(self):
        return self.raw.replace("\r", "").replace("\n", "")

@dataclass
class SimpleString(BaseString):
    raw: str

    def __str__(self):
        stripped_raw = self.strip_control_chars()
        return f"+{stripped_raw}\r\n"

@dataclass
class BulkString(BaseString):
    raw: str

    def __str__(self):
        stripped_raw = self.strip_control_chars()
        return f"${len(stripped_raw)}\r\n{stripped_raw}\r\n"


def main():
    # You can use print statements as follows for debugging, they'll be visible when running tests.
    print("Logs from your program will appear here!")

    # Uncomment this to pass the first stage
    #
    server_socket = socket.create_server(("localhost", 6379), reuse_port=True)
    hello_message = SimpleString("PONG")
    client_socket, _ = server_socket.accept() # wait for client

    with client_socket:
        while True:
            data = client_socket.recv(1024)
            print(data)
            if not data:
                break
            client_socket.send(str(hello_message).encode())


if __name__ == "__main__":
    main()
