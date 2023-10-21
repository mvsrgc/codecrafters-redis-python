# Uncomment this to pass the first stage
import asyncio
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

async def handle_client(reader, writer):
    while True:
        data = await reader.read(1024)
        if not data:
            break
        hello_message = SimpleString("PONG")
        writer.write(str(hello_message).encode())
        await writer.drain()

    writer.close()
    await writer.wait_closed()


async def main():
    # You can use print statements as follows for debugging, they'll be visible when running tests.
    print("Logs from your program will appear here!")

    server = await asyncio.start_server(handle_client, 'localhost', 6379, reuse_port=True)
    async with server:
        await server.serve_forever()


if __name__ == "__main__":
    asyncio.run(main())
