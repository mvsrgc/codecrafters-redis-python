# Uncomment this to pass the first stage
import asyncio
from dataclasses import dataclass
from enum import Enum

class DataTypes(Enum):
    SimpleString = 1
    BulkString = 2
    Array = 3

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

async def read_type(reader):
    dtype = await reader.read(1)
    match dtype:
        case b"*":
            return DataTypes.Array
        case b"+":
            return DataTypes.SimpleString
        case b"$":
            return DataTypes.BulkString
        case _:
            raise Exception(f"Data type not supported [{dtype}]")

async def read_data(reader):
    dtype = await read_type(reader)
    match dtype:
        case DataTypes.Array:
            return read_array(reader)
        case DataTypes.SimpleString:
            return read_simple_string(reader)
        case DataTypes.BulkString:
            return read_bulk_string(reader)
        case _:
            raise Exception(f"Read not supported [{dtype}]")

async def read_next_field(reader):
    content = b""
    while (not content.endswith(b"\r\n")) and (data := await reader.read(1)):
        content += data

    return content

async def handle_client(reader, writer):
    while True:
        data = await reader.read(1024)
        print(repr(data))
        if not data:
            break

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
