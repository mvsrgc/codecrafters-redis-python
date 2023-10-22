# Uncomment this to pass the first stage
import asyncio
from dataclasses import dataclass
from enum import Enum

class DataTypes(Enum):
    SimpleString = 1
    BulkString = 2
    Array = 3

def make_simple_string(text):
    result = f"+{text}\r\n"
    return result.encode()

def make_bulk_string(arguments):
    result = " ".join(arguments)
    result = f"${len(result)}\r\n{result}\r\n"
    return result.encode()

async def read_type(reader):
    print(f"------------------------------------")
    dtype = await reader.read(1)
    print(f"I am reading a type of {str(dtype)}")

    match dtype:
        case b"*":
            return DataTypes.Array
        case b"+":
            return DataTypes.SimpleString
        case b"$":
            print("Hey, I read a $")
            return DataTypes.BulkString
        case _:
            raise Exception(f"Data type not supported [{dtype}]")

async def read_data(reader):
    dtype = await read_type(reader)
    match dtype:
        case DataTypes.Array:
            return await read_array(reader)
        case DataTypes.SimpleString:
            return await read_simple_string(reader)
        case DataTypes.BulkString:
            return await read_bulk_string(reader)
        case _:
            raise Exception(f"Read not supported [{dtype}]")

async def read_array(reader):
    array_length = await read_next_field(reader)
    array_length = int(array_length.decode().strip())
    print(f"The array is of length {array_length}")

    results = []
    for _ in range(array_length):
        results.append(await read_data(reader))
        print(f"Results is {results}")

    return results

async def read_simple_string(reader):
    bytes_read = await read_next_field(reader)
    return bytes_read.decode().strip()

async def read_bulk_string(reader):
    print("Reading bulk string...")
    string_length = await read_next_field(reader)
    string_length = int(string_length.decode().strip())
    
    bytes_read = b""
    for _ in range(string_length):
        bytes_read += await reader.read(1)

    # This should be \r\n after a bulk string according to Redis protocol
    end_of_field = await reader.read(2)

    assert end_of_field == b'\r\n', f"BulkString length mismatch end of field mark {bytes_read}"

    return bytes_read.decode("ascii").strip()

async def read_next_field(reader):
    content = b""

    while (not content.endswith(b"\r\n")) and (data := await reader.read(1)):
        content += data

    print(f"Content of this field: {content}")
    return content

def handle_echo(writer, arguments):
    bulk_string = make_bulk_string(arguments)
    writer.write(str(bulk_string).encode())

async def handle_client(reader, writer):
    while True:
        print("Gonna read data")
        command = await read_data(reader)
        arguments = []

        if isinstance(command, list):
            command, *arguments = command

        match command.upper():
            case "ECHO":
                handle_echo(writer, arguments)

        await writer.drain()

    writer.close()
    await writer.wait_closed()


async def main():
    server = await asyncio.start_server(handle_client, 'localhost', 6379, reuse_port=True)
    async with server:
        await server.serve_forever()


if __name__ == "__main__":
    asyncio.run(main())
