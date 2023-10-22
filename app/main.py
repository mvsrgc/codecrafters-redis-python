import asyncio
from asyncio import StreamReader, StreamWriter
from enum import Enum
from typing import Any

END_OF_FIELD = b"\r\n"

KV = {}


class DataTypes(Enum):
    SimpleString = 1
    BulkString = 2
    Array = 3


async def read_type(client_reader: StreamReader) -> DataTypes:
    dtype = await client_reader.read(1)
    match dtype:
        case b"*":
            return DataTypes.Array
        case b"+":
            return DataTypes.SimpleString
        case b"$":
            return DataTypes.BulkString
        case _:
            raise Exception(f"DataType is not defined for [{dtype}]")


async def read_data(client_reader: StreamReader) -> Any:
    dtype = await read_type(client_reader)

    match dtype:
        case DataTypes.Array:
            return await read_array(client_reader)

        case DataTypes.SimpleString:
            return await read_simple_string(client_reader)

        case DataTypes.BulkString:
            return await read_bulk_string(client_reader)

        case _:
            raise Exception(f"Read method is not defined for [{dtype}]")


async def read_next_field(client_reader: StreamReader) -> bytes:
    content = b""

    while (not content.endswith(END_OF_FIELD)) and (
        data := await client_reader.read(1)
    ):
        content += data

    return content


async def read_simple_string(client_reader: StreamReader) -> str:
    bytes_read = await read_next_field(client_reader)

    return bytes_read.decode().strip()


async def read_bulk_string(client_reader: StreamReader) -> str:
    string_length = await read_next_field(client_reader)

    string_length = int(string_length.decode().strip())

    bytes_read = b""

    for i in range(string_length):
        bytes_read += await client_reader.read(1)

    end_of_field = await client_reader.read(2)

    assert (
        end_of_field == END_OF_FIELD
    ), f"BulkString length mismatch end of field mark {bytes_read}"

    return bytes_read.decode("ascii").lstrip().rstrip()


async def read_array(client_reader: StreamReader) -> list:
    array_length = await read_next_field(client_reader)

    array_length = int(array_length.decode().strip())

    return [await read_data(client_reader) for _ in range(array_length)]


async def handle_command(client_reader: StreamReader, client_writer: StreamWriter):
    while True:
        command = await read_data(client_reader)

        arguments = []

        if isinstance(command, list):
            command, *arguments = command

        match command.upper():
            case "PING":
                await handle_ping(client_writer)

            case "ECHO":
                await handle_echo(client_writer, arguments)

            case "SET":
                await handle_set(client_writer, arguments)


async def handle_ping(client_writer: StreamWriter) -> None:
    client_writer.write("+PONG\r\n".encode())

    await client_writer.drain()


async def handle_echo(client_writer: StreamWriter, arguments: list) -> None:
    bytes_to_send = make_bulk_string(arguments)

    client_writer.write(bytes_to_send)

    await client_writer.drain()

async def handle_set(client_writer: StreamWriter, arguments: list) -> None:
    client_writer.write("+You want SET?\r\n".encode())

    await client_writer.drain()


def make_bulk_string(arguments) -> bytes:
    result = " ".join(arguments)

    result = f"${len(result)}\r\n{result}\r\n"

    return result.encode("ascii")


async def main():
    server = await asyncio.start_server(
        client_connected_cb=handle_command, host="localhost", port=6379, reuse_port=True
    )

    async with server:
        await server.serve_forever()


if __name__ == "__main__":
    asyncio.run(main())
