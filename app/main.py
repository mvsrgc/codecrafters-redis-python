import asyncio
import argparse
import logging
from asyncio import StreamReader, StreamWriter
from dataclasses import dataclass
from datetime import datetime, timedelta
from enum import Enum
from typing import Dict, Any

END_OF_FIELD = b"\r\n"

KV = {}

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s]: %(message)s",
    handlers=[logging.StreamHandler()],
)


class ConfigManager:
    _instance = None
    _config: Dict[str, Any] = {}
    data_dir = ""
    db_file_name = ""

    def __new__(cls):
        if cls._instance is None:
            cls._instance = super(ConfigManager, cls).__new__(cls)
        return cls._instance

    @classmethod
    def initialize(cls, data_dir: str, db_file_name: str) -> None:
        cls.data_dir = data_dir
        cls.db_file_name = db_file_name
        config_path = f"{data_dir}/{db_file_name}"
        cls.load_config(config_path)

    @classmethod
    def load_config(cls, file_path: str) -> None:
        cls._config = {"name": "mvsrgc"}

    @classmethod
    def get_value(cls, key: str, default: Any = None) -> Any:
        return cls._config.get(key, default)


@dataclass
class Value:
    value: Any
    expire_time: Any


class RespTypes(Enum):
    Unknown = 0
    SimpleString = 1
    BulkString = 2
    Array = 3


async def read_type(client_reader: StreamReader) -> RespTypes:
    dtype = await client_reader.read(1)
    match dtype:
        case b"*":
            return RespTypes.Array
        case b"+":
            return RespTypes.SimpleString
        case b"$":
            return RespTypes.BulkString
        case _:
            return RespTypes.Unknown


async def read_client_input(client_reader: StreamReader) -> Any:
    dtype = await read_type(client_reader)

    match dtype:
        case RespTypes.Array:
            return await read_resp_array(client_reader)

        case RespTypes.SimpleString:
            return await read_resp_simple_string(client_reader)

        case RespTypes.BulkString:
            return await read_resp_bulk_string(client_reader)

        case _:
            raise Exception("Unexpected dtype")



async def read_next_resp_field(client_reader: StreamReader) -> bytes:
    content = b""

    while (not content.endswith(END_OF_FIELD)) and (
        data := await client_reader.read(1)
    ):
        content += data

    return content


async def read_resp_simple_string(client_reader: StreamReader) -> str:
    bytes_read = await read_next_resp_field(client_reader)

    return bytes_read.decode().strip()


async def read_resp_bulk_string(client_reader: StreamReader) -> str:
    string_length = await read_next_resp_field(client_reader)

    string_length = int(string_length.decode().strip())

    bytes_read = b""

    for _ in range(string_length):
        bytes_read += await client_reader.read(1)

    # Todo: Add error if end of the message is not \r\n
    # Right now, it waits forever.
    end_of_field = await client_reader.read(2)

    assert (
        end_of_field == END_OF_FIELD
    ), f"BulkString length mismatch end of field mark {bytes_read}"

    return bytes_read.decode().strip()


async def read_resp_array(client_reader: StreamReader) -> list:
    array_length = await read_next_resp_field(client_reader)

    array_length = int(array_length.decode().strip())

    return [await read_client_input(client_reader) for _ in range(array_length)]


async def handle_command(client_reader: StreamReader, client_writer: StreamWriter):
    logging.info(f"Client connected")
    while True:
        command = await read_client_input(client_reader)

        arguments = []

        if isinstance(command, list):
            command, *arguments = command

        # logging.info(f"Received command: {command}, Arguments: {arguments}")

        match command.upper():
            case "PING":
                await handle_ping(client_writer)

            case "ECHO":
                await handle_echo(client_writer, arguments)

            case "SET":
                await handle_set(client_writer, arguments)

            case "GET":
                await handle_get(client_writer, arguments)

            case "CONFIG":
                subcommand, key = arguments[:2]
                if subcommand.upper() == "GET":
                    await handle_config_get(client_writer, key)


async def handle_ping(client_writer: StreamWriter) -> None:
    client_writer.write("+PONG\r\n".encode())

    await client_writer.drain()


async def handle_echo(client_writer: StreamWriter, arguments: list) -> None:
    bytes_to_send = make_bulk_string(arguments)

    client_writer.write(bytes_to_send)

    await client_writer.drain()


async def handle_set(client_writer: StreamWriter, arguments: list) -> None:
    try:
        the_key, the_value = arguments[:2]
    except ValueError:
        return

    expire_time = None
    if len(arguments) > 3 and arguments[2] == "px":
        try:
            expire_time = datetime.now() + timedelta(milliseconds=int(arguments[3]))
        except ValueError:
            return

    value = Value(the_value, expire_time)
    KV[the_key] = value

    client_writer.write("+OK\r\n".encode())

    await client_writer.drain()


async def handle_get(client_writer: StreamWriter, arguments: list) -> None:
    if not arguments:
        return

    stored_item = KV.get(arguments[0])
    response = make_bulk_string(["nil"])

    if stored_item:
        if stored_item.expire_time is None or stored_item.expire_time > datetime.now():
            response = make_bulk_string([stored_item.value])
        else:
            response = make_null_string()

    client_writer.write(response)
    await client_writer.drain()


async def handle_config_get(client_writer: StreamWriter, key: str) -> None:
    if key == "dir":
        value = ConfigManager.data_dir
    elif key == "dbfilename":
        value = ConfigManager.db_file_name
    else:
        value = "nil"

    response = make_array([key, value])

    client_writer.write(response)
    await client_writer.drain()


def make_array(arguments) -> bytes:
    result = [make_bulk_string(arg) for arg in arguments]
    result_bytes = b"".join(result)

    response = f"*{len(arguments)}\r\n".encode() + result_bytes

    return response


def make_bulk_string(arguments) -> bytes:
    if isinstance(arguments, (list, tuple)):
        result = " ".join(arguments)
    else:
        result = arguments

    response = f"${len(result)}\r\n{result}\r\n"

    return response.encode()


def make_null_string():
    return b"$-1\r\n"


async def main():
    parser = argparse.ArgumentParser(description="Make your own Redis")
    parser.add_argument("--dir", type=str, help="Directory for Redis files")
    parser.add_argument("--dbfilename", type=str, help="Database filename")

    args = parser.parse_args()

    data_dir = args.dir
    db_file_name = args.dbfilename

    if data_dir and db_file_name:
        ConfigManager.initialize(data_dir, db_file_name)

    server = await asyncio.start_server(
        client_connected_cb=handle_command,
        host="localhost",
        port=6379,
        reuse_port=True,
    )

    async with server:
        await server.serve_forever()


if __name__ == "__main__":
    asyncio.run(main())
