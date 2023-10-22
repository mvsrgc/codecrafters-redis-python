import asyncio

async def tcp_echo_client():
    reader, writer = await asyncio.open_connection('127.0.0.1', 6379)

    message = '*3\r\n$3\r\nSET\r\n$4\r\nname\r\n$6\r\nmvsrgc\r\n'
    writer.write(message.encode())

    data = await reader.read(100)
    print(f'Received: {data.decode()}')

    message = '*2\r\n$3\r\nGET\r\n$4\r\nname\r\n'
    writer.write(message.encode())

    data = await reader.read(100)
    print(f'Received: {data.decode()}')

    writer.close()

asyncio.run(tcp_echo_client())
