import asyncio

async def tcp_echo_client():
    reader, writer = await asyncio.open_connection('127.0.0.1', 6379)

    message = '*2\r\n$4\r\nECHO\r\n$3\r\nhey\r\n'
    writer.write(message.encode())

    data = await reader.read(100)
    print(f'Received: {data.decode()}')

    writer.close()

asyncio.run(tcp_echo_client())
