import asyncio, asyncssh, sys

async def run_client() -> None:
    async with asyncssh.connect('127.0.0.1', username='', password='') as conn:
        await asyncssh.scp((conn, '/Users/rahul/Figure_1.png'), '.')

try:
    asyncio.get_event_loop().run_until_complete(run_client())
except (OSError, asyncssh.Error) as exc:
    sys.exit('SFTP operation failed: ' + str(exc))