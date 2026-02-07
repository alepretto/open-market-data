import asyncio
from pathlib import Path

from src.collector.zap_imoveis.pipeline import ZapPipe


async def main():

    DATA_PATH = f"{Path(__file__).parent.resolve()}/data/"

    pipe = ZapPipe(data_path=DATA_PATH)

    await pipe.run()


if __name__ == "__main__":
    asyncio.run(main())
