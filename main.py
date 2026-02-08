import asyncio
from pathlib import Path

from src.analysis.zap_imoveis import ZapImoveisAnalysis
from src.collector.zap_imoveis.pipeline import ZapPipe

DATA_PATH = f"{Path(__file__).parent.resolve()}/data"


async def main():

    pipe = ZapPipe(data_path=DATA_PATH)

    await pipe.run()


def make_analysis():

    analyst = ZapImoveisAnalysis(DATA_PATH)

    analyst.make()


if __name__ == "__main__":
    asyncio.run(main())
    # make_analysis()
