# Open Market Data

Projeto para coletar, estruturar e analisar dados de mercado (imobiliário) usando raspagem com Playwright, tratamento com Polars e armazenamento em parquet. O foco atual está no **Zap Imóveis**, com pipeline de coleta e uma análise inicial dos anúncios.

## ✨ Visão geral

O fluxo principal faz:

1. **Coleta** de anúncios no Zap Imóveis por bairro/cidade/estado.
2. **Normalização** dos dados em `parquet`.
3. **Análise** básica com limpeza de campos (preço, condomínio, IPTU, área, etc.).

> Observação: há suporte para integração com MinIO/S3 para leitura de catálogos geográficos (estados, cidades, distritos).

## 🧰 Tecnologias

- **Python 3.12**
- **Playwright** (scraping)
- **Polars** (tratamento e análise)
- **MinIO / boto3** (acesso a dados geográficos em parquet)
- **python-dotenv** (variáveis de ambiente)

## 📁 Estrutura do projeto

```
.
├── main.py                     # Entry-point do pipeline
├── src
│   ├── analysis
│   │   └── zap_imoveis.py       # Análises iniciais
│   ├── collector
│   │   └── zap_imoveis
│   │       ├── pipeline.py      # Orquestração do scraping
│   │       ├── scraper.py       # Raspagem Playwright
│   │       └── parser.py        # Parser dos cards
│   ├── core
│   │   ├── browser.py           # Gerenciamento do Playwright
│   │   └── geo_catalog.py       # Catálogo de estados/cidades/bairros
│   └── storage
│       ├── __init__.py          # Loader parquet S3/MinIO
│       ├── boto3.py             # Cliente S3
│       └── minio.py             # Cliente MinIO
└── data/                        # Saída local dos parquets (gerado)
```

## ✅ Pré-requisitos

- Python **3.12**
- Playwright instalado com browsers
- Dependências do projeto

## ⚙️ Instalação

```bash
pip install -r requirements.txt
```

Se você usa `uv`:

```bash
uv sync
```

Instale os browsers do Playwright:

```bash
playwright install
```

## 🔐 Variáveis de ambiente

Crie um arquivo `.env` com as variáveis necessárias para ler os dados de catálogo (S3/MinIO):

```dotenv
MINIO_ENDPOINT=localhost:9000
MINIO_ACCESS_KEY=seu_access_key
MINIO_SECRET_KEY=seu_secret_key
MINIO_REGION=us-east-1
BUCKET=nome_do_bucket
```

> Os arquivos esperados no bucket são:
> - `/br/states.parquet`
> - `/br/cities.parquet`
> - `/br/districs.parquet`

## 🚀 Como usar

### 1) Rodar o pipeline de coleta

```bash
python main.py
```

O pipeline utiliza o `ZapPipe`, que:

- Gera combinações de bairros x tipos de imóvel
- Filtra o que já foi coletado hoje
- Executa scraping por item

Os dados serão salvos em:

```
data/zap_imoveis/YYYY-MM-DD/<tipo_imovel>/<estado>_<cidade>_<bairro>.parquet
```

### 2) Rodar a análise

No `main.py`, descomente a função `make_analysis()` para analisar os dados:

```python
if __name__ == "__main__":
    asyncio.run(main())
    # make_analysis()
```

A análise converte campos como preço, condomínio e IPTU para formatos numéricos.

## 🧪 Testes

Atualmente não há testes automatizados incluídos.

## 🧭 Roadmap (ideias)

- ✅ Refino da análise dos dados
- ⬜ Normalização de preços e conversão de moeda
- ⬜ Inserção automática em banco ou data lake
- ⬜ Dashboard para visualização
- ⬜ Suporte a novos marketplaces (OLX, WebMotors, etc.)

## 📄 Licença

Este projeto está sob a licença definida em `LICENSE`.
