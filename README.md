# Kafka Avro to Parquet Pipeline

A Python application that produces Avro messages to Kafka and consumes them to save as Parquet files, using Confluent Schema Registry for schema management.

``Note
This is part of the kafka pipeline training
``
## Project Structure

```
kafka-avro-parquet/
├── src/
│   ├── __init__.py
|   ├── producer.py
│   └── consumer.py
│   
├── input/
├── output/
├── config/ 
├── pyproject.toml   
|
└── README.md
```

## Setup Instructions

### 1. Create Confluent Account

1. Go to [Confluent Cloud](https://confluent.cloud/)
2. Sign up for free account
3. Create a new cluster
4. Create a topic (e.g., `tmx-training`)

### 2. Get Confluent Credentials

**Kafka API Keys:**
- Go to Cluster → API Keys
- Create new API key/secret pair

**Schema Registry:**
- Go to Schema Registry → API Keys
- Create new API key/secret pair
- Note the Schema Registry URL

### 3. Install Dependencies

```bash
# Install Poetry if not already installed
curl -sSL https://install.python-poetry.org | python3 -

# Install project dependencies
poetry install
```

### 4. Configure Environment

Create `.env` file:
```env
CONFLUENT_BOOTSTRAPP=your-bootstrap-server
CONFLUENT_API_KEY=your-kafka-api-key
CONFLUENT_API_SECRET=your-kafka-api-secret
CONFLUENT_SCHEMA_REGISTRY_URL=your-schema-registry-url
CONFLUENT_SCHEMA_REGISTRY_KEY=your-schema-registry-key
CONFLUENT_SCHEMA_REGISTRY_SECRET=your-schema-registry-secret
```

### 5. Register Schema

1. Go to Confluent Cloud → Schema Registry
2. Create new subject: `tmx-training-value`
3. Set compatibility to `NONE` (for development)
4. Upload the schema from `src/schemas/user.avsc`

## Dependencies

Add to `pyproject.toml`:

```toml
requires-python = ">=3.13"
dependencies = [
    "confluent-kafka (>=2.11.0,<3.0.0)",
    "pandas (>=2.3.1,<3.0.0)",
    "pyarrow (>=20.0.0,<21.0.0)",
    "fastavro (>=1.11.1,<2.0.0)",
    "attrs (>=25.3.0,<26.0.0)",
    "httpx (>=0.28.1,<0.29.0)",
    "cachetools (>=6.1.0,<7.0.0)",
    "authlib (>=1.6.0,<2.0.0)"
]

```

Install with:
```bash
poetry add confluent-kafka[avro] pandas pyarrow python-dotenv
```

## Usage

### Run Producer
```bash
poetry run python src/producer.py
```

### Run Consumer
```bash
poetry run python src/consumer.py
```

## Schema Management

The Avro schema is automatically fetched from Confluent Schema Registry. The schema defines:
- `id` (int): User unique identifier
- `name` (string): User full name  
- `email` (string): User email address

## Output

Consumer saves messages as Parquet files in the `output/` directory for efficient analytics processing.

## Error Handling

- **Schema compatibility**: Set to `NONE` for development
- **Connection issues**: Check credentials and network access
- **Missing dependencies**: Use `poetry install` to ensure all packages are installed