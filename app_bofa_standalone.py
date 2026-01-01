import asyncio
import json
import uuid
import logging
from datetime import datetime
from typing import Optional, List

from fastapi import FastAPI, HTTPException
from pydantic import BaseModel, Field
from sqlalchemy import Column, String, Float, DateTime, select
from sqlalchemy.ext.asyncio import create_async_engine, AsyncSession, async_sessionmaker
from sqlalchemy.orm import declarative_base
from aiokafka import AIOKafkaProducer, AIOKafkaConsumer

# --- CONFIGURAÇÃO DE LOGGING ---
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger("BofA-Transaction-Engine")

# --- CONFIGURAÇÕES DE INFRAESTRUTURA (DOCKER) ---
KAFKA_BOOTSTRAP_SERVERS = "localhost:9092"
KAFKA_TOPIC = "banking.transactions.v1"
DATABASE_URL = "postgresql+asyncpg://claudio:bofa_password@localhost:5432/transaction_db"

# --- DATABASE SETUP (SQLAlchemy 2.0 Async) ---
Base = declarative_base()

class TransactionModel(Base):
    """Modelo da tabela para persistência ACID das transações no PostgreSQL."""
    __tablename__ = "transactions"
    
    id = Column(String, primary_key=True)
    account_id = Column(String, index=True, nullable=False)
    amount = Column(Float, nullable=False)
    currency = Column(String, default="USD")
    merchant_name = Column(String)
    created_at = Column(DateTime, default=datetime.utcnow)

engine = create_async_engine(DATABASE_URL, echo=False)
async_session = async_sessionmaker(engine, expire_on_commit=False)

# --- VALIDAÇÃO DE DADOS (Pydantic) ---
class TransactionSchema(BaseModel):
    account_id: str
    amount: float = Field(..., gt=0, description="O valor da transação deve ser positivo")
    currency: str = "USD"
    merchant_name: str

class TransactionResponse(TransactionSchema):
    id: str
    created_at: datetime

    class Config:
        from_attributes = True

# --- INICIALIZAÇÃO DA APP FASTAPI ---
app = FastAPI(
    title="BofA Transaction Ingress Service",
    description="Motor de ingestão assíncrona de alta performance."
)

producer: Optional[AIOKafkaProducer] = None

@app.on_event("startup")
async def startup_event():
    global producer
    
    try:
        async with engine.begin() as conn:
            await conn.run_sync(Base.metadata.create_all)
        logger.info("✅ PostgreSQL: Tabelas prontas.")
    except Exception as e:
        logger.error(f"❌ Erro ao preparar banco de dados: {e}")
    
    producer = AIOKafkaProducer(
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
        value_serializer=lambda v: json.dumps(v, default=str).encode('utf-8')
    )
    
    try:
        await producer.start()
        logger.info("✅ Kafka: Produtor conectado com sucesso.")
        asyncio.create_task(run_kafka_consumer())
    except Exception as e:
        logger.error(f"❌ Falha crítica ao conectar ao Kafka: {e}")

@app.on_event("shutdown")
async def shutdown_event():
    if producer:
        await producer.stop()
    await engine.dispose()
    logger.info("🛑 Conexões encerradas.")

# --- WORKER CONSUMIDOR ---
async def run_kafka_consumer():
    consumer = AIOKafkaConsumer(
        KAFKA_TOPIC,
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
        group_id="bofa_processor_group",
        value_deserializer=lambda m: json.loads(m.decode('utf-8'))
    )
    await consumer.start()
    logger.info("📡 Worker: Aguardando mensagens do Kafka...")
    
    try:
        async for msg in consumer:
            data = msg.value
            logger.info(f"📥 [KAFKA] Mensagem recebida: {data['id']}")
            
            async with async_session() as session:
                async with session.begin():
                    new_tx = TransactionModel(
                        id=data["id"],
                        account_id=data["account_id"],
                        amount=data["amount"],
                        currency=data["currency"],
                        merchant_name=data["merchant_name"]
                    )
                    session.add(new_tx)
                await session.commit()
            
            logger.info(f"💾 [DATABASE] Transação {data['id']} persistida no Postgres.")
    except Exception as e:
        logger.error(f"❌ Erro no Worker do Consumidor: {e}")
    finally:
        await consumer.stop()

# --- ENDPOINTS DA API ---

@app.post("/v1/transactions", status_code=202)
async def ingest_transaction(tx: TransactionSchema):
    """Ingere a transação e envia para o Kafka."""
    if not producer:
        raise HTTPException(status_code=503, detail="Kafka não disponível")
    
    tx_id = str(uuid.uuid4())
    payload = tx.dict()
    payload["id"] = tx_id
    
    await producer.send_and_wait(KAFKA_TOPIC, payload)
    logger.info(f"📤 [PRODUCER] Transação {tx_id} enviada para o Kafka.")
    
    return {
        "transaction_id": tx_id,
        "status": "Accepted",
        "message": "Transação em processamento seguro."
    }

@app.get("/v1/transactions", response_model=List[TransactionResponse])
async def list_transactions():
    """Lista todas as transações persistidas no PostgreSQL."""
    async with async_session() as session:
        result = await session.execute(select(TransactionModel).order_by(TransactionModel.created_at.desc()))
        transactions = result.scalars().all()
        return transactions

@app.get("/health")
async def health_check():
    return {
        "status": "healthy",
        "timestamp": datetime.utcnow().isoformat(),
        "services": {
            "api": "online",
            "kafka": "connected" if producer else "offline"
        }
    }