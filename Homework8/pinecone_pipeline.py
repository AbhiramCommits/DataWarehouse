import os
from datetime import datetime
from airflow import DAG
from airflow.models import Variable
from airflow.operators.python import PythonOperator
from sentence_transformers import SentenceTransformer
from pinecone import Pinecone, ServerlessSpec



def preprocess_file(**context):
    input_path = "/opt/airflow/dags/shakepoems.txt"
    output_path = "/opt/airflow/dags/shakepoems_chunks.txt"

    if not os.path.exists(input_path):
        raise FileNotFoundError(f"shakepoems.txt not found at: {input_path}")

    with open(input_path, "r") as f:
        text = f.read()

    chunk_size = 200
    chunks = [text[i:i+chunk_size] for i in range(0, len(text), chunk_size)]

    with open(output_path, "w") as f:
        for c in chunks:
            cleaned = c.replace("\n", " ").strip()
            f.write(cleaned + "\n")

    print(f"Preprocessed file into {len(chunks)} chunks → {output_path}")


def create_index(**context):
    api_key = Variable.get("PINECONE_API_KEY")

    # Use new Pinecone SDK
    pc = Pinecone(api_key=api_key)

    index_name = "shakepoems-index"
    dimension = 384   # MiniLM-L6-v2 model dims

    # Delete if already exists
    existing = pc.list_indexes().names()
    if index_name in existing:
        pc.delete_index(index_name)

    # Create new serverless index
    pc.create_index(
        name=index_name,
        dimension=dimension,
        metric="cosine",
        spec=ServerlessSpec(
            cloud="aws",
            region="us-east-1"
        )
    )

    print(f"Created Pinecone index: {index_name}")


def embed_and_upsert(**context):
    from pinecone import Pinecone
    from sentence_transformers import SentenceTransformer
    import math

    api_key = Variable.get("PINECONE_API_KEY")
    pc = Pinecone(api_key=api_key)
    index = pc.Index("shakepoems-index")

    model = SentenceTransformer("sentence-transformers/all-MiniLM-L6-v2")

    chunks_path = "/opt/airflow/dags/shakepoems_chunks.txt"
    with open(chunks_path, "r") as f:
        lines = [line.strip() for line in f.readlines()]

    batch_size = 20  # SAFE FOR LOW RAM
    total = len(lines)
    num_batches = math.ceil(total / batch_size)

    print(f"Total chunks: {total} — Processing in {num_batches} batches")

    for batch_i in range(num_batches):
        start = batch_i * batch_size
        end = min(start + batch_size, total)

        batch_texts = lines[start:end]
        embeddings = model.encode(batch_texts, batch_size=batch_size).tolist()

        vectors = []
        for j, emb in enumerate(embeddings):
            actual_index = start + j
            vectors.append({
                "id": str(actual_index),
                "values": emb,
                "metadata": {"text": batch_texts[j]}
            })

        index.upsert(vectors=vectors)

        print(f"Batch {batch_i + 1}/{num_batches} — Upserted {len(vectors)} vectors")

    print("All batches processed successfully!")


def search_query(**context):
    api_key = Variable.get("PINECONE_API_KEY")
    pc = Pinecone(api_key=api_key)
    index = pc.Index("shakepoems-index")

    model = SentenceTransformer("sentence-transformers/all-MiniLM-L6-v2")

    query = "love and destiny"
    q_emb = model.encode(query).tolist()

    res = index.query(
        vector=q_emb,
        top_k=5,
        include_metadata=True
    )

    print("🔍 Query:", query)
    print("----- Top Matches -----")
    for match in res["matches"]:
        print(match)


default_args = {
    "owner": "airflow",
    "start_date": datetime(2024, 1, 1)
}

with DAG(
    dag_id="pinecone_homework8",
    default_args=default_args,
    schedule=None,   # Airflow 3.x correct syntax
    catchup=False
):

    t1 = PythonOperator(
        task_id="preprocess_file",
        python_callable=preprocess_file
    )

    t2 = PythonOperator(
        task_id="create_index",
        python_callable=create_index
    )

    t3 = PythonOperator(
        task_id="embed_and_upsert",
        python_callable=embed_and_upsert
    )

    t4 = PythonOperator(
        task_id="search_pinecone",
        python_callable=search_query
    )

    t1 >> t2 >> t3 >> t4