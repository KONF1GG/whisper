FROM pytorch_base

WORKDIR /app  

COPY requirements.txt .

RUN pip install --no-cache-dir -r requirements.txt

COPY app/shopot.py .

CMD ["python", "shopot.py"]