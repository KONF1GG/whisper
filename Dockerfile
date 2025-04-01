FROM pytorch_base

ADD . /app
WORKDIR /app

RUN pip install -r requirements.txt --no-cache-dir

CMD ["python", "shopot.py"]

