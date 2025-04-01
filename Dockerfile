FROM pytorch_base

RUN apt-get update && \
    apt-get install -y ffmpeg && \
    rm -rf /var/lib/apt/lists/*

ADD . /app
WORKDIR /app

RUN pip install -r requirements.txt --no-cache-dir

CMD ["python", "shopot.py"]

