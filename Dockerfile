FROM pytorch/pytorch:2.1.0-cuda11.8-cudnn8-runtime

ENV DEBIAN_FRONTEND=noninteractive

RUN ln -fs /usr/share/zoneinfo/UTC /etc/localtime && \
    apt-get update && apt-get install -y tzdata && \
    dpkg-reconfigure --frontend noninteractive tzdata

RUN apt-get update && apt-get install -y build-essential && rm -rf /var/lib/apt/lists/*

RUN apt-get update && \
    apt-get install -y ffmpeg && \
    rm -rf /var/lib/apt/lists/*

ADD . /app
WORKDIR /app

RUN pip install -r requirements.txt --no-cache-dir

CMD ["python", "shopot.py"]

