FROM pytorch/pytorch:2.1.0-cuda11.8-cudnn8-runtime

ENV DEBIAN_FRONTEND=noninteractive
ENV LD_LIBRARY_PATH=/opt/conda/pkgs/pytorch-2.1.0-py3.10_cuda11.8_cudnn8.7.0_0/lib/python3.10/site-packages/torch/lib:$LD_LIBRARY_PATH

# Установка часового пояса
RUN ln -fs /usr/share/zoneinfo/UTC /etc/localtime && \
    apt-get update && apt-get install -y tzdata && \
    dpkg-reconfigure --frontend noninteractive tzdata

RUN apt-get update && apt-get install -y build-essential && rm -rf /var/lib/apt/lists/*

RUN apt-get update && apt-get install -y ffmpeg && rm -rf /var/lib/apt/lists/*

WORKDIR /app

COPY requirements.txt /app/
RUN pip install -r requirements.txt --no-cache-dir

COPY . /app/

CMD ["python", "shopot.py"]