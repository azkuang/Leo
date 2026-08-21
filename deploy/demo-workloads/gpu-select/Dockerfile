FROM pytorch/pytorch:2.3.0-cuda12.1-cudnn8-runtime
COPY allocate.py /allocate.c
ENTRYPOINT ["python", "/allocate.py"]
