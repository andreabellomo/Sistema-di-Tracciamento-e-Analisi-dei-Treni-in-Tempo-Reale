FROM apache/spark:3.4.2

USER 0
RUN pip install numpy
COPY ./mod/ /home/spark/
WORKDIR /home/spark
USER spark