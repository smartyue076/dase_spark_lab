sudo docker run -itd \
  --name spark_test \
  --network host \
  -v /home/xuyue:/home/xuyue \
  apache/spark:3.5.7-python3 \
  tail -f /dev/null
