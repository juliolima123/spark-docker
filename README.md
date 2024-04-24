# spark-docker

execução:

- docker-compose up --scale spark-worker=3
- make run-scaled -d
- make submit app=data_analysis_book/chapter03/word_non_null.py

A execução por make submit equivale a: docker exec da-spark-master spark-submit --master spark://spark-master:7077 --deploy-mode client ./apps/data_analysis_book/chapter03/word_non_null.py
