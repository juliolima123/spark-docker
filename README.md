# spark-docker

execução:

- docker-compose up --scale spark-worker=3
- make run-scaled -d
- make submit app=data_analysis_book/chapter03/word_non_null.py
