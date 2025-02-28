# Kafka-Kibana-Elastic
Проект реализует отпраку сообщений в Kafka. А так же логирование и при помощи Kibana+Elasticsearch.

Проект состоит из producer и consumer:
producer: генерирует сообщения для отправки в Kafka и отправляет лог в Elastic.
consumer: получает сообщения из Kafka и отправляет в Elastic.

Отправка струтурированных логов реализована за счёт отправки в Elasticsearch JSON.
