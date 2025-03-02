up:
	docker compose up -d

client:
	docker compose exec -it clickhouse clickhouse-client