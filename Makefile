.PHONY: up down build demo logs ps scale clean

# Bring the platform online (redis, orchestrator, telemetry, bot_fleet)
up:
	docker compose up -d --build
	@echo ""
	@echo "HFT Arena is up.  Dashboard: http://localhost:8000"

build:
	docker compose build

down:
	docker compose down --remove-orphans

# Run the end-to-end demo: submit the reference engine, launch a load run.
demo:
	./scripts/demo.sh

# Prove the load generator is distributed by scaling bot workers.
scale:
	docker compose up -d --scale bot_fleet=3

ps:
	docker compose ps

logs:
	docker compose logs -f --tail=80

# Tear everything down and remove any submission sandbox containers/images.
clean: down
	-docker ps -aq --filter "name=arena-sub-" | xargs -r docker rm -f
	-docker images -q "arena-sub-*" | xargs -r docker rmi -f
