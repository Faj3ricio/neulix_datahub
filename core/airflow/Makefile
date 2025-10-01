build:
	@docker compose build --no-cache 
		
down:
	@docker compose down

remove_images:
	@docker compose down --rmi all

remove_volumes:
	@docker compose down -v

up:
	@docker compose up --build -d
	
logs:
	@docker compose logs -f

prune:
	@docker system prune -a -f
	@ docker volume prune -f