## How to Run

###

Make sure Docker Desktop is running. Open a terminal in the project root directory where `docker-compose.yml` is located and run:

```bash
docker compose up -d
docker exec bank-kafka sh /scripts/create-topics.sh
npm install
npm start
your API is available at http://localhost:3000
