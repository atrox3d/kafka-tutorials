# WARNING 
.venv is for devcontainer **NOT intended for local use**

it is created from devcontainer.json and won't work locally

```json
"postCreateCommand": "pipx install uv && uv venv --allow-existing && uv sync && . .venv/bin/activate"
```

<br>

---

# devcontainer structure:

- .devoncontainer/devcontainer.json

    defines the devcontainer, specifiying the docker-compose file

- .env

    defines environment variables used inside docker-compose.yaml

- docker-compose.yaml

    defines the services:
    
    - devcontainer
    
        creates the vscode environment inside the container and provides python 3.11 image
    
    - zookeper
    
    - kafka


# Tutorials

1. [Using Kafka to Produce and Consume Messages Between Applications - FastAPI (Python)
](https://youtu.be/K4jjF8uWUmg?si=dUIJVpb2RvECYC7E)

        src/fastapi_producer_consumer

2. [Scalable & Event Driven Food Ordering App with Kafka & Python | System Design
](https://youtu.be/qi7uR3ItaOY?si=lKYEQD63Gq3H-wAG)

        src/event_driven_food_ordering_app
