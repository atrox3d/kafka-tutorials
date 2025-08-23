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

