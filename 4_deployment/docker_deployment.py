from prefect.deployments import Deployment
from prefect.infrastructure.docker import DockerContainer
from etl_web_to_gcs import main

docker_block = DockerContainer.load("eia-etl-container")

docker_dep = Deployment.build_from_flow(
    flow=main,
    name="docker-eia-etl-flow",
    infrastructure=docker_block
)

if __name__=='__main__':
    docker_dep.apply()
