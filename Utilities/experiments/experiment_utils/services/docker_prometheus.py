"""
Docker-based Prometheus service management for vertical scalability testing.
"""

import os

from .base import DockerServiceBase
from experiment_utils.providers.base import InfrastructureProvider


class DockerPrometheusService(DockerServiceBase):
    """Docker-based Prometheus service with resource constraints."""

    def __init__(self, provider: InfrastructureProvider, num_nodes: int):
        """
        Initialize Docker Prometheus service.

        Args:
            provider: Infrastructure provider for node communication and management
            num_nodes: Number of nodes to manage
        """
        super().__init__(provider, num_nodes)
        self.container_name = "prometheus-scalability-test"

    def get_container_name(self) -> str:
        """Get the Docker container name."""
        return self.container_name

    def get_service_url(self) -> str:
        """Get Prometheus URL for queries."""
        return "http://localhost:9090"

    def get_health_endpoint(self) -> str:
        """Get Prometheus health check endpoint."""
        return "/-/ready"

    def start(
        self, cpu_limit: float, memory_limit: str, experiment_output_dir: str, **kwargs
    ) -> None:
        """
        Start Prometheus in Docker container with resource limits.

        Args:
            cpu_limit: Number of CPUs to allocate (e.g., 4.0)
            memory_limit: Memory limit (e.g., "8g")
            experiment_output_dir: Directory containing prometheus config
            **kwargs: Additional configuration
        """
        # Stop and remove any existing container first
        self._force_cleanup_container()

        # Prepare volume mounts
        prometheus_config_dir = os.path.join(experiment_output_dir, "prometheus_config")
        prometheus_data_dir = os.path.join(experiment_output_dir, "prometheus_data")

        # Create data directory on remote host with proper permissions
        self.provider.execute_command(
            node_idx=0,
            cmd=f"mkdir -p {prometheus_data_dir} && chmod 777 {prometheus_data_dir}",
            cmd_dir=None,
            nohup=False,
            popen=False,
        )

        # Docker command with resource limits
        docker_cmd = (
            f"docker run -d --name {self.container_name} "
            f"--cpus={cpu_limit} --memory={memory_limit} "
            f"-p 9090:9090 "
            f"-v {prometheus_config_dir}/prometheus.yml:/etc/prometheus/prometheus.yml:ro "
            f"-v {prometheus_data_dir}:/prometheus "
            f"prom/prometheus:latest "
            f"--config.file=/etc/prometheus/prometheus.yml "
            f"--storage.tsdb.path=/prometheus "
            f"--web.console.libraries=/etc/prometheus/console_libraries "
            f"--web.console.templates=/etc/prometheus/consoles "
            f"--web.enable-lifecycle"
        )

        # Run Docker container
        self.provider.execute_command(
            node_idx=0,
            cmd=docker_cmd,
            cmd_dir=None,
            nohup=False,
            popen=False,
        )

        # Wait for Prometheus to be ready
        self._wait_for_service_ready()

    def stop(self, **kwargs) -> None:
        """
        Stop and remove Prometheus Docker container.

        Args:
            **kwargs: Additional configuration (currently unused)
        """
        # Stop and remove container
        cmd = f"docker stop {self.container_name}; docker rm {self.container_name}"
        self.provider.execute_command(
            node_idx=0,
            cmd=cmd,
            cmd_dir=None,
            nohup=False,
            popen=False,
            ignore_errors=True,
        )

    def get_prometheus_url(self) -> str:
        """
        Get Prometheus URL for queries.

        Returns:
            Prometheus base URL
        """
        return self.get_service_url()
