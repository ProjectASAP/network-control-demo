"""
Docker-based VictoriaMetrics service management for vertical scalability testing.
"""

import os

from .base import DockerServiceBase
from experiment_utils.providers.base import InfrastructureProvider


class DockerVictoriaMetricsService(DockerServiceBase):
    """Docker-based VictoriaMetrics single-node service with resource constraints."""

    def __init__(self, provider: InfrastructureProvider, num_nodes: int):
        """
        Initialize Docker VictoriaMetrics service.

        Args:
            provider: Infrastructure provider for node communication and management
            num_nodes: Number of nodes to manage
        """
        super().__init__(provider, num_nodes)
        self.container_name = "victoriametrics-scalability-test"

    def get_container_name(self) -> str:
        """Get the Docker container name."""
        return self.container_name

    def get_service_url(self) -> str:
        """Get VictoriaMetrics URL for queries."""
        return "http://localhost:8428"

    def get_health_endpoint(self) -> str:
        """Get VictoriaMetrics health check endpoint."""
        return "/health"

    def start(
        self, cpu_limit: float, memory_limit: str, experiment_output_dir: str, **kwargs
    ) -> None:
        """
        Start VictoriaMetrics in Docker container with resource limits.

        Args:
            cpu_limit: Number of CPUs to allocate (e.g., 4.0)
            memory_limit: Memory limit (e.g., "8g")
            experiment_output_dir: Directory for data storage
            **kwargs: Additional configuration
        """
        # Stop and remove any existing container first
        self._force_cleanup_container()

        # Prepare data directory
        vm_data_dir = os.path.join(experiment_output_dir, "victoriametrics_data")

        # Create data directory on remote host
        self.provider.execute_command(
            node_idx=0,
            cmd=f"mkdir -p {vm_data_dir}",
            cmd_dir=None,
            nohup=False,
            popen=False,
        )

        # Docker command with resource limits
        # VictoriaMetrics uses port 8428 by default, compatible with Prometheus remote write
        docker_cmd = (
            f"docker run -d --name {self.container_name} "
            f"--cpus={cpu_limit} --memory={memory_limit} "
            f"-p 8428:8428 "
            f"-v {vm_data_dir}:/victoria-metrics-data "
            f"victoriametrics/victoria-metrics:latest "
            f"-storageDataPath=/victoria-metrics-data "
            f"-httpListenAddr=:8428 "
            f"-retentionPeriod=1d"
        )

        # Run Docker container
        self.provider.execute_command(
            node_idx=0,
            cmd=docker_cmd,
            cmd_dir=None,
            nohup=False,
            popen=False,
        )

        # Wait for VictoriaMetrics to be ready
        self._wait_for_service_ready()

    def stop(self, **kwargs) -> None:
        """
        Stop and remove VictoriaMetrics Docker container.

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

    def get_victoriametrics_url(self) -> str:
        """
        Get VictoriaMetrics URL for queries.

        Returns:
            VictoriaMetrics base URL
        """
        return self.get_service_url()
