"""
Service management package for experiments.

This package contains service classes for managing various components
of the experiment infrastructure with consistent start/stop interfaces.
"""

from .base import BaseService, DockerServiceBase
from .kafka import KafkaService
from .flink import FlinkService
from .query_engine import (
    QueryEngineService,
    QueryEngineRustService,
    QueryEngineServiceFactory,
)
from .monitoring import MonitoringService
from .fake_exporters import (
    ExporterServiceFactory,
    PythonExporterService,
    RustExporterService,
    AvalancheExporterService,
)
from .system_exporters import SystemExportersService
from .prometheus import PrometheusService
from .prometheus_kafka_adapter import PrometheusKafkaAdapterService
from .prometheus_client_service import PrometheusClientService
from .remote_monitor_service import RemoteMonitorService
from .docker_prometheus import DockerPrometheusService
from .docker_victoriametrics import DockerVictoriaMetricsService
from .arroyo import ArroyoService
from .arroyo_throughput_monitor import ArroyoThroughputMonitor
from .prometheus_throughput_monitor import PrometheusThroughputMonitor
from .misc import (
    DeathstarService,
    ControllerService,
    DumbKafkaConsumerService,
)
from .grafana import GrafanaService


def create_prometheus_service(cfg, provider, num_nodes: int):
    """
    Create appropriate Prometheus service based on configuration.

    Args:
        cfg: Hydra configuration object
        provider: Infrastructure provider for node communication and management
        num_nodes: Number of nodes

    Returns:
        Appropriate Prometheus service instance
    """
    # Check if docker_resources config exists for vertical scalability testing
    if hasattr(cfg.experiment_params, "docker_resources"):
        tool = cfg.experiment_params.docker_resources.get("tool", "prometheus")
        if tool == "prometheus":
            return DockerPrometheusService(provider, num_nodes)
        elif tool == "victoriametrics":
            return DockerVictoriaMetricsService(provider, num_nodes)

    # Default to existing PrometheusService for backward compatibility
    return PrometheusService(provider, num_nodes)


__all__ = [
    "BaseService",
    "DockerServiceBase",
    "KafkaService",
    "FlinkService",
    "QueryEngineService",
    "QueryEngineRustService",
    "QueryEngineServiceFactory",
    "MonitoringService",
    "ExporterServiceFactory",
    "PythonExporterService",
    "RustExporterService",
    "AvalancheExporterService",
    "SystemExportersService",
    "PrometheusService",
    "PrometheusKafkaAdapterService",
    "PrometheusClientService",
    "RemoteMonitorService",
    "DockerPrometheusService",
    "DockerVictoriaMetricsService",
    "ArroyoService",
    "ArroyoThroughputMonitor",
    "PrometheusThroughputMonitor",
    "DeathstarService",
    "ControllerService",
    "DumbKafkaConsumerService",
    "GrafanaService",
    "create_prometheus_service",
]
