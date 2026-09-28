def test_package_and_app_import():
    import main
    from pipeline_orchestrator.executor import Orchestrator
    from pipeline_orchestrator.store import RunStore, VersionedDataSource

    # OpenAPI schema 在生成时会解析懒加载的 include_router
    paths = main.app.openapi()["paths"]
    assert "/api/pipelines" in paths
    assert "/api/pipelines/{pipeline_name}/trigger" in paths
    assert "/api/pipelines/runs/{run_id}/lineage" in paths
