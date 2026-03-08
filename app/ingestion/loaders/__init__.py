from app.ingestion.interfaces.session_loader import SessionLoader
from app.ingestion.loaders.acn_loader import AcnLoader
from app.ingestion.loaders.urbanev_loader import UrbanevLoader

LOADER_REGISTRY: dict[str, type[SessionLoader]] = {
    "acn": AcnLoader,
    "urbanev": UrbanevLoader,
}


def get_loader(dataset_name: str) -> SessionLoader:
    """Get a loader instance by dataset name."""
    loader_cls = LOADER_REGISTRY.get(dataset_name)
    if loader_cls is None:
        available = ", ".join(sorted(LOADER_REGISTRY.keys()))
        raise ValueError(f"Unknown dataset '{dataset_name}'. Available: {available}")
    return loader_cls()
