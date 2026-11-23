import importlib.util
import logging
import os
import sys
from pathlib import Path
from typing import Any, Dict, Optional

logger = logging.getLogger("lambda_loader")


def setup_lambda_environment():
    """Ensure all required environment variables are set before any lambda module is imported."""
    defaults = {
        "DYNAMODB_TABLE": "enrichment_cache",
        "DYNAMODB_ENDPOINT": "http://dynamodb-local:8000",
        "OPENSEARCH_HOST": "opensearch",
        "OPENSEARCH_PORT": "9200",
        "OPENSEARCH_SCHEME": "http",
        "OPENSEARCH_AUTH": "basic",
        "OPENSEARCH_USERNAME": "admin",
        "OPENSEARCH_PASSWORD": "admin",
        "OPENSEARCH_MODE": "provisioned",
        "AWS_REGION": "us-east-1",
        "AWS_ACCESS_KEY_ID": "local",
        "AWS_SECRET_ACCESS_KEY": "local",
        "INDEX_ORDER": "orders",
        "INDEX_ORDER_LINES": "order-lines",
        "INDEX_ORGANIZATION": "organizations",
        "INDEX_PRODUCTS": "products",
    }
    for k, v in defaults.items():
        if k not in os.environ:
            os.environ[k] = v


class LambdaRegistry:
    def __init__(self, lambdas_dir: Optional[str] = None):
        setup_lambda_environment()
        self.lambdas_dir = Path(lambdas_dir or os.environ.get("LAMBDAS_DIR", "/app/lambdas"))
        if not self.lambdas_dir.exists():
            # Fallback to local repo path if running outside docker
            self.lambdas_dir = Path(__file__).resolve().parent.parent.parent / "lambdas"

        self._modules: Dict[str, Any] = {}
        logger.info(f"Initialized LambdaRegistry with lambdas dir: {self.lambdas_dir}")

    def get_processor(self, folder_name: str) -> Any:
        if folder_name in self._modules:
            return self._modules[folder_name]

        lambda_path = self.lambdas_dir / folder_name / "lambda_function.py"
        if not lambda_path.exists():
            raise FileNotFoundError(f"Lambda entrypoint not found at: {lambda_path}")

        module_name = f"mod_{folder_name.replace('-', '_')}"
        logger.info(f"Dynamically loading Lambda module: {folder_name} from {lambda_path}")

        spec = importlib.util.spec_from_file_location(module_name, str(lambda_path))
        if spec is None or spec.loader is None:
            raise ImportError(f"Could not load spec for module: {module_name}")

        module = importlib.util.module_from_spec(spec)
        sys.modules[module_name] = module
        spec.loader.exec_module(module)

        self._modules[folder_name] = module
        return module

