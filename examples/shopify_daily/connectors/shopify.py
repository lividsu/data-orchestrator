from orchestrator import BaseConnector, register_connector


@register_connector("shopify")
class ShopifyConnector(BaseConnector):
    def fetch(self, **kwargs):
        raise NotImplementedError

    def push(self, data, **kwargs):
        raise NotImplementedError

    def ping(self) -> bool:
        return False
