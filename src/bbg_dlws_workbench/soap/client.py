
from zeep import Client, Settings
from zeep.transports import Transport
from .transport import build_session_with_p12

def create_client(wsdl_url: str, p12_path: str, p12_password: str) -> Client:
    session = build_session_with_p12(p12_path, p12_password)
    transport = Transport(session=session, operation_timeout=30)
    settings = Settings(strict=False, xml_huge_tree=True)
    return Client(wsdl=wsdl_url, transport=transport, settings=settings)
