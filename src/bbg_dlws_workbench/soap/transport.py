
import ssl, requests, urllib3, tempfile
from requests.adapters import HTTPAdapter
from urllib3 import PoolManager
from cryptography.hazmat.primitives.serialization import Encoding, NoEncryption, PrivateFormat
from cryptography.hazmat.primitives.serialization.pkcs12 import load_key_and_certificates

class P12HttpAdapter(HTTPAdapter):
    def __init__(self, p12_path: str, p12_password: str, **kwargs):
        self.p12_path = p12_path
        self.p12_password = p12_password
        super().__init__(**kwargs)

    def init_poolmanager(self, connections, maxsize, block=False, **pool_kwargs):
        ctx = ssl.create_default_context(purpose=ssl.Purpose.SERVER_AUTH)
        ctx.minimum_version = ssl.TLSVersion.TLSv1_2
        # Load p12
        with open(self.p12_path, "rb") as f:
            key, cert, chain = load_key_and_certificates(f.read(), self.p12_password.encode())
        pem_key = key.private_bytes(Encoding.PEM, PrivateFormat.TraditionalOpenSSL, NoEncryption())
        pem_cert = cert.public_bytes(Encoding.PEM)
        pem_chain = b"".join(c.public_bytes(Encoding.PEM) for c in (chain or []))
        # Write temp PEMs; load into SSL context
        with tempfile.NamedTemporaryFile(delete=False) as cert_file, tempfile.NamedTemporaryFile(delete=False) as key_file:
            cert_file.write(pem_cert + pem_chain)
            cert_file.flush()
            key_file.write(pem_key)
            key_file.flush()
            ctx.load_cert_chain(certfile=cert_file.name, keyfile=key_file.name)
        pool_kwargs["ssl_context"] = ctx
        self.poolmanager = PoolManager(num_pools=connections, maxsize=maxsize, block=block, **pool_kwargs)

def build_session_with_p12(p12_path: str, p12_password: str) -> requests.Session:
    s = requests.Session()
    s.mount("https://", P12HttpAdapter(p12_path, p12_password))
    return s
