
from pydantic import BaseModel, Field, HttpUrl, FilePath, PositiveInt
from typing import List, Literal, Optional, Union

class CertConfig(BaseModel):
    p12_path: FilePath
    p12_password: str

class ConnectionConfig(BaseModel):
    wsdl_url: HttpUrl = "https://service.bloomberg.com/assets/dl/dlws.wsdl"
    endpoint: HttpUrl
    cert: CertConfig

class CsvSourceConfig(BaseModel):
    path: FilePath
    id_column: str = "id"
    yellow_key_column: str = "yellow_key"
    type_column: str = "type"
    extra_columns: List[str] = []

class IdentifiersConfig(BaseModel):
    source: Literal["csv", "inline"] = "csv"
    csv: Optional[CsvSourceConfig] = None
    inline: List[dict] = Field(default_factory=list)

class OverrideKV(BaseModel):
    name: str
    value: str

class FieldsConfig(BaseModel):
    # Either inline list or file path
    inline: List[str] = Field(default_factory=list)
    file: Optional[FilePath] = None

class RequestConfig(BaseModel):
    kind: Literal["history", "data", "fundamentals_headers"] = "history"
    type: Literal["bulk", "hist", "instrument_list"] = "bulk"
    identifiers: IdentifiersConfig
    fields: FieldsConfig
    overrides: List[OverrideKV] = Field(default_factory=list)

    # Optional op-specific params (keep minimal; extend later)
    history_params: dict = Field(default_factory=dict)   # e.g., {"start":"2024-01-01","end":"2024-12-31","periodicity":"DAILY"}
    data_params: dict = Field(default_factory=dict)      # reserved
    fundamentals_params: dict = Field(default_factory=dict)  # e.g., {"keyword":"", "dlCategories": ["Fundamentals"]}

class ChunkingConfig(BaseModel):
    enabled: bool = True
    max_identifiers_per_request: PositiveInt = 500

class PollingConfig(BaseModel):
    attempts: PositiveInt = 120
    interval_seconds: PositiveInt = 5
    per_attempt_timeout_seconds: PositiveInt = 15

class OutputConfig(BaseModel):
    uri: str
    format: Literal["csv"] = "csv"
    include_raw_xml: bool = False
    append_mode: bool = False

class LoggingConfig(BaseModel):
    level: Literal["DEBUG", "INFO", "WARNING", "ERROR"] = "INFO"
    json_mode: bool = False

class AppConfig(BaseModel):
    connection: ConnectionConfig
    request: RequestConfig
    chunking: ChunkingConfig = ChunkingConfig()
    polling: PollingConfig = PollingConfig()
    output: OutputConfig
    logging: LoggingConfig = LoggingConfig()
