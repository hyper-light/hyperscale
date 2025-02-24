import gzip
import re
from typing import Dict, Literal, List, TypeVar, Tuple

import orjson
from pydantic import (
    BaseModel, 
    StrictStr, 
    StrictBytes, 
    StrictInt, 
    StrictFloat,
)

from hyperscale.core.engines.client.shared.models import (
    CallResult,
    Cookies,
    RequestType,
    URLMetadata,
)
from hyperscale.core.engines.client.tracing import Span

space_pattern = re.compile(r"\s+")


T = TypeVar("T", bound=BaseModel)


SMTPTimings = dict[
    Literal[
        "request_start",
        "connect_start",
        "connect_end",
        "server_ack_start",
        "server_ack_end",
        "ehlo_start",
        "ehlo_end",
        "tls_check_start",
        "tls_check_end",
        "tls_upgrade_start",
        "tls_upgrade_end",
        "ehlo_tls_start",
        "ehlo_tls_end",
        "login_start",
        "login_end",
        "send_mail_start",
        "send_mail_end",
        "request_end",
    ],
    float | None,
]


class SMTPResponse(CallResult):
    recipients: StrictStr | List[StrictStr]
    sender: StrictStr
    server: StrictStr
    email: StrictStr
    recipient_responses: Dict[str, Tuple[int, str, Exception | None]] = None
    last_smtp_code: int | None = None,
    last_smtp_message: StrictStr | StrictBytes | None = None
    last_smtp_options: dict[
        StrictStr | StrictBytes,
        StrictStr | StrictBytes | StrictInt | StrictFloat | None,
    ]  | None = None
    encoding: StrictStr | None = None
    error: Exception | None = None
    timings:SMTPTimings | None = None
    trace: Span | None = None

    @property
    def status(self):
        return self.last_smtp_code


    @classmethod
    def response_type(cls):
        return RequestType.SMTP

    @property
    def successful(self) -> bool:

        if self.error:
            return False
        
        if self.recipient_responses:
            return len([
                err for _, _, err in self.recipient_responses.values() if err
            ]) < len(self.recipient_responses)
        
        return True
    
    def context(self):
        return self.last_smtp_message
