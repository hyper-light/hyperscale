from pydantic import BaseModel, StrictInt, StrictFloat, StrictStr


class FileAttributes(BaseModel):
    size: StrictInt | None = None
    alloc_size: StrictInt | None = None
    uid: StrictInt | None = None
    gid: StrictInt | None = None
    owner: StrictStr | None = None
    group: StrictStr | None = None
    permissions: StrictInt | None = None
    atime: StrictInt | None = None
    atime_ns: StrictInt | StrictFloat | None = None
    crtime: StrictInt | None = None
    crtime_ns: StrictInt | StrictFloat | None = None
    mtime: StrictInt | None = None
    mtime_ns: StrictInt | StrictFloat | None = None
    ctime: StrictInt | None = None
    ctime_ns: StrictInt | StrictFloat | None = None
    mime_type: StrictStr | None = None
    nlink: StrictInt | None = None