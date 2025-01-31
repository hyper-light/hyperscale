from hyperscale.core.engines.client.http2.settings import ChangedSetting, Settings, SettingCodes
from .base_event import BaseEvent


class RemoteSettingsChanged(BaseEvent):
    """
    The RemoteSettingsChanged event is fired whenever the remote peer changes
    its settings. It contains a complete inventory of changed settings,
    including their previous values.

    In HTTP/2, settings changes need to be acknowledged. h2 automatically
    acknowledges settings changes for efficiency. However, it is possible that
    the caller may not be happy with the changed setting.

    When this event is received, the caller should confirm that the new
    settings are acceptable. If they are not acceptable, the user should close
    the connection with the error code :data:`PROTOCOL_ERROR
    <h2.errors.ErrorCodes.PROTOCOL_ERROR>`.

    .. versionchanged:: 2.0.0
       Prior to this version the user needed to acknowledge settings changes.
       This is no longer the case: h2 now automatically acknowledges
       them.
    """

    def __init__(
        self,
        old_settings: Settings | dict[int, int],
        new_settings: dict[int, int],
    ) -> None:
        #: A dictionary of setting byte to
        #: :class:`ChangedSetting <h2.settings.ChangedSetting>`, representing
        #: the changed settings.
        self.changed_settings: dict[int, ChangedSetting] = {}

        for setting, new_value in new_settings.items():

            try:
                s = SettingCodes(setting)

            except Exception:
                s = setting

            original_value = old_settings.get(s)
            change = ChangedSetting(s, original_value, new_value)
            self.changed_settings[s] = change

    def __repr__(self) -> str:
        return "<RemoteSettingsChanged changed_settings:{{{}}}>".format(
            ", ".join(repr(cs) for cs in self.changed_settings.values()),
        )