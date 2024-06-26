class UnsafeNotEnabledException(Exception):
    def __init__(self, feature_name: str) -> None:
        super().__init__(
            f"\nErr. - Attempting to use unsafe feature - {feature_name} - wihtout --enable-unsafe flag.\nPlease pass this flag if you want to use unstable features.\n"
        )
