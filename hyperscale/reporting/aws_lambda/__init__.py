from .aws_lambda_config import AWSLambdaConfig as AWSLambdaConfig


def __getattr__(name: str):
    # Load the reporter, and the client library it imports, only when it's used.
    if name == "AWSLambda":
        from .aws_lambda import AWSLambda

        globals()["AWSLambda"] = AWSLambda
        return AWSLambda

    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")
