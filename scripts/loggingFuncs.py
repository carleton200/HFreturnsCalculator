import logging, functools
def log_exceptions(method):
    @functools.wraps(method)
    def wrapper(*args, **kwargs):
        try:
            return method(*args, **kwargs)
        except Exception as e:
            logging.exception(f"Error in {method.__qualname__}: {e}")
            raise  # Re-raise the exception after logging
    return wrapper
def attach_logging_to_class(cls):
    for attr_name, attr_value in cls.__dict__.items():
        if callable(attr_value):  # Only wrap methods
            setattr(cls, attr_name, log_exceptions(attr_value))
    return cls