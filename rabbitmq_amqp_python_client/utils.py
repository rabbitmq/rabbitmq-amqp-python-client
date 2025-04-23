def validate_annotations(annotations: []) -> bool:  # type: ignore
    validated = True
    for annotation in annotations:
        if annotation.startswith("x-"):
            pass
        else:
            validated = False
            return validated
    return validated
