def validate_annotations(annotations: []) -> bool:  # type: ignore
    validated = True
    for annotation in annotations:
        if len(annotation) > 0 and annotation[:2] == "x-":
            pass
        else:
            validated = False
            return validated
    return validated
