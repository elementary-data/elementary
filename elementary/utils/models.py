def get_shortened_model_name(model):
    if model is None:
        # this can happen for example when a Singular test is failing for having no refs.
        return None
    return model.split(".")[-1]
