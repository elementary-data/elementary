def get_shortened_model_name(model):
    if model is None:
        # this can happen for example when a Singular test is failing for having no refs.
        return None
    # versioned models have 4 parts where the last part is version.
    return model.split(".", 2)[-1]
