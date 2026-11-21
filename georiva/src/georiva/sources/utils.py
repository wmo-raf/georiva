from django.apps import apps


def get_all_child_models(base_model):
    all_models = apps.get_models()

    child_models = [model for model in all_models if issubclass(model, base_model) and model is not base_model]
    return child_models


def get_child_model_by_name(base_model, model_name):
    child_models = get_all_child_models(base_model)

    for model in child_models:
        verbose_name = model._meta.verbose_name

        if model.__name__.lower() == model_name.lower():
            return model

        if verbose_name.lower() == model_name.lower():
            return model

    return None
