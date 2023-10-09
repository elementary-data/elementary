def pluralize_string(number, singular_form, plural_form):
    if number == 1:
        return f"{number} {singular_form}"
    else:
        return f"{number} {plural_form}"
