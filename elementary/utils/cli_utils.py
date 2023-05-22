import click
from click import ClickException


class RequiredIf(click.Option):
    def __init__(self, *args, **kwargs):
        self.required_if = kwargs.pop("required_if")
        if not self.required_if:
            raise ClickException("'required_if' parameter is required")

        kwargs["help"] = (
            kwargs.get("help", "")
            + " NOTE: This argument must be configured together with %s."
            % self.required_if
        ).strip()
        super(RequiredIf, self).__init__(*args, **kwargs)

    def handle_parse_result(self, ctx, opts, args):
        we_are_present = self.name in opts
        other_present = self.required_if in opts

        if we_are_present and not other_present:
            raise click.UsageError(
                "Illegal usage: `%s` must be configured with `%s`"
                % (self.name, self.required_if)
            )
        else:
            self.prompt = None

        return super(RequiredIf, self).handle_parse_result(ctx, opts, args)
