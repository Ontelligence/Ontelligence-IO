import os
import inspect
import mako.template
import jinja2


_ENGINES = {
    'jinja': jinja2.Template,
    'mako': mako.template.Template
}


def render_template(file_path: str, engine='jinja', *args, **kwargs):
    abs_path = os.path.join(os.path.dirname(inspect.getmodule(inspect.stack()[1][0]).__file__), file_path)
    with open(abs_path, 'r') as f:
        template = _ENGINES[engine](f.read())
    return template.render(*args, **kwargs)
