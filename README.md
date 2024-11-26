Pyinstaller

Fix error in
root\.venv\Lib\site-packages\plombery\notifications\templates.py
Package Loader Problem, this works:

# from jinja2 import Environment, PackageLoader, select_autoescape

# _jinja_env = Environment(
#     loader=PackageLoader("plombery.notifications", package_path="email_templates"),
#     autoescape=select_autoescape(),
# )

# _pipeline_run_template = _jinja_env.get_template("transactional.html")

def render_pipeline_run(
    pipeline_name: str, pipeline_status: str, pipeline_run_url: str
):
    return ""
    # return _pipeline_run_template.render(
    #     pipeline_name=pipeline_name,
    #     pipeline_status=pipeline_status,
    #     pipeline_run_url=pipeline_run_url,
    # )


Fix error in root\.venv\Lib\site-packages\starlette\staticfiles.py
if check_dir and directory is not None and not os.path.isdir(directory):
            os.makedirs(directory)
            #raise RuntimeError(f"Directory '{directory}' does not exist")



pyinstaller -F --collect-all apprise app.py