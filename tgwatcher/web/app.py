"""TGWatcher Web Application — Flask server with REST API and static file serving."""
import argparse
import os
from pathlib import Path

from flask import Flask, send_from_directory, Response

from tgwatcher.web.api import api, init_services
from tgwatcher.web.async_loop import AsyncLoopManager
import yaml


def _resolve_config_path(config_path: str | None = None) -> str:
    if config_path:
        return config_path
    env_path = os.environ.get("TGWATCHER_CONFIG")
    if env_path:
        return env_path
    return str(Path.cwd() / "config.yaml")


def create_app(config_path: str | None = None) -> Flask:
    config_path = _resolve_config_path(config_path)
    with open(config_path, "r", encoding="utf-8") as f:
        config = yaml.safe_load(f)

    from tgwatcher.tz_utils import set_tz_offset
    set_tz_offset(config.get("timezone", {}).get("utc_offset_hours", 8))

    static_dir = Path(__file__).resolve().parent / "static"
    app = Flask(__name__, static_folder=str(static_dir))
    app.register_blueprint(api)

    async_loop = AsyncLoopManager()
    async_loop.start()
    init_services(config, async_loop=async_loop)

    @app.route("/")
    def index():
        return send_from_directory(app.static_folder, "index.html")

    @app.route("/favicon.ico")
    def favicon():
        return Response(status=204)

    @app.after_request
    def _no_cache_static(response):
        if response.content_type and ("javascript" in response.content_type or "html" in response.content_type):
            response.headers["Cache-Control"] = "no-cache, no-store, must-revalidate"
            response.headers["Pragma"] = "no-cache"
        return response

    return app


def main():
    parser = argparse.ArgumentParser(description="TGWatcher Web GUI")
    parser.add_argument("--port", type=int, default=5800, help="Port to run on")
    parser.add_argument("--config", type=str, default=None, help="Path to config.yaml")
    parser.add_argument("--debug", action="store_true", help="Enable debug mode")
    parser.add_argument("--no-browser", action="store_true", help="Don't open browser automatically")
    args = parser.parse_args()

    app = create_app(args.config)
    url = f"http://localhost:{args.port}"
    print(f"TGWatcher Web GUI starting on {url}")

    if not args.no_browser:
        import webbrowser
        import threading
        def _open():
            import time
            time.sleep(1.5)
            webbrowser.open(url)
        threading.Thread(target=_open, daemon=True).start()

    app.run(host="0.0.0.0", port=args.port, debug=args.debug)


if __name__ == "__main__":
    main()
