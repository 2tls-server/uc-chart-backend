import os, importlib
from urllib.parse import urlparse

from concurrent.futures import ThreadPoolExecutor

from fastapi.middleware.cors import CORSMiddleware
from starlette.middleware.sessions import SessionMiddleware
from starlette.middleware.trustedhost import TrustedHostMiddleware
import uvicorn

from authlib.integrations.starlette_client import OAuth

from helpers.config_loader import get_config
from core import ChartFastAPI

config = get_config()
debug = config.get("server", {}).get("debug")

if debug:
    app = ChartFastAPI(config=config)
else:
    app = ChartFastAPI(
        config=config,
        docs_url=None,
        redoc_url=None,
        openapi_url=None,
    )
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)
app.add_middleware(SessionMiddleware, secret_key=config["server"]["secret-key"])
if not debug:
    domain = urlparse(config["server"]["base-url"]).netloc
    app.add_middleware(TrustedHostMiddleware, allowed_hosts=[domain, "127.0.0.1"])


@app.middleware("http")
async def force_https_redirect(request, call_next):
    response = await call_next(request)

    if config["server"]["force-https"] and not debug:
        if response.headers.get("Location"):
            response.headers["Location"] = response.headers.get("Location").replace(
                "http://", "https://", 1
            )

    return response


# app.mount("/static", StaticFiles(directory="static"), name="static")
# templates = Jinja2Templates(directory="templates")


import os
import importlib


def load_routes(folder, cleanup: bool = True):
    global app
    """Load Routes from the specified directory."""

    routes = []

    def traverse_directory(directory):
        for root, dirs, files in os.walk(directory, topdown=False):
            for file in files:
                if not "__pycache__" in root and os.path.join(root, file).endswith(
                    ".py"
                ):
                    route_name: str = (
                        os.path.join(root, file)
                        .removesuffix(".py")
                        .replace("\\", "/")
                        .replace("/", ".")
                    )

                    # Check if the route is dynamic or static
                    if "{" in route_name and "}" in route_name:
                        routes.append(
                            (route_name, False)
                        )  # Dynamic route (priority lower)
                    else:
                        routes.append(
                            (route_name, True)
                        )  # Static route (priority higher)

    traverse_directory(folder)

    # Sort the routes: static first, dynamic last. Deeper routes (subdirectories) have higher priority.
    # We are sorting by two factors:
    # 1. Whether the route is static (True first) or dynamic (False second).
    # 2. Depth of the route (deeper subdirectory routes come first).
    routes.sort(key=lambda x: (not x[1], x[0]))  # Static first, dynamic second

    for route_name, is_static in routes:
        try:
            route = importlib.import_module(route_name)
        except NotImplementedError:
            continue

        route_version = route_name.split(".")[0]
        route_name_parts = route_name.split(".")

        # it's the index for the route
        if route_name.endswith(".index"):
            del route_name_parts[-1]

        route_name = ".".join(route_name_parts)
        app.include_router(
            route.router,
            prefix="/" + route_name.replace(".", "/"),
            tags=(
                route.router.tags + [route_version]
                if isinstance(route.router.tags, list)
                else [route_version]
            ),
        )

        print(f"[API] Loaded Route {route_name}")


async def startup_event():
    oauth = OAuth()
    oauth.register(
        name="discord",
        client_id=config["oauth"]["discord-client-id"],
        client_secret=config["oauth"]["discord-client-secret"],
        access_token_url="https://discord.com/api/oauth2/token",
        access_token_params=None,
        authorize_url="https://discord.com/api/oauth2/authorize",
        authorize_params=None,
        api_base_url="https://discord.com/api/v10/",
        client_kwargs={"scope": "identify guilds"},
    )
    app.oauth = oauth
    await app.init()
    folder = "api"
    if len(os.listdir(folder)) == 0:
        print("[WARN] No routes loaded.")
    else:
        load_routes(folder)
        print("Routes loaded!")


app.add_event_handler("startup", startup_event)


async def start_fastapi():
    config_server = uvicorn.Config(
        "app:app",
        host="0.0.0.0",
        port=config["server"]["port"],
        workers=9,
        log_level="critical",
    )
    server = uvicorn.Server(config_server)
    await server.serve()


if __name__ == "__main__":
    raise SystemExit("Please run main.py")
