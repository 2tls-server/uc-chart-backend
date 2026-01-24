from core import ChartFastAPI

from fastapi import APIRouter, Request, HTTPException, status
from helpers.session import get_session, Session

from database import charts, leaderboards

from helpers.models import ChartVisibilityData
from helpers.webhook_handler import WebhookMessage, WebhookEmbed
from helpers.sanitizers import sanitize_md
from helpers.urls import url_creator

router = APIRouter()


@router.patch("/")
async def main(
    request: Request,
    id: str,
    data: ChartVisibilityData,
    session: Session = get_session(
        enforce_auth=True, enforce_type=False, allow_banned_users=False
    ),
):
    if len(id) != 32 or not id.isalnum():
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST, detail="Invalid chart ID."
        )
    app: ChartFastAPI = request.app
    user = await session.user()

    if user.mod:
        query = charts.update_status(chart_id=id, status=data.status)
    else:
        query = charts.update_status(
            chart_id=id, sonolus_id=user.sonolus_id, status=data.status
        )

    async with app.db_acquire() as conn:
        result = await conn.fetchrow(query)
        if result:
            await conn.execute(leaderboards.update_leaderboard_visibility(chart_id=id, status=data.status))

            d = result.model_dump()
            if app.config["discord"]["all-visibility-changes-webhook"].strip() != "":
                wmsg = WebhookMessage(
                    app.config["discord"]["all-visibility-changes-webhook"],
                    app.config["discord"]["avatar-url"],
                    app.config["discord"]["username"],
                )
                wembed = (
                    WebhookEmbed()
                    .set_title("Chart visibility change")
                    .set_description(
                        f"The chart `{sanitize_md(result.title)}` (`{sanitize_md(result.author_full)}`) was changed to `{data.status}` from `{result.status}`.\n\n{url_creator(app.config['server']['sonolus-server-url'], 'levels', app.config['server']['sonolus-server-chart-prefix'] + result.id, as_sonolus_open=True)}"
                    )
                    .set_timestamp(True)
                    .set_thumbnail(
                        url_creator(
                            app.s3_asset_base_url,
                            result.author,
                            result.id,
                            result.jacket_file_hash,
                        )
                    )
                    .set_color(
                        "RED"
                        if data.status == "PRIVATE"
                        else "ORANGE" if data.status == "UNLISTED" else "GREEN"
                    )
                )
                wmsg.add_embed(wembed)
                await wmsg.send()
            if (
                app.config["discord"]["published-webhook"]
                if result.is_first_publish
                else ""
            ).strip() != "":
                wmsg = WebhookMessage(
                    app.config["discord"]["published-webhook"],
                    app.config["discord"]["avatar-url"],
                    app.config["discord"]["username"],
                )
                wembeds = [
                    WebhookEmbed()
                    .set_description(
                        "## 創作譜面が公開されました / New Chart Published!"
                    )
                    .set_color("PURPLE")
                ]
                wembed = (
                    WebhookEmbed()
                    .set_title(sanitize_md(result.title))
                    .set_description(
                        f"- *{sanitize_md(result.artists)}*\n譜面作者 / Charted by: `{sanitize_md(result.author_full)}`\n\n今すぐプレイ！ / Play it now!\n{url_creator(app.config['server']['sonolus-server-url'], 'levels', app.config['server']['sonolus-server-chart-prefix'] + result.id, as_sonolus_open=True)}"
                    )
                    .set_timestamp(True)
                    .set_thumbnail(
                        url_creator(
                            app.s3_asset_base_url,
                            result.author,
                            result.id,
                            result.jacket_file_hash,
                        )
                    )
                    .set_color("BLUE")
                )
                wembeds.append(wembed)
                for embed in wembeds:
                    wmsg.add_embed(embed)
                await wmsg.send()
            if user.mod:
                d["mod"] = True
            if user.sonolus_id == d["author"]:
                d["owner"] = True
            return d
        if user.mod:
            raise HTTPException(
                status=status.HTTP_404_NOT_FOUND,
                detail=f'Chart with ID "{id}" not found for any user!',
            )
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f'Chart with ID "{id}" not found for this user.',
        )
