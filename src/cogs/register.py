import discord
from discord import app_commands
from discord.ext import commands
from src.database import db
import asyncio
from src.utils.opgg_client import opgg_client
from src.utils.opgg_compat import Region
import urllib.parse
import logging

logger = logging.getLogger(__name__)

class Register(commands.Cog):
    def __init__(self, bot):
        self.bot = bot

    user_group = app_commands.Group(name="user", description="LoLアカウントを管理します")

    async def _send_user_list(self, interaction: discord.Interaction):
        users = await db.get_users_by_server(interaction.guild.id)
        if not users:
            if interaction.response.is_done():
                await interaction.followup.send("このサーバーに登録されているユーザーはいません。")
            else:
                await interaction.response.send_message("このサーバーに登録されているユーザーはいません。")
            return

        msg = f"**{interaction.guild.name} の登録ユーザー一覧**\n"
        for u in users:
            member = interaction.guild.get_member(u['discord_id'])
            d_name = member.display_name if member else str(u['discord_id'])
            l_id = u.get('local_id', '-')
            msg += f"ID: {l_id} | Discord: {d_name} | Riot: {u['riot_id']}\n"
        
        if interaction.response.is_done():
            await interaction.followup.send(msg)
        else:
            await interaction.response.send_message(msg)

    @user_group.command(name="show", description="登録されているユーザーの一覧を表示します")
    async def user_show(self, interaction: discord.Interaction):
        await self._send_user_list(interaction)

    @user_group.command(name="add", description="ユーザーを登録します")
    @app_commands.describe(
        riot_id="Riot ID (Name#Tag) または OPGGのURL",
        discord_user="対象の表示名・名前・ID (未指定または 'me' で自分を登録)"
    )
    async def user_add(self, interaction: discord.Interaction, riot_id: str, discord_user: str = "me"):
        await interaction.response.defer()

        # 1. Resolve Target User
        target_user = None
        if discord_user.lower() == 'me':
            target_user = interaction.user
        else:
            guild = interaction.guild
            # Try ID first
            if discord_user.isdigit():
                target_user = guild.get_member(int(discord_user))
            
            # Try Name or Display Name
            if not target_user:
                target_user = discord.utils.find(
                    lambda m: m.name == discord_user or m.display_name == discord_user, 
                    guild.members
                )

        if not target_user:
            await interaction.followup.send(f"ユーザー '{discord_user}' が見つかりませんでした。表示名、またはIDを正しく入力してください。")
            return

        # 2. Parse Riot ID
        game_name, tag_line, error = self.parse_riot_id(riot_id)
        if error:
            await interaction.followup.send(error)
            return

        # 3. Validate and Register
        try:
            summoner = await opgg_client.get_summoner(game_name, tag_line, Region.JP)
            if not summoner:
                await interaction.followup.send(f"ユーザー '{game_name}#{tag_line}' がOP.GGで見つかりませんでした。")
                return
            
            fake_puuid = f"OPGG:{summoner.summoner_id}"
            real_riot_id = f"{game_name}#{tag_line.upper()}"
            
            await db.register_user(interaction.guild.id, target_user.id, real_riot_id, fake_puuid)
            await interaction.followup.send(f"✅ 登録完了: {target_user.display_name} -> **{real_riot_id}**")
            logger.info(f"Registered user: {target_user.display_name} ({target_user.id}) -> {real_riot_id} (Server: {interaction.guild.name})")
        except Exception as e:
            logger.error(f"Error registering user: {e}", exc_info=True)
            await interaction.followup.send(f"登録エラー: {e}")

    @user_group.command(name="del", description="IDを指定してユーザー登録を解除します")
    @app_commands.describe(user_id="解除するユーザーの登録ID")
    async def user_del(self, interaction: discord.Interaction, user_id: int):
        try:
            await db.delete_user_by_local_id(interaction.guild.id, user_id)
            await interaction.response.send_message(f"✅ 登録解除完了: ID {user_id}")
            logger.info(f"Deleted user by local_id: {user_id} (Server: {interaction.guild.name})")
            await self._send_user_list(interaction)
        except Exception as e:
            logger.error(f"Error deleting user: {e}", exc_info=True)
            if not interaction.response.is_done():
                await interaction.response.send_message(f"削除エラー: {e}", ephemeral=True)
            else:
                await interaction.followup.send(f"削除エラー: {e}")

    @user_group.command(name="help", description="userコマンドの使い方を表示します")
    async def user_help(self, interaction: discord.Interaction):
        msg = """
**user コマンドの使い方**
`/user show` : 現在登録されているユーザーの一覧を表示します。
`/user add riot_id: [RiotID] (discord_user: [対象])` : ユーザーを登録します。
`/user del user_id: [ID]` : 指定した ID の登録を解除します（IDは `/user show` で確認可能）。

**入力例**
- 自分の登録: `/user add riot_id: Name#Tag`
- 他人の登録(名前): `/user add riot_id: Name#Tag discord_user: 表示名`
- 他人の登録(ID): `/user add riot_id: Name#Tag discord_user: 1234567890`
- URLでの登録: `/user add riot_id: https://www.op.gg/summoners/jp/Name-Tag`
"""
        await interaction.response.send_message(msg)

    def parse_riot_id(self, input_str: str):
        if 'op.gg' in input_str:
            try:
                parsed = urllib.parse.urlparse(input_str)
                path_parts = parsed.path.split('/')
                if len(path_parts) >= 4 and path_parts[1] == 'summoners':
                    decoded_part = urllib.parse.unquote(path_parts[-1])
                    if '-' in decoded_part:
                        name = decoded_part.rsplit('-', 1)[0]
                        tag = decoded_part.rsplit('-', 1)[1]
                        return name, tag, None
                return None, None, "URL形式を認識できませんでした。"
            except Exception as e:
                return None, None, f"URL解析エラー: {e}"
        elif '#' in input_str:
            parts = input_str.split('#', 1)
            return parts[0], parts[1], None
        else:
            return None, None, "RiotIDの形式が正しくありません (Name#Tag または OPGGのURL)。"

async def setup(bot):
    await bot.add_cog(Register(bot))
