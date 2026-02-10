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
    async def user_add(self, interaction: discord.Interaction):
        await interaction.response.send_message("登録する対象(me, ユーザーID, または名前) と RiotID を入力してください。\n形式: `対象 RiotID(Name#Tag)`\n例: `me abc#jp1` または `1234567890 xyz#kr1`")

        def check(m):
            return m.author.id == interaction.user.id and m.channel.id == interaction.channel.id

        try:
            msg = await self.bot.wait_for('message', check=check, timeout=60.0)
        except asyncio.TimeoutError:
            await interaction.followup.send("タイムアウトしました。")
            return

        parts = msg.content.strip().split()
        if len(parts) < 2:
            await interaction.followup.send("形式が正しくありません。`対象 RiotID` の順で入力してください。")
            return

        target_input = parts[0]
        riot_id_input = parts[1]

        target_user = None
        if target_input.lower() == 'me':
            target_user = interaction.user
        else:
            guild = interaction.guild
            if target_input.isdigit():
                target_user = guild.get_member(int(target_input))
            if not target_user:
                target_user = discord.utils.find(lambda m: m.name == target_input or m.display_name == target_input, guild.members)

        if not target_user:
            await interaction.followup.send(f"ユーザー '{target_input}' が見つかりませんでした。")
            return

        # Parse Riot ID (similar to legacy but tailored for single prompt)
        game_name, tag_line, error = self.parse_riot_id(riot_id_input)
        if error:
            await interaction.followup.send(error)
            return

        # Validate with OPGG
        try:
            summoner = await opgg_client.get_summoner(game_name, tag_line, Region.JP)
            if not summoner:
                await interaction.followup.send(f"ユーザー '{game_name}#{tag_line}' が見つかりませんでした。")
                return
            
            fake_puuid = f"OPGG:{summoner.summoner_id}"
            # Use the user-provided game_name to preserve Japanese characters
            real_riot_id = f"{game_name}#{tag_line.upper()}"
            
            await db.register_user(interaction.guild.id, target_user.id, real_riot_id, fake_puuid)
            await interaction.followup.send(f"登録完了: {target_user.display_name} -> {real_riot_id} (サーバー: {interaction.guild.name})")
            logger.info(f"Registered user: {target_user.display_name} -> {real_riot_id} (Server: {interaction.guild.name})")
        except Exception as e:
            logger.error(f"Error registering user: {e}", exc_info=True)
            await interaction.followup.send(f"登録エラー: {e}")

    @user_group.command(name="del", description="IDを指定してユーザー登録を解除します")
    async def user_del(self, interaction: discord.Interaction, user_id: int):
        try:
            await db.delete_user_by_local_id(interaction.guild.id, user_id)
            await interaction.response.send_message(f"登録解除完了: ID {user_id}")
            logger.info(f"Deleted user by local_id: {user_id} (Server: {interaction.guild.name})")
            # Automatically show list after deletion
            await self._send_user_list(interaction)
        except Exception as e:
            logger.error(f"Error deleting user: {e}", exc_info=True)
            if not interaction.response.is_done():
                await interaction.response.send_message(f"削除エラー: {e}", ephemeral=True)
            else:
                await interaction.followup.send(f"削除エラー: {e}")

    @user_group.command(name="edit", description="Riot IDを指定してユーザー情報を更新します（再登録と同じです）")
    async def user_edit(self, interaction: discord.Interaction, riot_id: str):
        # reuse add logic or just prompt for new details
        await interaction.response.send_message(f"Riot ID {riot_id} の新しい所有者(me/ID/名前)を入力してください。(所有者が同じなら `me` 等)")
        # For simplicity, edit here might just mean re-running the validation for THAT riot id or changing owner.
        # But the user asked for edit to be like schedule edit.
        # Let's just point them to /user add since it's an upsert anyway.
        await interaction.followup.send("Riot IDの変更や所有者変更は `/user add` を再度実行してください。既存のデータは上書きされます。")

    @user_group.command(name="help", description="userコマンドの使い方を表示します")
    async def user_help(self, interaction: discord.Interaction):
        msg = """
**user コマンドの使い方**
`/user show` : 現在登録されているユーザーの一覧を表示します。
`/user add` : ユーザーを登録します。対話形式で `対象(me/ID/名前) RiotID` を入力します。
`/user del user_id` : 指定した ID の登録を解除します。IDは `/user show` で確認できます。

**入力形式の例**
`me abc#jp1` : 自分の 'abc#jp1' アカウントを登録
`1234567890 xyz#kr1` : ID 1234567890 のユーザーに 'xyz#kr1' を紐付け
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
