import logging

logger = logging.getLogger(__name__)

import discord
from discord import app_commands
from discord.ext import commands

class Utils(commands.Cog):
    def __init__(self, bot):
        self.bot = bot

    @commands.command()
    async def ping(self, ctx):
        """Botの生存確認用コマンド"""
        logger.info(f"DEBUG: 'ping' command triggered by {ctx.author}")
        latency = round(self.bot.latency * 1000)
        await ctx.send(f'Pong! (応答速度: {latency}ms)')

    @commands.command()
    async def sync(self, ctx):
        """スラッシュコマンドを現在のサーバーに強制同期します"""
        logger.info(f"DEBUG: 'sync' command triggered by {ctx.author}")
        if not ctx.author.guild_permissions.administrator:
            await ctx.send("このコマンドは管理者専用です。")
            return
            
        await ctx.send("スラッシュコマンドを同期中...")
        try:
            self.bot.tree.copy_global_to(guild=ctx.guild)
            synced = await self.bot.tree.sync(guild=ctx.guild)
            await ctx.send(f"{len(synced)} 件のコマンドをこのサーバーに同期しました。/コマンドが使えるようになっているはずです。")
        except Exception as e:
            await ctx.send(f"同期中にエラーが発生しました: {e}")

    @commands.command()
    async def unsync(self, ctx):
        """このサーバー固有のスラッシュコマンド設定を削除します（重複解消用）"""
        logger.info(f"DEBUG: 'unsync' command triggered by {ctx.author}")
        if not ctx.author.guild_permissions.administrator:
            await ctx.send("このコマンドは管理者専用です。")
            return
            
        await ctx.send("サーバー固有のコマンド設定を削除中...")
        try:
            self.bot.tree.clear_commands(guild=ctx.guild)
            await self.bot.tree.sync(guild=ctx.guild)
            await ctx.send("このサーバー固有のコマンド設定を削除しました。重複が解消されるまで数分かかる場合があります。")
        except Exception as e:
            await ctx.send(f"削除中にエラーが発生しました: {e}")

async def setup(bot):
    await bot.add_cog(Utils(bot))
