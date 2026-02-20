import discord
from discord import app_commands
from discord.ext import commands
from apscheduler.schedulers.asyncio import AsyncIOScheduler
from src.database import db
from src.utils import rank_calculator
from src.utils.opgg_client import opgg_client
from src.utils.opgg_compat import Region, OPGG, IS_V2
from src.utils.graph_generator import generate_rank_graph, generate_report_image
from datetime import datetime, date, timedelta
import asyncio
import io
import logging
from tabulate import tabulate

logger = logging.getLogger(__name__)

class Scheduler(commands.Cog):
    def __init__(self, bot):
        self.bot = bot
        self.scheduler = AsyncIOScheduler()
        self.scheduler.start()

    async def cog_load(self):
        await self.reload_schedules()

    async def reload_schedules(self):
        self.scheduler.remove_all_jobs()
        
        # 1. System-wide Rank Collection Job (Daily 23:55)
        # Records data for the current day
        self.scheduler.add_job(
            self.fetch_all_users_rank,
            'cron',
            hour=23,
            minute=55,
            second=0,
            name="daily_rank_fetch"
        )

        # 2. User-defined Reporting Jobs
        schedules = await db.get_all_schedules()
        for s in schedules:
            sched_time = s['schedule_time']
            channel_id = s['channel_id']
            # period_days is valid but we prefer period_type now
            # Legacy fallback: if period_type is missing, infer from days?
            # DB migration ensures period_type exists (defaults to 'daily')
            period_type = s.get('period_type', 'daily')
            server_id = s['server_id'] or 0
            
            if s['status'] != 'ENABLED':
                continue

            # Configure cron parameters based on period_type
            cron_kwargs = {
                'hour': sched_time.hour,
                'minute': sched_time.minute,
                'second': sched_time.second
            }
            
            if period_type == 'weekly':
                cron_kwargs['day_of_week'] = 'fri'
            elif period_type == 'monthly':
                cron_kwargs['day'] = 1

            self.scheduler.add_job(
                self.run_daily_report,
                'cron',
                args=[server_id, channel_id, period_type, s['output_type']],
                **cron_kwargs
            )
        logger.info(f"Loaded {len(schedules)} reporting schedules (including Weekly/Monthly constraints).")


    # Schedule Command Group
    schedule_group = app_commands.Group(name="schedule", description="定期実行スケジュールを管理します")

    async def _send_schedule_list(self, interaction: discord.Interaction):
        schedules = await db.get_schedules_by_server(interaction.guild.id)
        if not schedules:
            if interaction.response.is_done():
                await interaction.followup.send("このサーバーに登録されているスケジュールはありません。")
            else:
                await interaction.response.send_message("このサーバーに登録されているスケジュールはありません。")
            return

        msg = "**登録スケジュール一覧**\n"
        for s in schedules:
            t = s['schedule_time']
            t_str = t.strftime("%H:%M") if hasattr(t, 'strftime') else str(t)
            status_emoji = "✅" if s['status'] == 'ENABLED' else "❌"
            
            l_id = s.get('local_id', '-')
            p_type = s.get('period_type', 'daily')
            p_display = "日時"
            if p_type == 'weekly': p_display = "週次"
            elif p_type == 'monthly': p_display = "月次"
            msg += f"{status_emoji} ID: {l_id} | 時間: {t_str} | Ch: <#{s['channel_id']}> | 期間: {p_display} | 形式: {s['output_type']}\n"
        
        if interaction.response.is_done():
            await interaction.followup.send(msg)
        else:
            await interaction.response.send_message(msg)

    @schedule_group.command(name="show", description="現在登録されているスケジュールの一覧を表示します")
    async def schedule_show(self, interaction: discord.Interaction):
        await self._send_schedule_list(interaction)

    @schedule_group.command(name="add", description="スケジュールを登録します")
    async def schedule_add(self, interaction: discord.Interaction):
        await interaction.response.send_message("登録するスケジュールを入力してください。\n形式: `時間(HH:MM) チャンネル(ID/here) 期間(daily/weekly/monthly) 出力形式(table/graph)`\n例: `21:00 here daily graph`")

        def check(m):
            return m.author.id == interaction.user.id and m.channel.id == interaction.channel.id

        try:
            msg = await self.bot.wait_for('message', check=check, timeout=60.0)
        except asyncio.TimeoutError:
            await interaction.followup.send("タイムアウトしました。")
            return

        time_str, channel_id, period_type, output_type, error = self.parse_schedule_input(msg.content, interaction.channel.id)
        if error:
            await interaction.followup.send(error)
            return

        try:
            await db.register_schedule(interaction.guild.id, time_str, channel_id, interaction.user.id, period_type, output_type)
            await self.reload_schedules()
            
            p_msg = "毎日"
            if period_type == 'weekly': p_msg = "毎週金曜日"
            elif period_type == 'monthly': p_msg = "毎月1日"
            
            await interaction.followup.send(f"スケジュール登録完了: {time_str} にチャンネル <#{channel_id}> へ通知 ({p_msg}, 形式: {output_type})")
        except Exception as e:
            await interaction.followup.send(f"エラーが発生しました: {e}")

    @schedule_group.command(name="del", description="スケジュールIDを指定して削除します")
    async def schedule_del(self, interaction: discord.Interaction, schedule_id: int):
        s = await db.get_schedule_by_id(interaction.guild.id, schedule_id)
        if not s:
            await interaction.response.send_message(f"スケジュールID {schedule_id} は存在しません。", ephemeral=True)
            return

        try:
            await db.delete_schedule(interaction.guild.id, schedule_id)
            await self.reload_schedules()
            await interaction.response.send_message(f"スケジュールID {schedule_id} を削除しました。")
            # Automatically show list after deletion
            await self._send_schedule_list(interaction)
        except Exception as e:
            if not interaction.response.is_done():
                await interaction.response.send_message(f"エラーが発生しました: {e}", ephemeral=True)
            else:
                await interaction.followup.send(f"エラーが発生しました: {e}")

    @schedule_group.command(name="enable", description="スケジュールを有効にします")
    async def schedule_enable(self, interaction: discord.Interaction, schedule_id: int):
        s = await db.get_schedule_by_id(interaction.guild.id, schedule_id)
        if not s:
            await interaction.response.send_message(f"スケジュールID {schedule_id} は存在しません。", ephemeral=True)
            return
        await db.set_schedule_status(interaction.guild.id, schedule_id, 'ENABLED')
        await self.reload_schedules()
        await interaction.response.send_message(f"スケジュールID {schedule_id} を有効にしました。")

    @schedule_group.command(name="disable", description="スケジュールを無効にします")
    async def schedule_disable(self, interaction: discord.Interaction, schedule_id: int):
        s = await db.get_schedule_by_id(interaction.guild.id, schedule_id)
        if not s:
            await interaction.response.send_message(f"スケジュールID {schedule_id} は存在しません。", ephemeral=True)
            return
        await db.set_schedule_status(interaction.guild.id, schedule_id, 'DISABLED')
        await self.reload_schedules()
        await interaction.response.send_message(f"スケジュールID {schedule_id} を無効にしました。")

    @schedule_group.command(name="edit", description="スケジュールIDを指定してスケジュールを変更します")
    async def schedule_edit(self, interaction: discord.Interaction, schedule_id: int):
        s = await db.get_schedule_by_id(interaction.guild.id, schedule_id)
        if not s:
            await interaction.response.send_message(f"スケジュールID {schedule_id} は存在しません。", ephemeral=True)
            return

        current_time = s['schedule_time'].strftime("%H:%M") if hasattr(s['schedule_time'], 'strftime') else str(s['schedule_time'])
        
        p_type = s.get('period_type', 'daily')
        await interaction.response.send_message(f"変更内容を入力してください (ID: {schedule_id})\n現在の設定: `{current_time}` <#{s['channel_id']}> `{p_type} {s['output_type']}`\n形式: `時間 チャンネル 期間 形式` (例: `22:00 here daily graph`)")

        def check(m):
            return m.author.id == interaction.user.id and m.channel.id == interaction.channel.id

        try:
            msg = await self.bot.wait_for('message', check=check, timeout=60.0)
        except asyncio.TimeoutError:
            await interaction.followup.send("タイムアウトしました。")
            return

        time_str, channel_id, period_type, output_type, error = self.parse_schedule_input(msg.content, interaction.channel.id)
        if error:
            await interaction.followup.send(error)
            return

        try:
            await db.update_schedule(interaction.guild.id, schedule_id, time_str, channel_id, period_type, output_type)
            await self.reload_schedules()
            await interaction.followup.send(f"スケジュールID {schedule_id} を更新しました。")
        except Exception as e:
            await interaction.followup.send(f"エラーが発生しました: {e}")

    @schedule_group.command(name="help", description="scheduleコマンドの使い方を表示します")
    async def schedule_help(self, interaction: discord.Interaction):
        msg = """
**schedule コマンドの使い方**
`/schedule show` : 現在登録されているスケジュールの一覧を表示します。
`/schedule add` : 新しいスケジュールを登録します。対話形式で `時間 チャンネル 期間 形式` を入力します。
`/schedule edit schedule_id` : 指定したIDのスケジュールを変更します。
`/schedule enable schedule_id` : スケジュールを有効化します。
`/schedule disable schedule_id` : スケジュールを無効化します。
`/schedule del schedule_id` : 指定したIDのスケジュールを削除します。

**形式について**
`table`: 見やすい表形式で出力
`graph`: 登録ユーザー全員の推移を1つのグラフで出力

**入力形式の例**
`21:00 here daily table` : 毎日21時に、このチャンネルに、日次レポートを送信します。
※実行時に最新のランク情報を取得して表示します。当日分は「(取得時刻時点)」と表示されます。
`09:30 1234567890 weekly graph` : 毎週金曜日の9:30に、指定チャンネルに週次レポートをグラフで表示します。
`08:00 here monthly table` : 毎月1日の8:00に、月次レポートを表示します。
"""
        await interaction.response.send_message(msg)

    @app_commands.command(name="fetch", description="指定したユーザーの現在のランク情報を取得してDBに登録します")
    @app_commands.describe(riot_id="対象ユーザーのRiot ID (例: Name#Tag) または 'all' で全ユーザー")
    async def fetch(self, interaction: discord.Interaction, riot_id: str):
        await interaction.response.defer()
        try:
            if riot_id.lower() == "all":
                results = await self.fetch_all_users_rank(server_id=interaction.guild.id)
                msg_content = f"✅ このサーバーの全ユーザーのランク情報を取得しました: 成功 {results['success']}, 失敗 {results['failed']} (合計 {results['total']})"
                if results['failed'] > 0 and results.get('failed_list'):
                     msg_content += f"\n❌ 失敗したユーザー: {', '.join(results['failed_list'])}"
                await interaction.followup.send(msg_content)
                return

            # Find user in DB
            user = await db.get_user_by_riot_id(interaction.guild.id, riot_id)
            if not user:
                await interaction.followup.send(f"ユーザー `{riot_id}` はこのサーバーに登録されていません。`/user add` で登録してください。")
                return
            
            # Fetch and save current rank
            success = await self.fetch_and_save_rank(user)
            if success:
                # Get the latest rank from DB to display
                today = date.today()
                history = await db.get_rank_history(user['server_id'], user['discord_id'], riot_id, today, today)
                if history:
                    h = history[0]
                    rank_display = rank_calculator.format_rank_display(h['tier'], h['rank'], h['lp'])
                    await interaction.followup.send(f"✅ `{riot_id}` のランク情報を取得しました: **{rank_display}**")
                else:
                    await interaction.followup.send(f"✅ `{riot_id}` のランク情報を取得しましたが、履歴の確認に失敗しました。")
            else:
                await interaction.followup.send(f"❌ `{riot_id}` のランク情報取得に失敗しました。OPGGで見つからないか、エラーが発生しました。")
        except Exception as e:
            logger.error(f"Error in fetch command (Server: {interaction.guild.name}): {e}", exc_info=True)
            await interaction.followup.send(f"実行中にエラーが発生しました: {e}")

    @app_commands.command(name="report", description="指定した期間の集計結果を「表」または「グラフ」で表示します")
    @app_commands.describe(
        period="集計期間 (daily, weekly, monthly)",
        output_type="出力形式 (table, graph)",
        riot_id="特定のユーザーのみを表示する場合に指定 (例: Name#Tag)"
    )
    @app_commands.choices(
        period=[
            app_commands.Choice(name="Daily (7日間/前日比)", value="daily"),
            app_commands.Choice(name="Weekly (2ヶ月/前週比)", value="weekly"),
            app_commands.Choice(name="Monthly (6ヶ月/前月比)", value="monthly"),
        ],
        output_type=[
            app_commands.Choice(name="Table (表形式)", value="table"),
            app_commands.Choice(name="Graph (グラフ形式)", value="graph"),
        ]
    )
    async def report(self, interaction: discord.Interaction, period: str = "daily", output_type: str = "table", riot_id: str = None):
        await interaction.response.defer()
        try:
            # Calculate days and start_date based on period
            today = date.today()
            if period == "daily":
                days = 7
                start_date = today - timedelta(days=7)
            elif period == "weekly":
                days = 60
                start_date = today - timedelta(days=60)
            else:
                days = 180
                start_date = today - timedelta(days=180)

            # --- GRAPH OUTPUT ---
            if output_type == "graph":
                if riot_id:
                    # Single user graph
                    user = await db.get_user_by_riot_id(interaction.guild.id, riot_id)
                    if not user:
                        await interaction.followup.send(f"ユーザー `{riot_id}` は登録されていません。")
                        return
                    rows = await db.get_rank_history_for_graph(interaction.guild.id, user['discord_id'], riot_id, start_date)
                    if not rows:
                        await interaction.followup.send(f"`{riot_id}` のグラフ表示用データがありません。")
                        return
                    buf = generate_rank_graph({riot_id: [dict(r) for r in rows]}, period, f": {riot_id.split('#')[0]}")
                    filename = "rank_graph.png"
                    msg = f"**{riot_id}** のランク推移 ({period})"
                else:
                    # All users graph
                    users = await db.get_users_by_server(interaction.guild.id)
                    if not users:
                        await interaction.followup.send("ユーザーが登録されていません。")
                        return
                    user_data = {}
                    for u in users:
                        rows = await db.get_rank_history_for_graph(interaction.guild.id, u['discord_id'], u['riot_id'], start_date)
                        if rows: user_data[u['riot_id']] = [dict(r) for r in rows]
                    if not user_data:
                        await interaction.followup.send("グラフ表示可能なデータがありません。")
                        return
                    buf = generate_rank_graph(user_data, period, " (全員)")
                    filename = "all_rank_graph.png"
                    msg = f"**全員** のランク推移 ({period})"
                
                if buf:
                    file = discord.File(fp=buf, filename=filename)
                    await interaction.followup.send(msg, file=file)
                else:
                    await interaction.followup.send("グラフの生成に失敗しました。")
                return

            # --- TABLE OUTPUT (Legacy Report) ---
            if riot_id:
                # Individual report (Image)
                user = await db.get_user_by_riot_id(interaction.guild.id, riot_id)
                if not user:
                    await interaction.followup.send(f"ユーザー `{riot_id}` はこのサーバーに登録されていません。")
                    return
                
                # 1. Fetch Data (Async)
                start_date = today - timedelta(days=days)
                history = await db.get_rank_history(user['server_id'], user['discord_id'], user['riot_id'], start_date, today)
                
                # 2. Generate Image (Sync in Thread)
                buf = await asyncio.to_thread(self._generate_single_user_report_impl, user, history, days)

                if buf:
                    file = discord.File(buf, filename=f"report_{riot_id.replace('#', '_')}.png")
                    await interaction.followup.send(file=file)
                else:
                    await interaction.followup.send(f"`{riot_id}` の過去 {days} 日間のデータが見つかりませんでした。")
            else:
                # All users report (Image)
                users = await db.get_users_by_server(interaction.guild.id)
                if not users:
                    await interaction.followup.send("このサーバーに登録されているユーザーがいません。")
                    return

                # 1. Fetch Data (Async)
                start_date = today - timedelta(days=days)
                data_map = {}
                for user in users:
                    h = await db.get_rank_history(user['server_id'], user['discord_id'], user['riot_id'], start_date, today)
                    data_map[user['riot_id']] = {x['fetch_date']: x for x in h}

                # 2. Generate Image (Sync in Thread)
                buf = await asyncio.to_thread(self._generate_report_image_payload_impl, data_map, today, days, period)
                
                if buf:
                    file = discord.File(fp=buf, filename="report.png")
                    await interaction.followup.send(f"**集計レポート ({period})**", file=file)
                else:
                    await interaction.followup.send("レポートの生成に失敗しました。")
        except Exception as e:
            logger.error(f"Error in report command (Server: {interaction.guild.name}): {e}", exc_info=True)
            await interaction.followup.send(f"集計出力中にエラーが発生しました: {e}")

    def parse_schedule_input(self, text: str, current_channel_id: int):
        parts = text.strip().split()
        if len(parts) < 4:
            return None, None, None, None, "入力形式が正しくありません。`時間 チャンネル 期間 形式` の順で入力してください。(例: 21:00 here daily graph)"
        
        t_str = parts[0]
        c_str = parts[1]
        p_str = parts[2].lower()
        o_str = parts[3].lower()

        # Validate Time
        if ':' not in t_str:
             return None, None, None, None, "時間の形式が正しくありません (例: 21:00)"
        
        # Validate Channel
        channel_id = None
        if c_str.lower() == 'here':
            channel_id = current_channel_id
        elif c_str.isdigit():
            channel_id = int(c_str)
        elif c_str.startswith('<#') and c_str.endswith('>'):
            cid_str = c_str[2:-1]
            if cid_str.isdigit():
                channel_id = int(cid_str)
            else:
                 return None, None, None, None, "チャンネルメンションの形式が正しくありません"
        else:
             return None, None, None, None, "チャンネル指定が正しくありません ('here'、ID、またはチャンネル指定)"

        # Validate Period
        period_type = "daily"
        if p_str in ['daily', 'd']:
            period_type = "daily"
        elif p_str in ['weekly', 'w']:
            period_type = "weekly"
        elif p_str in ['monthly', 'm']:
            period_type = "monthly"
        elif p_str.isdigit():
             return None, None, None, None, "期間は `daily`, `weekly`, `monthly` のいずれかを指定してください"
        else:
             return None, None, None, None, "期間は `daily`, `weekly`, `monthly` のいずれかを指定してください"

        # Validate Output Type
        if o_str not in ['table', 'graph']:
            return None, None, None, None, "出力形式は `table` または `graph` を指定してください"

        return t_str, channel_id, period_type, o_str, None

    async def fetch_all_users_rank(self, backfill: bool = False, server_id: int = None):
        """Fetch current rank for all users with concurrent renewal."""
        logger.info(f"Starting concurrent rank collection (backfill={backfill}, server_id={server_id})...")
        today = date.today()
        
        if server_id:
            users = await db.get_users_by_server(server_id)
        else:
            users = await db.get_all_users()
        
        results = {'total': len(users), 'success': 0, 'failed': 0}
        
        # 1. Request Renewal for ALL users concurrently
        logger.info(f"Step 1: Requesting renewal for {len(users)} users...")
        renewal_tasks = []
        for user in users:
            renewal_tasks.append(self.request_renewal_for_user(user))
        
        # Wait for all renewal requests to initiate
        # We process them in parallel to speed up the requests
        renewal_results = await asyncio.gather(*renewal_tasks, return_exceptions=True)
        
        success_users = []
        formatted_failed_users = []

        # Check renewal results
        for i, res in enumerate(renewal_results):
            user = users[i]
            if isinstance(res, Exception):
                logger.error(f"Renewal failed for {user['riot_id']}: {res}")
                formatted_failed_users.append(user['riot_id'])
            elif res:
                success_users.append(user)
            else:
                formatted_failed_users.append(user['riot_id'])

        logger.info(f"Renewal requests sent. Successful: {len(success_users)}, Failed: {len(formatted_failed_users)}")

        # 2. Wait for OPGG to process renewals (10 seconds total)
        if success_users:
            logger.info("Step 2: Waiting 10 seconds for OPGG processing...")
            await asyncio.sleep(10)

        # 3. Fetch data for successful users
        logger.info(f"Step 3: Fetching data for {len(success_users)} users...")
        
        for user in success_users:
            try:
                # Fetch without renewal (already done)
                success = await self.fetch_and_save_rank(user, today, skip_renewal=True)
                if success:
                    results['success'] += 1
                else:
                    results['failed'] += 1
                    formatted_failed_users.append(user['riot_id'])
                
                # Backfill logic (if requested)
                if backfill and success:
                    await self._backfill_user(user, today)

                await asyncio.sleep(1) # Mild rate limit for fetching
            except Exception as e:
                logger.error(f"Failed to fetch data for {user['riot_id']}: {e}")
                results['failed'] += 1
                formatted_failed_users.append(user['riot_id'])

        # Add initial failed users (renewal failure) to result count
        # results['failed'] already contains fetch failures from loop above
        results['failed'] += (len(users) - len(success_users))
        results['failed_list'] = list(set(formatted_failed_users)) # unique list

        logger.info(f"Global rank collection completed: {results}")
        return results

    async def request_renewal_for_user(self, user):
        """Helper to valid user and request renewal."""
        riot_id = user['riot_id']
        if '#' not in riot_id: return False
        name, tag = riot_id.split('#', 1)
        
        try:
            summoner = await opgg_client.get_summoner(name, tag, Region.JP)
            if not summoner: return False
            return await opgg_client.renew_summoner(summoner)
        except Exception as e:
            logger.error(f"Error requesting renewal for {riot_id}: {e}")
            return False

    async def _backfill_user(self, user, today):
        """Helper for backfilling history."""
        try:
            uid = user['discord_id']
            rid = user['riot_id']
            if '#' not in rid: return
            name, tag = rid.split('#')
            summoner = await opgg_client.get_summoner(name, tag, Region.JP)
            if summoner:
                history = await opgg_client.get_tier_history(summoner.summoner_id, Region.JP)
                for entry in history:
                    h_date = entry['updated_at'].date()
                    if h_date < today:
                        await db.add_rank_history(
                            user['server_id'], uid, rid, 
                            entry['tier'], entry['rank'], entry['lp'],
                            0, 0, h_date
                        )
        except Exception as e:
            logger.error(f"Backfill error for {rid}: {e}")

    async def run_daily_report(self, server_id: int, channel_id: int, period_type: str, output_type: str = 'table'):
        guild = self.bot.get_guild(server_id)
        guild_name = guild.name if guild else "Unknown"
        logger.info(f"Running report for server '{guild_name}' (ID: {server_id}), channel {channel_id} (type: {output_type}, period: {period_type})")
        
        # Derive days from period_type
        period_days = 7
        if period_type == 'weekly':
            period_days = 60
        elif period_type == 'monthly':
            period_days = 180

        channel = self.bot.get_channel(channel_id)
        if not channel:
            logger.warning(f"Channel {channel_id} not found.")
            return

        users = await db.get_users_by_server(server_id)
        if not users:
            logger.info(f"No users in server {server_id} for report.")
            return

        # 1. Fetch latest data for all users before generating report
        await self.fetch_all_users_rank(server_id=server_id)

        today = date.today()
        
        try:
            if output_type == 'graph':
                # Generate multi-user graph
                start_date = today - timedelta(days=period_days)
                user_data = {}
                for u in users:
                    rows = await db.get_rank_history_for_graph(u['server_id'], u['discord_id'], u['riot_id'], start_date)
                    if rows:
                        user_data[u['riot_id']] = [dict(r) for r in rows]
                
                if not user_data:
                    await channel.send(f"過去 {period_days} 日間のグラフデータがありません。")
                    return

                # Run graph generation in thread
                buf = await asyncio.to_thread(generate_rank_graph, user_data, period_type, " (定例レポート)")
                if buf:
                    file = discord.File(fp=buf, filename="scheduled_graph.png")
                    await channel.send(content=f"**定期レポート ({period_type})**", file=file)
                else:
                    await channel.send("グラフの生成に失敗しました。")
            else:
                # Image-based Table output
                # 1. Fetch Data (Async)
                start_date = today - timedelta(days=period_days)
                data_map = {}
                for user in users:
                    h = await db.get_rank_history(user['server_id'], user['discord_id'], user['riot_id'], start_date, today)
                    data_map[user['riot_id']] = {x['fetch_date']: x for x in h}

                # 2. Generate Image (Sync in Thread)
                buf = await asyncio.to_thread(self._generate_report_image_payload_impl, data_map, today, period_days, period_type)

                if buf:
                    file = discord.File(fp=buf, filename="scheduled_report.png")
                    await channel.send(content=f"**定期レポート ({period_type})**", file=file)
                else:
                    await channel.send("レポートの生成に失敗しました。")

        except Exception as e:
            await channel.send(f"レポート生成中にエラーが発生しました: {e}")
            logger.error(f"Error in scheduled report: {e}", exc_info=True)

    async def fetch_and_save_rank(self, user, target_date=None, skip_renewal=False):
        if target_date is None:
            target_date = date.today()

        discord_id = user['discord_id']
        riot_id = user['riot_id'] # Expected "Name#Tag"
        if '#' not in riot_id:
            return False

        name, tag = riot_id.split('#', 1)
        
        # Get Summoner
        try:
            logger.info(f"Fetching rank for {riot_id} on {target_date} (Server: {user.get('server_id', 'Unknown')}, SkipRenewal: {skip_renewal})")
            summoner = await opgg_client.get_summoner(name, tag, Region.JP)
            if not summoner:
                logger.warning(f"User not found on OPGG: {riot_id}")
                return False
                
            if not skip_renewal:
                # Trigger renewal to ensure data is fresh
                await opgg_client.renew_summoner(summoner)
                # Renewal can take some time on OP.GG side. Increased sleep to 8s for reliability.
                await asyncio.sleep(8)

            # Get Rank
            tier, rank, lp, wins, losses = await opgg_client.get_rank_info(summoner)
            logger.info(f"Rank info for {riot_id}: {tier} {rank} {lp}LP (W:{wins} L:{losses})")
            await db.add_rank_history(user['server_id'], discord_id, riot_id, tier, rank, lp, wins, losses, target_date)
            return True
        except Exception as e:
            logger.error(f"Error in fetch_and_save_rank for {riot_id}: {e}", exc_info=True)
            return False


    def _generate_single_user_report_impl(self, user, history, period_days: int) -> io.BytesIO:
        """Generate vertical image report for a single user (Sync implementation)."""
        rid = user['riot_id']
        
        if not history:
            return None

        # Prepare rows
        header = ["日付", "ランク", "前日比", "戦績"]
        table_rows = []
        # Sort history by date descending
        history.sort(key=lambda x: x['fetch_date'], reverse=True)
        
        for i, h in enumerate(history):
            d_str = h['fetch_date'].strftime("%m/%d")
            r_str = rank_calculator.format_rank_display(h['tier'], h['rank'], h['lp'])
            diff_str = "-"
            record_str = "-"
            if i + 1 < len(history):
                prev_h = history[i+1]
                diff_str = rank_calculator.calculate_diff_text(prev_h, h, include_prefix=False)
                w = h['wins'] - prev_h['wins']
                l = h['losses'] - prev_h['losses']
                g = w + l
                if g > 0:
                    rate = int((w / g) * 100)
                    record_str = f"{g}戦{w}勝({rate}%)"
            table_rows.append([d_str, r_str, diff_str, record_str])

        return generate_report_image(header, table_rows, f"{rid} Report (Last {period_days} Days)")

    def _generate_report_image_payload_impl(self, data_map, today: date, period_days: int, period_type: str = 'daily') -> io.BytesIO:
        """Generate table image for all users (Sync implementation)."""
        all_dates = set()
        
        for user_history in data_map.values():
            all_dates.update(user_history.keys())
            
        sorted_dates = sorted(list(all_dates))
        if not sorted_dates:
            return None

        # Filter dates based on period_type
        filtered_dates = []
        if period_type == 'weekly':
            # Group by ISO Year/Week, pick latest date in each week
            weeks = {}
            for d in sorted_dates:
                year, week, _ = d.isocalendar()
                key = (year, week)
                weeks[key] = d # Will always be the latest due to sorted order
            filtered_dates = sorted(weeks.values())
        elif period_type == 'monthly':
            # Group by Year/Month, pick latest date in each month
            months = {}
            for d in sorted_dates:
                key = (d.year, d.month)
                months[key] = d # Will always be the latest due to sorted order
            filtered_dates = sorted(months.values())
        else:
            filtered_dates = sorted_dates

        # Headers: Riot ID, Recent Dates, Diff 1, Diff 2 (Period)
        MAX_DATES_IN_IMAGE = 5
        shown_dates = filtered_dates[-MAX_DATES_IN_IMAGE:]
        
        # Comparison logic
        # 1. "Recent Diff": 
        #    Daily -> vs Yesterday (1 day ago)
        #    Weekly -> vs Last Week (7 days ago)
        #    Monthly -> vs Last Month (30 days ago)
        diff_label = "前日比"
        diff_days = 1
        if period_type == 'weekly':
            diff_label = "前週比"
            diff_days = 7
        elif period_type == 'monthly':
            diff_label = "前月比"
            diff_days = 30
        
        # 2. "Period Diff":
        #    Daily -> 7-day diff (start of period)
        #    Weekly -> 60-day diff
        #    Monthly -> 180-day diff
        period_label = "期間比"
        if period_type == 'daily': period_label = "7日比"
        elif period_type == 'weekly': period_label = "2ヶ月比"
        elif period_type == 'monthly': period_label = "半年比"
        
        def get_today_time_suffix(d):
            if d != today:
                return ""
            for hm in data_map.values():
                entry = hm.get(d)
                if entry and 'reg_date' in entry:
                    f_time = entry['reg_date']
                    return f"({f_time.hour}:{f_time.minute:02d}時点)"
            return "(現在)"

        # Date headers based on period_type
        if period_type == 'weekly':
            date_headers = []
            for d in shown_dates:
                week_num = (d.day - 1) // 7 + 1
                label = f"{d.month}月/{week_num}週目"
                date_headers.append(label + get_today_time_suffix(d))
        elif period_type == 'monthly':
            date_headers = []
            for d in shown_dates:
                label = f"{d.month}月"
                date_headers.append(label + get_today_time_suffix(d))
        else:
            date_headers = []
            for d in shown_dates:
                label = d.strftime("%m/%d")
                date_headers.append(label + get_today_time_suffix(d))
            
        headers = ["RIOT ID"] + date_headers + [diff_label, period_label]
        table_data = []
        for rid, h_map in data_map.items():
            row = [rid.split('#')[0]]
            
            # Rank for each date
            for d in shown_dates:
                entry = h_map.get(d)
                row.append(rank_calculator.format_rank_display(entry['tier'], entry['rank'], entry['lp']) if entry else "-")
            
            # Diff 1 (vs Pre-defined offset)
            anchor_date = sorted_dates[-1]
            anchor_entry = h_map.get(anchor_date)
            
            # Find closest entry on or before target date
            diff1_date = anchor_date - timedelta(days=diff_days)
            
            def get_entry_near(target_d):
                candidates = [d for d in h_map.keys() if d <= target_d]
                if not candidates: return None
                best = max(candidates)
                return h_map[best]

            diff1_entry = get_entry_near(diff1_date)
            diff1_text = "-"
            if diff1_entry and anchor_entry:
                diff1_text = rank_calculator.calculate_diff_text(diff1_entry, anchor_entry, include_prefix=True)
            row.append(diff1_text)
            
            # Diff 2 (Period start)
            start_entry = get_entry_near(sorted_dates[0])
            period_diff_text = "-"
            if start_entry and anchor_entry:
                period_diff_text = rank_calculator.calculate_diff_text(start_entry, anchor_entry, include_prefix=True)
            row.append(period_diff_text)
            
            table_data.append(row)

        return generate_report_image(headers, table_data, f"Rank Report ({period_type.capitalize()})")

async def setup(bot):
    await bot.add_cog(Scheduler(bot))
