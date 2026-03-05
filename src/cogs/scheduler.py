import discord
from discord import app_commands
from discord.ext import commands
from apscheduler.schedulers.asyncio import AsyncIOScheduler
from src.database import db
from src.utils import rank_calculator
from src.utils.opgg_client import opgg_client
from src.utils.opgg_compat import Region
from src.utils.graph_generator import generate_rank_graph, generate_report_image
from src.utils.graph_utils import split_user_data_by_rank
from datetime import date, timedelta
from src.utils.time_utils import get_now_jst, get_today_jst
import asyncio
import io
import logging

logger = logging.getLogger(__name__)

class Scheduler(commands.Cog):
    def __init__(self, bot):
        self.bot = bot
        self.scheduler = AsyncIOScheduler(timezone='Asia/Tokyo')
        self.scheduler.start()

    async def cog_load(self):
        await self.reload_schedules()

    async def reload_schedules(self):
        self.scheduler.remove_all_jobs()
        
        # 1. System-wide Rank Collection Job (Daily 23:55)
        self.scheduler.add_job(
            self.fetch_all_users_rank,
            'cron',
            hour=23,
            minute=55,
            second=0,
            args=[False, None, True], # is_final=True
            name="daily_rank_fetch"
        )

        # 2. Group User-defined Reporting Jobs by Time
        schedules = await db.get_all_schedules()
        time_groups = {} # (hour, minute, second) -> [schedules]

        for s in schedules:
            if s['status'] != 'ENABLED':
                continue
            
            sched_time = s['schedule_time']
            time_key = (sched_time.hour, sched_time.minute, sched_time.second)
            
            if time_key not in time_groups:
                time_groups[time_key] = []
            time_groups[time_key].append(s)

        # Register one job per time slot
        for (h, m, sec), group in time_groups.items():
            self.scheduler.add_job(
                self.run_composite_report,
                'cron',
                hour=h,
                minute=m,
                second=sec,
                args=[group],
                name=f"report_at_{h:02d}{m:02d}{sec:02d}"
            )

        logger.info(f"Loaded {len(schedules)} schedules into {len(time_groups)} time-based jobs.")


    # Schedule Command Group
    schedule_group = app_commands.Group(name="schedule", description="定期実行スケジュールを管理します")

    async def _send_schedule_list(self, interaction: discord.Interaction):
        schedules = await db.get_schedules_by_server(interaction.guild.id)
        if not schedules:
            msg = "このサーバーに登録されているスケジュールはありません。"
            if interaction.response.is_done():
                await interaction.followup.send(msg)
            else:
                await interaction.response.send_message(msg)
            return

        tables = [s for s in schedules if s['output_type'] == 'table']
        graphs = [s for s in schedules if s['output_type'] == 'graph']

        msg = "**登録スケジュール一覧**\n"
        
        if tables:
            msg += "\n■表形式\n"
            for s in tables:
                t = s['schedule_time']
                t_str = t.strftime("%H:%M") if hasattr(t, 'strftime') else str(t)
                status_emoji = "✅" if s['status'] == 'ENABLED' else "❌"
                l_id = s.get('local_id', '-')
                p_display = "日時"
                if s['period_type'] == 'weekly': p_display = "週次"
                elif s['period_type'] == 'monthly': p_display = "月次"
                msg += f"{status_emoji} ID: {l_id} | 時間: {t_str} | Ch: <#{s['channel_id']}> | 期間: {p_display}\n"

        if graphs:
            msg += "\n■グラフ形式\n"
            for s in graphs:
                t = s['schedule_time']
                t_str = t.strftime("%H:%M") if hasattr(t, 'strftime') else str(t)
                status_emoji = "✅" if s['status'] == 'ENABLED' else "❌"
                l_id = s.get('local_id', '-')
                p_display = "日時"
                if s['period_type'] == 'weekly': p_display = "週次"
                elif s['period_type'] == 'monthly': p_display = "月次"
                s_display = "分割" if s.get('split', True) else "全体"
                msg += f"{status_emoji} ID: {l_id} | 時間: {t_str} | Ch: <#{s['channel_id']}> | 期間: {p_display} | 表示形式: {s_display}\n"
        
        if interaction.response.is_done():
            await interaction.followup.send(msg)
        else:
            await interaction.response.send_message(msg)

    @schedule_group.command(name="show", description="現在登録されているスケジュールの一覧を表示します")
    async def schedule_show(self, interaction: discord.Interaction):
        await self._send_schedule_list(interaction)

    @schedule_group.command(name="add", description="スケジュールを登録します")
    @app_commands.describe(
        output_type="出力形式 (table, graph)",
        time="時間 (HH:MM 例: 21:00)",
        channel="送信先チャンネル ('here' または ID)",
        period="実行周期 (daily, weekly, monthly)",
        split="グラフ分割 (True: 自動分割, False: 全員1枚) ※graph形式のみ"
    )
    @app_commands.choices(
        output_type=[
            app_commands.Choice(name="Table (表形式)", value="table"),
            app_commands.Choice(name="Graph (グラフ形式)", value="graph"),
        ],
        period=[
            app_commands.Choice(name="Daily (毎日)", value="daily"),
            app_commands.Choice(name="Weekly (毎週金曜)", value="weekly"),
            app_commands.Choice(name="Monthly (毎月1日)", value="monthly"),
        ]
    )
    async def schedule_add(self, interaction: discord.Interaction, output_type: str, time: str, channel: str = "here", period: str = "daily", split: bool = True):
        await interaction.response.defer()
        
        # Warning for table + split
        warning_msg = ""
        if output_type == "table" and split is not True:
             warning_msg = "\n⚠️ **注意**: 表形式(table)では分割設定は適用されないため、設定項目は無視されました。"

        # Parse channel
        channel_id = None
        if channel.lower() == 'here':
            channel_id = interaction.channel.id
        elif channel.isdigit():
            channel_id = int(channel)
        else:
             await interaction.followup.send("チャンネル指定が正しくありません ('here' または IDを指定してください)")
             return

        try:
            await db.register_schedule(interaction.guild.id, time, channel_id, interaction.user.id, period, output_type, split)
            await self.reload_schedules()
            
            p_msg = "毎日"
            if period == 'weekly': p_msg = "毎週金曜日"
            elif period == 'monthly': p_msg = "毎月1日"
            
            s_msg = " (分割)" if (output_type == 'graph' and split) else ""
            await interaction.followup.send(f"スケジュール登録完了: {time} にチャンネル <#{channel_id}> へ通知 ({p_msg}, 形式: {output_type}{s_msg}){warning_msg}")
        except Exception as e:
            await interaction.followup.send(f"エラーが発生しました: {e}")

    @schedule_group.command(name="del", description="スケジュールIDを指定して削除します")
    @app_commands.describe(output_type="削除対象の形式 (table, graph)", schedule_id="スケジュールID")
    @app_commands.choices(output_type=[
        app_commands.Choice(name="Table (表形式)", value="table"),
        app_commands.Choice(name="Graph (グラフ形式)", value="graph"),
    ])
    async def schedule_del(self, interaction: discord.Interaction, output_type: str, schedule_id: int):
        s = await db.get_schedule_by_id(interaction.guild.id, schedule_id, output_type)
        if not s:
            await interaction.response.send_message(f"{output_type}のスケジュールID {schedule_id} は存在しません。", ephemeral=True)
            return

        try:
            await db.delete_schedule(interaction.guild.id, schedule_id, output_type)
            await self.reload_schedules()
            await interaction.response.send_message(f"{output_type}のスケジュールID {schedule_id} を削除しました。")
            await self._send_schedule_list(interaction)
        except Exception as e:
            await interaction.followup.send(f"エラーが発生しました: {e}")

    @schedule_group.command(name="enable", description="スケジュールを有効にします")
    @app_commands.describe(output_type="形式 (table, graph)", schedule_id="スケジュールID")
    @app_commands.choices(output_type=[
        app_commands.Choice(name="Table (表形式)", value="table"),
        app_commands.Choice(name="Graph (グラフ形式)", value="graph"),
    ])
    async def schedule_enable(self, interaction: discord.Interaction, output_type: str, schedule_id: int):
        s = await db.get_schedule_by_id(interaction.guild.id, schedule_id, output_type)
        if not s:
            await interaction.response.send_message(f"{output_type}のスケジュールID {schedule_id} は存在しません。", ephemeral=True)
            return
        await db.set_schedule_status(interaction.guild.id, schedule_id, output_type, 'ENABLED')
        await self.reload_schedules()
        await interaction.response.send_message(f"{output_type}のスケジュールID {schedule_id} を有効にしました。")

    @schedule_group.command(name="disable", description="スケジュールを無効にします")
    @app_commands.describe(output_type="形式 (table, graph)", schedule_id="スケジュールID")
    @app_commands.choices(output_type=[
        app_commands.Choice(name="Table (表形式)", value="table"),
        app_commands.Choice(name="Graph (グラフ形式)", value="graph"),
    ])
    async def schedule_disable(self, interaction: discord.Interaction, output_type: str, schedule_id: int):
        s = await db.get_schedule_by_id(interaction.guild.id, schedule_id, output_type)
        if not s:
            await interaction.response.send_message(f"{output_type}のスケジュールID {schedule_id} は存在しません。", ephemeral=True)
            return
        await db.set_schedule_status(interaction.guild.id, schedule_id, output_type, 'DISABLED')
        await self.reload_schedules()
        await interaction.response.send_message(f"{output_type}のスケジュールID {schedule_id} を無効にしました。")

    @schedule_group.command(name="edit", description="指定したIDのスケジュールを変更します")
    @app_commands.describe(
        output_type="変更対象の形式 (table, graph)",
        schedule_id="スケジュールID",
        time="新しい時間 (HH:MM 例: 22:00)",
        channel="新しい送信先 ('here' または ID)",
        period="新しい実行周期 (daily, weekly, monthly)",
        split="グラフ分割 (True: 自動分割, False: 全員1枚) ※graph形式のみ"
    )
    @app_commands.choices(
        output_type=[
            app_commands.Choice(name="Table (表形式)", value="table"),
            app_commands.Choice(name="Graph (グラフ形式)", value="graph"),
        ],
        period=[
            app_commands.Choice(name="Daily (毎日)", value="daily"),
            app_commands.Choice(name="Weekly (毎週金曜)", value="weekly"),
            app_commands.Choice(name="Monthly (毎月1日)", value="monthly"),
        ]
    )
    async def schedule_edit(self, interaction: discord.Interaction, output_type: str, schedule_id: int, time: str = None, channel: str = None, period: str = None, split: bool = None):
        await interaction.response.defer()
        
        s = await db.get_schedule_by_id(interaction.guild.id, schedule_id, output_type)
        if not s:
            await interaction.followup.send(f"{output_type}のスケジュールID {schedule_id} は存在しません。")
            return

        # Fallbacks to current values
        new_time = time or s['schedule_time'].strftime("%H:%M")
        new_period = period or s['period_type']
        new_split = split if split is not None else s.get('split', True)
        
        new_channel_id = s['channel_id']
        if channel:
            if channel.lower() == 'here':
                new_channel_id = interaction.channel.id
            elif channel.isdigit():
                new_channel_id = int(channel)
            else:
                await interaction.followup.send("チャンネル指定が正しくありません。")
                return

        # Warning for table + split
        warning_msg = ""
        if output_type == "table" and split is not None:
             warning_msg = "\n⚠️ **注意**: 表形式(table)では分割設定は適用されないため、設定項目は無視されました。"

        try:
            await db.update_schedule(interaction.guild.id, schedule_id, output_type, new_time, new_channel_id, new_period, new_split)
            await self.reload_schedules()
            await interaction.followup.send(f"{output_type}のスケジュールID {schedule_id} を更新しました。{warning_msg}")
        except Exception as e:
            await interaction.followup.send(f"エラーが発生しました: {e}")

    @schedule_group.command(name="help", description="scheduleコマンドの使い方を表示します")
    async def schedule_help(self, interaction: discord.Interaction):
        msg = """
**schedule コマンドの使い方**
`/schedule show` : 現在登録されているスケジュールの一覧を表示します。
`/schedule add output_type time [channel] [period] [split]` : 新しいスケジュールを登録します。
`/schedule edit output_type schedule_id [time] [channel] [period] [split]` : 指定したIDのスケジュールを変更します。
`/schedule enable output_type schedule_id` : スケジュールを有効化します。
`/schedule disable output_type schedule_id` : スケジュールを無効化します。
`/schedule del output_type schedule_id` : 指定したIDのスケジュールを削除します。

**output_type (形式について)**
`table`: 見やすい表形式で出力（schedules_table に保存）
`graph`: 登録ユーザー全員の推移をグラフで出力（schedules_graph に保存）

**分割オプション (graph形式のみ)**
`True` (デフォルト): ランク帯に応じて自動的にグラフを分割して出力
`False`: 全員を1枚のグラフにまとめて出力
※ `table` 形式では分割オプションは使用できません。

**使用例**
`/schedule add output_type:Table time:21:00` : 毎日21時に日次レポートを送信
`/schedule add output_type:Graph time:21:00 split:False` : 毎日21時に全員を1枚にまとめたグラフを送信
`/schedule edit output_type:Graph schedule_id:1 time:22:00` : graphスケジュールID 1の時間を22:00に変更
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
                today = get_today_jst()
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
        split="グラフ分割 (True: 自動分割, False: 全員1枚) ※graph形式のみ",
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
    async def report(self, interaction: discord.Interaction, period: str = "daily", output_type: str = "table", split: bool = None, riot_id: str = None):
        await interaction.response.defer()
        
        # Validation for table + split
        if output_type == "table" and split is not None:
            await interaction.followup.send("表形式(table)が指定された場合、分割オプション(split)は指定できません。")
            return
            
        # Default split behavior
        if split is None:
            split = True

        try:
            # Calculate days and start_date based on period
            today = get_today_jst()
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
                    rows = await db.get_rank_history(interaction.guild.id, user['discord_id'], riot_id, start_date, today)
                    logger.info(f"Graph single user: {riot_id} returned {len(rows)} rows (start={start_date}, end={today})")
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
                        rows = await db.get_rank_history(interaction.guild.id, u['discord_id'], u['riot_id'], start_date, today)
                        logger.info(f"Graph all users: {u['riot_id']} returned {len(rows)} rows (start={start_date}, end={today})")
                        if rows: user_data[u['riot_id']] = [dict(r) for r in rows]
                    
                    if not user_data:
                        await interaction.followup.send("グラフ表示可能なデータがありません。")
                        return

                    # Split data into rank-based groups for readability if requested
                    if split:
                        groups = split_user_data_by_rank(user_data)
                    else:
                        groups = [user_data]
                    
                    for i, group_data in enumerate(groups):
                        title_suffix = " (全員)" if len(groups) == 1 else f" (全員 - その{i+1})"
                        buf = await asyncio.to_thread(generate_rank_graph, group_data, period, title_suffix)
                        if buf:
                            filename = f"all_rank_graph_{i+1}.png"
                            msg = f"**全員** のランク推移 ({period})" + (f" [{i+1}/{len(groups)}]" if len(groups) > 1 else "")
                            file = discord.File(fp=buf, filename=filename)
                            await interaction.followup.send(msg, file=file)
                        else:
                            await interaction.followup.send(f"グラフ {i+1} の生成に失敗しました。")
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
                    # Fetch Discord name
                    member = interaction.guild.get_member(user['discord_id'])
                    discord_name = member.display_name if member else str(user['discord_id'])
                    data_map[user['riot_id']] = {
                        'history': {x['fetch_date']: x for x in h},
                        'discord_name': discord_name
                    }

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


    async def fetch_all_users_rank(self, backfill: bool = False, server_id: int = None, is_final: bool = False):
        """Fetch current rank for all users with concurrent renewal."""
        logger.info(f"Starting concurrent rank collection (backfill={backfill}, server_id={server_id}, is_final={is_final})...")
        today = get_today_jst()
        
        if server_id:
            users = await db.get_users_by_server(server_id)
        else:
            users = await db.get_all_users()
        
        results = {'total': len(users), 'success': 0, 'failed': 0, 'failed_users': []}
        
        # 1. Request Renewal for ALL users concurrently
        logger.info(f"Step 1: Requesting renewal for {len(users)} users...")
        renewal_tasks = []
        for user in users:
            renewal_tasks.append(self.request_renewal_for_user(user))
        
        # Wait for all renewal requests to initiate
        # We process them in parallel to speed up the requests
        renewal_results = await asyncio.gather(*renewal_tasks, return_exceptions=True)
        
        success_users = []
        failed_users = []

        # Check renewal results
        for i, res in enumerate(renewal_results):
            user = users[i]
            if isinstance(res, Exception):
                logger.error(f"Renewal failed for {user['riot_id']}: {res}")
                failed_users.append(user)
            elif res:
                success_users.append(user)
            else:
                failed_users.append(user)

        logger.info(f"Renewal requests sent. Successful: {len(success_users)}, Failed: {len(failed_users)}")

        # 2. Wait for OPGG to process renewals (10 seconds total)
        if success_users:
            logger.info("Step 2: Waiting 10 seconds for OPGG processing...")
            await asyncio.sleep(10)

        # 3. Fetch data for successful users
        logger.info(f"Step 3: Fetching data for {len(success_users)} users...")
        
        for user in success_users:
            try:
                # Fetch without renewal (already done)
                success = await self.fetch_and_save_rank(user, today, skip_renewal=True, is_final=is_final)
                if success:
                    results['success'] += 1
                else:
                    failed_users.append(user)
                
                # Backfill logic (if requested)
                if backfill and success:
                    await self._backfill_user(user, today)

                await asyncio.sleep(1) # Mild rate limit for fetching
            except Exception as e:
                logger.error(f"Failed to fetch data for {user['riot_id']}: {e}")
                failed_users.append(user)

        # Calculate failed count: total - success (avoids double-counting)
        results['failed'] = results['total'] - results['success']
        results['failed_users'] = failed_users

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

    async def run_composite_report(self, schedules: list):
        """Execute multiple reports sequentially with priority sorting."""
        today = get_today_jst()
        
        # 1. Filter schedules that should run today
        to_run = []
        for s in schedules:
            p_type = s.get('period_type', 'daily')
            if p_type == 'daily':
                to_run.append(s)
            elif p_type == 'weekly' and today.weekday() == 4: # Friday
                to_run.append(s)
            elif p_type == 'monthly' and today.day == 1: # 1st of month
                to_run.append(s)

        if not to_run:
            return

        # 2. Priority Sorting
        # Priority order:
        # Table (Daily) -> Graph (Daily) -> Table (Weekly) -> Graph (Weekly) -> Table (Monthly) -> Graph (Monthly)
        def sort_key(s):
            p_order = {'daily': 0, 'weekly': 1, 'monthly': 2}
            o_order = {'table': 0, 'graph': 1}
            return (p_order.get(s.get('period_type', 'daily'), 0), o_order.get(s['output_type'], 0))
        
        to_run.sort(key=sort_key)

        # 3. Pre-fetch rank data for all relevant servers once
        server_ids = set(s['server_id'] or 0 for s in to_run)
        fetching_results = {} # server_id -> results
        for sid in server_ids:
            if sid != 0:
                fetching_results[sid] = await self.fetch_all_users_rank(server_id=sid, is_final=False)

        # 4. Sequential execution
        target_channels = {} # server_id -> set(channel_id)
        for s in to_run:
            sid = s['server_id'] or 0
            if sid not in target_channels:
                target_channels[sid] = set()
            target_channels[sid].add(s['channel_id'])

            try:
                await self.run_daily_report(
                    server_id=sid,
                    channel_id=s['channel_id'],
                    period_type=s.get('period_type', 'daily'),
                    output_type=s['output_type'],
                    split=s.get('split', True),
                    skip_fetch=True
                )
                
                # Small delay between reports to ensure Discord order and prevent rate limits
                await asyncio.sleep(2)
            except Exception as e:
                logger.error(f"Error in composite report for schedule {s.get('id')}: {e}")

        # 5. Report missing users once per channel after all images (Scheduled results)
        for sid, channels in target_channels.items():
            res = fetching_results.get(sid)
            if res and res.get('failed_users'):
                failed_msg = "以下のユーザーのランク情報を取得できませんでした。Riot ID が変更されたか、アカウントが削除された可能性があります。\n"
                for fu in res['failed_users']:
                    failed_msg += f"- {fu['riot_id']} (登録ID: {fu['local_id']})\n"
                failed_msg += "\n古い登録を削除するには下記のコマンドを使用してください：\n"
                failed_msg += "`/user del user_id:[登録ID]`"
                
                for cid in channels:
                    channel = self.bot.get_channel(cid)
                    if channel:
                        await channel.send(failed_msg)

    async def run_daily_report(self, server_id: int, channel_id: int, period_type: str, output_type: str = 'table', split: bool = True, skip_fetch: bool = False):
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
        if not skip_fetch:
            await self.fetch_all_users_rank(server_id=server_id)

        today = get_today_jst()
        
        try:
            if output_type == 'graph':
                # Generate multi-user graph
                start_date = today - timedelta(days=period_days)
                user_data = {}
                for u in users:
                    rows = await db.get_rank_history(u['server_id'], u['discord_id'], u['riot_id'], start_date, today)
                    logger.info(f"Scheduled graph: {u['riot_id']} returned {len(rows)} rows (start={start_date}, end={today})")
                    if rows:
                        user_data[u['riot_id']] = [dict(r) for r in rows]
                
                if not user_data:
                    await channel.send(f"過去 {period_days} 日間のグラフデータがありません。")
                    return

                # Split data into rank-based groups for readability if requested
                if split:
                    groups = split_user_data_by_rank(user_data)
                else:
                    groups = [user_data]
                
                for i, group_data in enumerate(groups):
                    title_suffix = " (定例レポート)" if len(groups) == 1 else f" (定例レポート - その{i+1})"
                    # Run graph generation in thread
                    buf = await asyncio.to_thread(generate_rank_graph, group_data, period_type, title_suffix)
                    if buf:
                        filename = f"scheduled_graph_{i+1}.png"
                        title_date = today.strftime("%m/%d")
                        msg_prefix = f"**定期レポート {title_date} ({period_type})**" + (f" [{i+1}/{len(groups)}]" if len(groups) > 1 else "")
                        file = discord.File(fp=buf, filename=filename)
                        await channel.send(content=msg_prefix, file=file)
                    else:
                        await channel.send(f"グラフ {i+1} の生成に失敗しました。")
            else:
                # Image-based Table output
                # 1. Fetch Data (Async)
                start_date = today - timedelta(days=period_days)
                data_map = {}
                for user in users:
                    h = await db.get_rank_history(user['server_id'], user['discord_id'], user['riot_id'], start_date, today)
                    # Fetch Discord name
                    member = channel.guild.get_member(user['discord_id'])
                    discord_name = member.display_name if member else str(user['discord_id'])
                    data_map[user['riot_id']] = {
                        'history': {x['fetch_date']: x for x in h},
                        'discord_name': discord_name
                    }

                # 2. Generate Image (Sync in Thread)
                buf = await asyncio.to_thread(self._generate_report_image_payload_impl, data_map, today, period_days, period_type)

                if buf:
                    file = discord.File(fp=buf, filename="scheduled_report.png")
                    title_date = today.strftime("%m/%d")
                    await channel.send(content=f"**定期レポート {title_date} ({period_type})**", file=file)
                else:
                    await channel.send("レポートの生成に失敗しました。")

        except Exception as e:
            await channel.send(f"レポート生成中にエラーが発生しました: {e}")
            logger.error(f"Error in scheduled report: {e}", exc_info=True)

    async def fetch_and_save_rank(self, user, target_date=None, skip_renewal=False, is_final: bool = False):
        if target_date is None:
            target_date = get_today_jst()

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
            
            if is_final:
                await db.add_end_rank_history(user['server_id'], discord_id, riot_id, tier, rank, lp, wins, losses, target_date, reg_date=get_now_jst())
            else:
                await db.add_rank_history(user['server_id'], discord_id, riot_id, tier, rank, lp, wins, losses, target_date, reg_date=get_now_jst())
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
        
        for entry in data_map.values():
            user_history = entry['history']
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

        # Headers: Player, Recent Dates, Diff 1, Diff 2 (Period)
        MAX_DATES_IN_IMAGE = 7
        shown_dates = filtered_dates[-MAX_DATES_IN_IMAGE:]
        
        # 1. "Recent Diff": 
        #    Daily -> vs Yesterday (1 day ago)
        #    Weekly -> vs Last Week (7 days ago)
        #    Monthly -> vs Last Month (30 days ago)
        diff_label = "前回取得比"
        diff_days = 1
        if period_type == 'weekly':
            diff_label = "前回取得比"
            diff_days = 7
        elif period_type == 'monthly':
            diff_label = "前回取得比"
            diff_days = 30
        
        # 2. "Period Diff":
        #    Comparison across the shown period (MAX_DATES_IN_IMAGE=7)
        period_label = "期間比"
        
        def get_date_time_suffix(d, is_recent=False):
            if not is_recent:
                return ""
            
            latest_time = None
            for entry in data_map.values():
                hm = entry['history']
                data_entry = hm.get(d)
                if data_entry and 'reg_date' in data_entry:
                    if latest_time is None or data_entry['reg_date'] > latest_time:
                        latest_time = data_entry['reg_date']
            
            if latest_time:
                return f"({latest_time.hour:02d}:{latest_time.minute:02d})"
            return ""

        # Date headers based on period_type
        if period_type == 'weekly':
            date_headers = []
            for d in shown_dates:
                week_num = (d.day - 1) // 7 + 1
                label = f"{d.month}月/{week_num}週目"
                date_headers.append(label + get_date_time_suffix(d, is_recent=(d in shown_dates[-2:])))
        elif period_type == 'monthly':
            date_headers = []
            for d in shown_dates:
                label = f"{d.month}月"
                date_headers.append(label + get_date_time_suffix(d, is_recent=(d in shown_dates[-2:])))
        else:
            date_headers = []
            for d in shown_dates:
                label = d.strftime("%m/%d")
                date_headers.append(label + get_date_time_suffix(d, is_recent=(d in shown_dates[-2:])))
            
        headers = ["PLAYER"] + date_headers + [diff_label, period_label]

        def get_entry_near(h_map, target_d):
            candidates = [d for d in h_map.keys() if d <= target_d]
            if not candidates: return None
            best = max(candidates)
            return h_map[best]

        table_data = []
        for rid, entry in data_map.items():
            h_map = entry['history']
            d_name = entry['discord_name']
            
            # Format: RiotID\n(DiscordName)
            player_cell = f"{rid.split('#')[0]}\n({d_name})"
            row = [player_cell]
            
            # Rank for each date
            for d in shown_dates:
                h_entry = h_map.get(d)
                row.append(rank_calculator.format_rank_display(h_entry['tier'], h_entry['rank'], h_entry['lp']) if h_entry else "-")
            
            # 1. Recent Diff (vs Previous data point in filtered_dates)
            anchor_date = shown_dates[-1]
            anchor_entry = h_map.get(anchor_date)
            
            diff1_text = "-"
            if len(shown_dates) >= 2 and anchor_entry:
                prev_date = shown_dates[-2]
                prev_entry = get_entry_near(h_map, prev_date)
                if prev_entry:
                    diff1_text = rank_calculator.calculate_diff_text(prev_entry, anchor_entry)
            row.append(diff1_text)
            
            # 2. Period Diff (vs earliest data point in shown_dates)
            # Compare against the start of the shown period (MAX_DATES_IN_IMAGE=7)
            period_diff_text = "-"
            if anchor_entry and len(shown_dates) >= 1:
                compare_date = shown_dates[0]
                compare_entry = get_entry_near(h_map, compare_date)
                
                if compare_entry:
                    period_diff_text = rank_calculator.calculate_diff_text(compare_entry, anchor_entry)
            row.append(period_diff_text)
            
            table_data.append(row)

        return generate_report_image(headers, table_data, f"Rank Report ({period_type.capitalize()})")

async def setup(bot):
    await bot.add_cog(Scheduler(bot))
