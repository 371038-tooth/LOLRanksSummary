import os
import sys
import logging
from pathlib import Path
from dotenv import load_dotenv

# Set timezone to JST (for Linux environments like Railway)
os.environ['TZ'] = 'Asia/Tokyo'
load_dotenv()

# Add project root to sys.path to ensure 'src' package is found
root_path = Path(__file__).resolve().parent.parent
if str(root_path) not in sys.path:
    sys.path.append(str(root_path))

import discord
from discord.ext import commands
from src.database import db
from src.utils.opgg_client import opgg_client

from logging.handlers import RotatingFileHandler

# Configure logging
log_dir = os.path.join(root_path, 'logs')
os.makedirs(log_dir, exist_ok=True)

logging.basicConfig(
    level=logging.INFO,
    format='[%(asctime)s] [%(levelname)-8s] %(name)s: %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S',
    handlers=[
        logging.StreamHandler(sys.stdout),
        RotatingFileHandler(
            os.path.join(log_dir, 'bot.log'),
            maxBytes=5*1024*1024,
            backupCount=5,
            encoding='utf-8'
        )
    ]
)
logger = logging.getLogger(__name__)

APP_NAME = "LOLRanksSummary"
APP_ABBR = "LOLRS"

class LOLRSBot(commands.Bot):
    def __init__(self):
        intents = discord.Intents.default()
        intents.message_content = True
        intents.members = True
        super().__init__(
            command_prefix='!',
            intents=intents,
            help_command=None
        )
            
    async def setup_hook(self):
        # Connect to Database
        await db.connect()
        logger.info("Connected to Database")
        
        # Load extensions
        await self.load_extension('src.cogs.register')
        await self.load_extension('src.cogs.scheduler')
        await self.load_extension('src.cogs.utils')
        
        # Sync slash commands
        await self.tree.sync()
        logger.info("Global slash commands synced")

    async def on_message(self, message):
        if message.author.bot:
            return
        
        # Logging to see if messages are reaching the bot
        guild_info = f"Guild: {message.guild.name} ({message.guild.id})" if message.guild else "DM"
        logger.debug(f"Message from {message.author} in {guild_info} #{message.channel}: {message.content}")
        
        await self.process_commands(message)

    async def close(self):
        await opgg_client.close()
        await db.close()
        await super().close()

    async def on_ready(self):
        logger.info(f'Logged in as {self.user} (ID: {self.user.id})')

def main():
    # Attempt to get token from environment variables
    raw_token = os.getenv('DISCORD_BOT_TOKEN') or os.getenv('DISCORD_TOKEN')
    
    if not raw_token:
        logger.error("Error: No Discord token found in environment variables.")
        return

    # Clean the token for robustness
    token = "".join(char for char in raw_token if char.isprintable()).strip().strip('"').strip("'")
    
    # Prefix handling (case-insensitive)
    if token.lower().startswith('bot '):
        token = token[4:].strip()

    bot = LOLRSBot()
    bot.run(token)

if __name__ == '__main__':
    main()
