import discord
import os
from datetime import datetime
import time
import asyncio
from discord import Intents

class Printer(discord.Client):
    def __init__(self, missing_days, seller_ids_list, launcher, *args, **kwargs):
        super().__init__(*args, **kwargs, intents=Intents.default())
        self.missing_days = missing_days
        self.seller_ids_list = seller_ids_list
        self.launcher = launcher
        self.initialize()
        
    def initialize(self):
        self.discord_token = os.getenv('DISCORD_TOKEN')
        self.channel_id = int(os.getenv('DISCORD_CHANNEL_ID'))
        self.channel = None
        self.finished_setup = asyncio.Event()
        self.launched_status = asyncio.Event()
        self.to_start_recording = asyncio.Event()
        self.to_end_recording = asyncio.Event()
        self.to_send_output = asyncio.Event()
        self.launched_logs = None
        self.output_logs = None
        self.execution_time = None
        self.saved_records = None
        
    async def on_ready(self):
        print(f'Logged in as {self.user}')
        self.channel = self.get_channel(self.channel_id)
        if self.channel:
            print(f'Connected to channel {self.channel}')
            self.finished_setup.set()
        else:
            print(f"Could not find channel with ID {self.channel_id}")

    async def run_discord_client(self):
        await self.start(self.discord_token)

    async def create_logs(self, logs_type:list=['launched', 'outputs']):
        if logs_type == 'launched':
            current_time = datetime.now()
            launched_day = datetime.strftime(current_time, '%Y-%m-%d %H:%M:%S')
            records_count = len(self.seller_ids_list) * len(self.missing_days)
            launched_logs = f"""
    ------------------Launched Logs------------------
    Launched Jobs For: ```ml\nScrape Lazada Brand Portal```
    Environment: Production
    Launcher: {self.launcher}
    Records count: {records_count}
    Launched at: {launched_day}
            """
            self.launched_logs = launched_logs

        elif logs_type == 'outputs':
            await self.to_end_recording.wait()
            output_logs = f"""
    ------------------Output Logs------------------
    Just Finished: ```ml\nScrape Lazada Brand Portal```
    Environment: Production
    Duration: {self.execution_time:.4f}
    Launcher: {self.launcher}
    Records saved: {self.saved_records}
            """
            self.output_logs = output_logs
    
    async def start_recording(self):
        await self.to_start_recording.wait()
        self.start_time = time.time()

    async def end_recording(self, df):
        await self.to_end_recording.wait()
        self.end_time = time.time()
        self.saved_records = len(df)
        self.execution_time = self.end_time - self.start_time

    async def send_launched_logs(self):
        await self.finished_setup.wait()
        if self.launched_logs is not None:
            message = self.launched_logs
            self.launched_logs = None
        await self.channel.send(message)

    async def send_output_logs(self):
        await self.to_send_output.wait()
        if self.output_logs is not None:
            message = self.output_logs
            self.output_logs = None
        await self.channel.send(message)

