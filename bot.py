import discord
from discord import app_commands, Interaction, Embed, File
from discord.ext import commands
from discord.ext.commands.context import Context
from discord import Reaction, Attachment
from discord.threads import Thread
from discord.abc import GuildChannel, PrivateChannel
from discord.ui import View, Button
import gspread
import gspread.utils
from gspread.worksheet import Worksheet
from oauth2client.service_account import ServiceAccountCredentials
from datetime import datetime, timezone, timedelta

from typing import Literal, Union, List, get_args, Dict, Tuple, Optional, Any
from collections import Counter
import sys, os
import asyncio
from math import ceil
from dotenv import load_dotenv
import asyncio

# -------------- CONFIGURATION SECTION --------------
load_dotenv()  # Load variables from .env

DISCORD_TOKEN = os.getenv("DISCORD_TOKEN")
SHEET_KEY = os.getenv("SHEET_KEY")

SILVER_SHEET = "View" 
WAR_SHEET = "War" 
DEPLOYMENT_SHEET = "Deployment" 
MERCENARIES_SHEET = "Mercenaries" 
RESOURCE_SHEET = "Resource" 
PRODUCTION_SHEET = "Production" 
BUILDINGS_SHEET = "Buildings" 
ARTEFACTS_SHEET = "Artefacts"  
LOG_SHEET = "Log"  

ALLOWED_ROLES = ["Mod"]  # Roles allowed to edit silver

# Production Tiles
CROPS_TILE = "CropsTile"
FUEL_TILE = "FuelTile"
STONE_TILE = "StoneTile"
TIMBER_TILE = "TimberTile"
LIVESTOCK_TILE = "LivestockTile"
MOUNTS_TILE = "MountsTile"
METAL_TILE = "MetalTile"
FIBER_TILE = "FiberTile"

# Resources
CROPS = "Crops"
FUEL = "Fuel"
STONE = "Stone"
TIMBER = "Timber"
LIVESTOCK = "Livestock"
MOUNTS = "Mounts"
METAL = "Metal"
FIBER = "Fiber"
INDUSTRY = "Industry"
ENERGY = "Energy"
TOOLS = "Tools"
CEMENT = "Cement"
SUPPLIES = "Supplies"
USED_SPAWNS = "Used Spawns"

# Troops
ARMY = "Army"
NAVY = "Navy"

# War
ARMY_CAP = "Army cap"
NAVY_CAP = "Navy cap"
ARMY_DOCTRINE = "Army coctrine"
NAVY_DOCTRINE = "Navy coctrine"
TEMP_ARMY = "Temp Army"
TEMP_NAVY = "Temp Navy"

# Mix
NATION_NAME = "Nation name"
CAPITAL = "Capital"
RELIGION = "Religion"
CULTURE = "Culture"
SUBCULTURE = "Subculture"
XP = "XP"
SILVER = "Silver"
TILES = "Tiles"
STATUS = "Status"
UNION_LEADER = "Union Leader"
UNION_LEADER_ID = "Union Leader ID"
TITLE = "Title"
OTHER_BONUSES = "Other bonuses"

# Buildings
T1_CITY = "T1City"
T2_CITY = "T2City"
T3_CITY = "T3City"
T1_INDUSTRY = "T1Industry"
T2_INDUSTRY = "T2Industry"
T1_FORT = "T1Fort"
T2_FORT = "T2Fort"
T3_FORT = "T3Fort"
MONUMENT = "Monument"
EMPORIUM = "Emporium"

# Artefacts
ARTEFACT = "Artefact"
ARTEFACT_BONUS = "Artefact bonus"

# Log
USER_ID = "UserID"
DISCORD_NAME = "Discord name"
DISPLAY_NAME = "Display name"
DATE = "Date"
AMOUNT = "Amount"
TYPE = "Type"
SOURCE = "Source"
EDITOR_ID = "EditorID"
EDITOR_DISCORD_NAME = "Editor Discord name"
RESULT = "Result"
MESSAGE_LINK = "Message Link"

# Mercenaries
UNTIL = "Until"
SENDER_ID = "SenderID"
SENDER_NAME = "Sender name"


production_tiles = Literal[
    CROPS_TILE,
    FUEL_TILE,
    STONE_TILE,
    TIMBER_TILE,
    LIVESTOCK_TILE,
    MOUNTS_TILE,
    METAL_TILE,
    FIBER_TILE
]

resources = Literal[
    CROPS,
    FUEL,
    STONE,
    TIMBER,
    LIVESTOCK,
    MOUNTS,
    METAL,
    FIBER,
    INDUSTRY,
    ENERGY,
    TOOLS,
    CEMENT,
    SUPPLIES,
    USED_SPAWNS
]

troops = Literal[
    ARMY,
    NAVY
]

war = Literal[
    ARMY,
    NAVY,
    ARMY_CAP,
    NAVY_CAP,
    ARMY_DOCTRINE,
    NAVY_DOCTRINE,
    TEMP_ARMY,
    TEMP_NAVY
]

mix = Literal[
    NATION_NAME,
    CAPITAL,
    RELIGION,
    CULTURE,
    SUBCULTURE,
    XP,
    SILVER,
    TILES,
    STATUS,
    UNION_LEADER,
    UNION_LEADER_ID,
    TITLE,
    OTHER_BONUSES
]

buildings = Literal[
    T1_CITY,
    T2_CITY,
    T3_CITY,
    T1_INDUSTRY,
    T2_INDUSTRY,
    T1_FORT,
    T2_FORT,
    T3_FORT,
    MONUMENT,
    EMPORIUM
]

artefacts = Literal[
    ARTEFACT
]


unit_type = Union[resources, production_tiles, mix, war, buildings, artefacts]

RESOURCE_COLUMNS = {
    CROPS: 3,
    FUEL: 4,
    STONE: 5,
    TIMBER: 6,
    LIVESTOCK: 7,
    MOUNTS: 8,
    METAL: 9,
    FIBER: 10,
    INDUSTRY: 11,
    ENERGY: 12,
    TOOLS: 13,
    CEMENT: 14,
    SUPPLIES: 15,
}

SHEET_COLUMNS = {
    # -------------- VIEW SHEET --------------
    USER_ID: 1,
    DISCORD_NAME: 2,
    DISPLAY_NAME: 3,

    NATION_NAME: 4,
    CAPITAL: 5,

    RELIGION: 6,
    CULTURE: 7,
    SUBCULTURE: 8,

    XP: 9,
    SILVER: 10,
    TILES: 11,

    STATUS: 12,
    UNION_LEADER: 13,
    UNION_LEADER_ID: 14,

    TITLE: 15,
    OTHER_BONUSES: 16,

    # -------------- WAR SHEET --------------
    ARMY: 3,
    ARMY_CAP: 4,
    NAVY: 5,
    NAVY_CAP: 6,
    ARMY_DOCTRINE: 7,
    NAVY_DOCTRINE: 8,
    TEMP_ARMY: 9,
    TEMP_NAVY: 10,

    # -------------- DEPLOYMENT SHEET --------------

    #"Date": 3,
    #"Amount": 4,
    #"Type": 5,
    #"Source": 6,

    # -------------- MERCENARIES SHEET --------------
    
    SENDER_ID: 7,
    SENDER_NAME: 8,

    # -------------- RESOURCE AND PRODUCTION SHEETS --------------
    CROPS: 3,
    CROPS_TILE: 3,
    FUEL: 4,
    FUEL_TILE: 4,
    STONE: 5,
    STONE_TILE: 5,
    TIMBER: 6,
    TIMBER_TILE: 6,
    LIVESTOCK: 7,
    LIVESTOCK_TILE: 7,
    MOUNTS: 8,
    MOUNTS_TILE: 8,
    METAL: 9,
    METAL_TILE: 9,
    FIBER: 10,
    FIBER_TILE: 10,

    INDUSTRY: 11,
    ENERGY: 12,
    TOOLS: 13,
    CEMENT: 14,
    SUPPLIES: 15,
    USED_SPAWNS: 16,

    # -------------- BUILDINGS SHEET --------------
    T1_CITY: 3,
    T2_CITY: 4,
    T3_CITY: 5,
    T1_INDUSTRY: 6,
    T2_INDUSTRY: 7,
    T1_FORT: 8,
    T2_FORT: 9,
    T3_FORT: 10,
    MONUMENT: 11,
    EMPORIUM: 12,

    # -------------- ARTIFACT SHEET --------------
    ARTEFACT: 3,
    ARTEFACT_BONUS: 4,

    # -------------- LOG SHEET --------------
    DATE: 3,
    AMOUNT: 4,
    TYPE: 5,
    SOURCE: 6,
    EDITOR_ID: 7,
    EDITOR_DISCORD_NAME: 8,
    RESULT: 9,
    MESSAGE_LINK: 10,
}

normalized_unit_map = {
    key.strip().replace(" ", "").lower(): key for key in SHEET_COLUMNS.keys()
}

EXPANSION_CHANNEL_ID = "expansion_channel_id"
BUILDING_CHANNEL_ID = "building_channel_id"
RESOURCE_CHANNEL_ID = "resource_channel_id"

config = {
    EXPANSION_CHANNEL_ID: None,  # Store the ID of the expansion log channel
    BUILDING_CHANNEL_ID: None,  # Store the ID of the building log channel
    RESOURCE_CHANNEL_ID: None   # Store the ID of the resource spawn log channel
}

    # Advanced resource mapping
    
advanced_resource_map = {
    CROPS: SUPPLIES,
    MOUNTS: SUPPLIES,
    LIVESTOCK: SUPPLIES,
    METAL: TOOLS,
    TIMBER: TOOLS,
    FUEL: ENERGY,
    FIBER: ENERGY,
    STONE: CEMENT
}

# Identify columns for resources in resource_sheet
# and the corresponding tile columns in production_sheet.
# For example, if you have columns "Crops" -> "CropsTile", "Fuel" -> "FuelTile", etc.
resource_to_tile = {
    CROPS: CROPS_TILE,
    FUEL: FUEL_TILE,
    STONE: STONE_TILE,
    TIMBER: TIMBER_TILE,
    LIVESTOCK: LIVESTOCK_TILE,
    MOUNTS: MOUNTS_TILE,
    METAL: METAL_TILE,
    FIBER: FIBER_TILE
}

NUMBER_OF_SPAWNS = 3

# -------------- GOOGLE SHEETS SETUP --------------
# Define the scope
scope = ["https://www.googleapis.com/auth/spreadsheets"]

creds = ServiceAccountCredentials.from_json_keyfile_name(os.path.join(os.getcwd(), "credentials.json"), scope)
client = gspread.authorize(creds)


silver_sheet = client.open_by_key(SHEET_KEY).worksheet(SILVER_SHEET)
war_sheet = client.open_by_key(SHEET_KEY).worksheet(WAR_SHEET)
deployment_sheet = client.open_by_key(SHEET_KEY).worksheet(DEPLOYMENT_SHEET)
mercenaries_sheet = client.open_by_key(SHEET_KEY).worksheet(MERCENARIES_SHEET)
resource_sheet = client.open_by_key(SHEET_KEY).worksheet(RESOURCE_SHEET)
production_sheet = client.open_by_key(SHEET_KEY).worksheet(PRODUCTION_SHEET)
buildings_sheet = client.open_by_key(SHEET_KEY).worksheet(BUILDINGS_SHEET)
artefacts_sheet = client.open_by_key(SHEET_KEY).worksheet(ARTEFACTS_SHEET)
log_sheet = client.open_by_key(SHEET_KEY).worksheet(LOG_SHEET)


# -------------- DISCORD BOT SETUP --------------
class UncutHelpCommand(commands.HelpCommand):
    """
    A custom help command that doesn't truncate docstrings or arguments.
    """

    def get_ending_note(self):
        """
        Remove or override the built-in ending note if you like.
        """
        return "Use !help <command> for more info on a command."

    async def send_bot_help(self, mapping):
        """
        Sends a list of all commands when the user just types !help.
        """
        # Example: build a string or multiple messages from docstrings
        help_text = []
        for cog, commands_ in mapping.items():
            filtered = await self.filter_commands(commands_, sort=True)
            command_signatures = [self.get_command_signature(c) for c in filtered]
            if command_signatures:
                cog_name = getattr(cog, "qualified_name", "No Category")
                help_text.append(f"**{cog_name}**")
                help_text.extend(command_signatures)
                help_text.append("")  # blank line

        # Join them and send as a single message
        full_help = "\n".join(help_text)
        await self.get_destination().send(full_help)

    async def send_command_help(self, command):
        """
        Sends help information for a specific command (e.g. !help status).
        """
        # Use command.help, command.signature, etc. to build your own text
        help_text = (
            f"**Command:** {command.qualified_name}\n"
            f"**Usage:** {self.get_command_signature(command)}\n\n"
            f"{command.help or 'No details provided.'}"
        )
        await self.get_destination().send(help_text)

    async def send_group_help(self, group):
        """
        Sends help for a group command.
        """
        # Similar to send_command_help, but can loop over subcommands
        subcommands = group.commands
        help_text = (
            f"**Group:** {group.qualified_name}\n\n"
            f"{group.help}\n\n"
            "Subcommands:\n"
        )
        for subcommand in subcommands:
            help_text += f"  - {subcommand.name}: {subcommand.short_doc}\n"

        await self.get_destination().send(help_text)

    async def send_cog_help(self, cog):
        """
        Sends help for all commands in a specific Cog.
        """
        commands_ = cog.get_commands()
        help_text = [f"**Cog:** {cog.qualified_name}\n", cog.description, ""]
        for command in commands_:
            help_text.append(f"{self.get_command_signature(command)} - {command.short_doc}")
        full_text = "\n".join(help_text)
        await self.get_destination().send(full_text)


intents = discord.Intents.default()
intents.members = True
intents.message_content = True
bot = commands.Bot(command_prefix='!', intents=intents)
#bot.help_command = UncutHelpCommand()
tree = bot.tree


# Helper Functions

async def send(
    ctx: Context,
    msg: str | None = None,
    embed: Embed | None = None,
    file: File | None = None,
    files: list[File] | None = None,
    ephemeral: bool = False,
    mention_author: bool = False,
):
    return  await ctx.send(
            content=msg, embed=embed, file=file, files=files, mention_author=mention_author
        )
    if isinstance(ctx.interaction, Interaction):
        print("interaction")
        if not ctx.interaction.response.is_done():
            print("not done")
            await ctx.interaction.response.send_message(
                content=msg, embed=embed, file=file, files=files, ephemeral=ephemeral
            )
        else:
            print("fallback")
            # Fallback for follow-up messages if the response is already sent
            await ctx.interaction.followup.send(content=msg, embed=embed, file=file, files=files, ephemeral=ephemeral)
    else:
        print("context")
        await ctx.send(
            content=msg, embed=embed, file=file, files=files, mention_author=mention_author
        )

def author(ctx: commands.Context):
    """
    Retrieves the author/user who invoked the command, regardless of the context type.

    Args:
        ctx: The command context, which can be either a Context or an Interaction.

    Returns:
        The invoking user as a discord.Member or discord.User object.
    """
    return ctx.author

def get_unit(string: str) -> unit_type | None :
    """Returns the key corresponding to the normalized string or None if invalid."""
    search_normalized = string.strip().replace(" ", "").lower()
    return normalized_unit_map.get(search_normalized)

def get_sheet(unit: unit_type) -> Worksheet:
    """
    Returns the appropriate sheet given a unit type
    """
    if unit in get_args(mix): return silver_sheet
    if unit in get_args(production_tiles): return production_sheet
    if unit in get_args(resources): return resource_sheet
    if unit in get_args(buildings): return buildings_sheet

    if unit in get_args(war): return war_sheet
    if unit in get_args(artefacts): return artefacts_sheet
    else: return None

def batch_reset_column(sheet: gspread.Worksheet, column_name: str):
    # 1) Get the column index from your SHEET_COLUMNS mapping
    col_index = SHEET_COLUMNS[column_name]
    
    # 2) Determine the total rows
    
    num_rows = len(sheet.get_all_values())  # includes header
    if num_rows < 2:
        return  # No data rows

    # 3) Read only that column range (excluding headers). 
    #    For example, from row 2 to row N. If header is row 1, data starts row 2.
    cell_range = f"{gspread.utils.rowcol_to_a1(2, col_index)}:{gspread.utils.rowcol_to_a1(num_rows, col_index)}"

    # 4) Modify in Python
    # Replace each existing value with "0"
    #for i in range(len(column_data)):
     #   column_data[i] = [0]  # one-element list with 0
    column_data = [[0]]*(num_rows - 1)

    # 5) Write back in batch
    sheet.update(column_data, cell_range)

def get_user_balance(user_id: int, unit: unit_type) -> str | None :
    """
    Returns a given's user's unit balance.
    """

    sheet = get_sheet(unit)

    try:
        # Get all values in the column for user IDs
        user_ids = sheet.col_values(SHEET_COLUMNS[USER_ID])  
        user_row = user_ids.index(str(user_id)) + 1  # Convert index to 1-based row number
    except:
        return None

    record = sheet.cell(user_row, SHEET_COLUMNS[unit]).value


    return record

def set_user_balance(user_id: int, user_name: str, display_name: str, value: str, unit: unit_type) -> str | None :
    """
    Sets a given's user's unit balance.
    """
    sheet = get_sheet(unit)
    
    try:
        # Get all values in the column for user IDs
        user_ids = sheet.col_values(SHEET_COLUMNS[USER_ID])  
        user_row = user_ids.index(str(user_id)) + 1  # Convert index to 1-based row number
    except:
        return None

    sheet.update_cell(user_row, SHEET_COLUMNS[unit], value)
    sheet.update_cell(user_row, SHEET_COLUMNS[DISCORD_NAME], user_name)
    if sheet == silver_sheet:
        sheet.update_cell(user_row, SHEET_COLUMNS[DISPLAY_NAME], display_name)
    return value

def add_user_balance(user_id: int, user_name: str, display_name: str, amount: str, unit: unit_type) -> str | None :
    """
    Adds some amount of a unit to a given's user's unit balance.
    """
    current = get_user_balance(user_id, unit)
    if current is None:
        return None

    try:
        # Convert both current and amount to integers for addition
        current = int(current)
        amount = int(amount)
        sum = current + amount
    except ValueError:
        # If either value is non-numeric, treat as concatenated strings
        sum = f"{current}, {amount}"

    return set_user_balance(user_id, user_name, display_name, sum, unit)

def log_transaction(user_id: int, user_name: str, change_amount: str, unit: unit_type, source: str, editor_id: int, editor_name: str, result: str, message_link: str) -> str:
    timestamp = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC")

    row = [
        str(user_id),      # UserID
        user_name,         # Discord name
        timestamp,         # Date
        change_amount,     # Amount
        unit,              # Type
        source,            # Source
        str(editor_id),    # EditorID
        editor_name,       # Editor Discord name
        result,            # Result
        message_link       # Message Link
    ]

    log_sheet.append_row(row)


    return result

def is_authorized(ctx: Context) -> bool:
    """
    Check if the user invoking the command has one of the allowed roles or administrator permissions.

    Args:
        ctx: The context of the command, which can be either a Context or an Interaction.

    Returns:
        bool: True if the user is authorized, False otherwise.
    """
    invoking_user = author(ctx)  # Unified way to get the invoking user

    # Check if the user has administrator permissions
    if hasattr(invoking_user, "guild_permissions") and invoking_user.guild_permissions.administrator:
        return True

    # Check if the user has one of the allowed roles
    if hasattr(invoking_user, "roles"):
        for role in invoking_user.roles:
            if role.name in ALLOWED_ROLES:
                return True

    return False

def show_log(entry: Dict[str, int | float | str], all_params: bool):
    if all_params:
        line = (f"**User:** {entry['Discord name']} | **Change:** {entry['Amount']} {entry['Type']} | "
                f"**Source:** {entry['Source']} | **Editor:** {entry['Editor Discord name']} | "
                f"**Time:** {entry['Date']} | **New Balance:** {entry['Result']} {entry['Type']}")
    else:
        line = (f"- **User:** {entry['Discord name']} | **Change:** {entry['Amount']} {entry['Type']} | "
                f"**Source:** {entry['Source']} | "
                f"**Time:** {entry['Date']}")
    return line

def group_costs(costs: List[Tuple[int, unit_type]]) -> List[Tuple[int, unit_type]]:
    """
    Groups costs by the same unit type, summing quantities for each unit type.

    Args:
        costs: A list of (quantity, unit_type) tuples.

    Returns:
        A list of grouped (quantity, unit_type) tuples, where each unit_type appears at most once.
    """
    grouped = Counter()

    for (quantity, unit) in costs:
        grouped[unit] += quantity  # Sum quantities for the same unit type
   
    # Convert the dictionary back to a list of tuples
    return [(quantity, unit) for unit, quantity in grouped.items()]

def building_need(unit: unit_type, variable_cost: str, existing_tier: int = 0, coastal: bool = False ) -> Tuple[List[Tuple[int, unit_type]], List[Tuple[int, unit_type]]]:
    """
    Returns a tuple of 2 lists of (quantity, resource) tuples indicating the resource costs, and the rewards given upon building (e.g. increase army cap)
    for constructing/upgrading a particular building.

    :param unit: The building type (from T1City, T2City, T3City, T1Industry, etc.).
    :param use_tools: Whether the user is substituting tools for the essay/roleplay requirement.
    :param either_choice: A list of integers indicating which 'or' path is chosen 
                          for buildings with multiple branching requirements.
    :param existing_tier: The existing tier of either a monument or an emporium.
    :param coastal: Whether the building is coastal or not in case of bonuses to navy
    """
    
    if unit not in get_args(buildings):
        return None

    cost = []
    match unit:

        # ------------- CITIES -------------

        case "T1City":
            cost = parse_resource_list(variable_cost)[0], int(coastal)*[(1, "Navy cap")]
        
        case "T2City":
            cost = [(1,"T1City")] + parse_resource_list(variable_cost)[0], []
        
        case "T3City":
            cost = [(1, "T2City")] + parse_resource_list(variable_cost)[0], [(2, "Army cap")]
        
        # ------------- INDUSTRY -------------

        case "T1Industry":
            cost = parse_resource_list(variable_cost)[0], [(3, INDUSTRY)]
        
        case "T2Industry":
            cost = [(1, "T1Industry")] + parse_resource_list(variable_cost)[0], [(6, INDUSTRY)]

        # ------------- FORTS -------------

        case "T1Fort":
            cost = parse_resource_list(variable_cost)[0], [(1, "Army cap")]

        case "T2Fort":
            cost = [(1, "T1Fort")] + parse_resource_list(variable_cost)[0], []

        case "T3Fort":
            cost = [(1, "T2Fort")] + parse_resource_list(variable_cost)[0], []

        # ------------- MONUMENTS -------------

        case "Monument":
            match existing_tier:
                case 0: cost = parse_resource_list(variable_cost), []
                case 1: cost = [(1, "Monument")] + parse_resource_list(variable_cost)[0], []
                case 2: cost = [(2, "Monument")] + parse_resource_list(variable_cost)[0], []

         # ------------- EMPORIUM -------------

        case "Emporium":
            match existing_tier:
                case 0: cost = parse_resource_list(variable_cost)[0], []
                case 1: cost = [(1, "Emporium")] + parse_resource_list(variable_cost)[0], []

    return group_costs(cost[0]), group_costs(cost[1])

def parse_resource_list(resource_string: str) -> Tuple[List[Tuple[int, unit_type]], List[str]]:
    """
    Given a string like: "100 Silver, 2 Crops"
    returns a list of (resource_name, amount), e.g. [(100, "Silver"), (2, "Crops")].

    - Splits by commas for multiple resources.
    - Each part should look like "<qty> <resource name>".
      e.g. "100 Silver" or "2 Crops".
    - If parsing fails, quantity defaults to 0, and the resource name is taken from what remains.
    """
    resource_string = resource_string.strip()
    if not resource_string:
        return [], []
    
    invalid_resources = []
    resource_list = []

     # Split by commas and process each part
    for part in resource_string.split(","):
        tokens = part.strip().split(None, 1)  # Split into quantity and resource name

        if len(tokens) != 2:  # Ensure we have both quantity and resource
            invalid_resources.append(part.strip())
            continue

        # Parse the quantity
        try:
            qty = int(tokens[0])
        except ValueError:
            qty = 0  # Default quantity to 0 if parsing fails

        # Normalize and validate the resource name
        resource_name = tokens[1].strip()
        unit = get_unit(resource_name)  # Convert to standardized unit
        if unit is None:
            invalid_resources.append(resource_name)
            continue

        # Add to the resource list
        resource_list.append((qty, unit))

    return resource_list, invalid_resources

async def check_debt(ctx: Context, member: discord.Member, costs: List[Tuple[int, unit_type]], auto_cancel: bool = False, extra_message: str = ""):
    insufficient = []
    cost_summary_lines = []
    
    for qty, unit in costs:
        # Retrieve current user resource balance
        current_balance = get_user_balance(member.id, unit)
        if current_balance is None:
            await send(ctx, f"User {member.display_name} does not exist. Use !create to create the user entry.")
            return None
        current_balance = int(current_balance)

        new_balance = current_balance - qty
        if new_balance < 0:
            # This means the user would go negative for this resource
            insufficient.append(unit)
        cost_summary_lines.append(
            f"{unit}: {current_balance} -> {current_balance - qty}"
        )

    # Prepare an embed or a message summarizing cost
    cost_message = (
        f"{extra_message}"
        "**Cost Summary**\n" + "\n".join(cost_summary_lines)
    )

    # If we have any resources going negative, warn the user
    if insufficient:
        # Build a warning embed or text
        embed = discord.Embed(
            title = f"Insufficient Resources - {'Cancelling Action.'*int(auto_cancel)+'Proceed?'*int(not auto_cancel)}",
            description=(
                f"{member.display_name} does not have enough resources for this action without going into debt:\n"
                f"Would go negative in: {', '.join(insufficient)}\n\n"
                f"{"React with ✅ to confirm the action anyway, or ❌ to cancel."*int(not auto_cancel)+
                "Cancelling action."*int(auto_cancel)}"
            ),
            color=discord.Color.red()
        )
        embed.add_field(name="Cost Details", value=cost_message, inline=False)
        msg = await send(ctx, embed=embed)

        if auto_cancel: return None

        # Add reaction emojis
        await msg.add_reaction("✅")
        await msg.add_reaction("❌")

        def check(reaction: Reaction, user: discord.Member):
            return (
                user == author(ctx)
                and str(reaction.emoji) in ["✅", "❌"]
                and reaction.message.id == msg.id
            )

        try:
            reaction, user = await bot.wait_for("reaction_add", timeout=60.0, check=check)
        except asyncio.TimeoutError:
            await send(ctx, "Action timed out. No changes were made.")
            return None

        if str(reaction.emoji) == "❌":
            await send(ctx, "Action canceled.")
            return None
        # If ✅, proceed with deduction
    else:
        # No resources go negative, just show a confirmation
        await send(ctx, 
            embed=discord.Embed(
                title="Resources Sufficient",
                description=(
                    f"{member.display_name} has enough resources for this action:\n\n{cost_message}\n"
                    "Deducting resources now..."
                ),
                color=discord.Color.green()
            )
        )
        return True

def troop_costs(unit: unit_type, count: int, use_silver: bool) -> List[Tuple[int, unit_type]]:
    """
    Returns a list of (quantity, resource) tuples indicating the resource costs of deploying troops.
    for constructing/upgrading a particular building.

    :param unit: The troop type (from Army and Navy).
    :param use_silver: Whether to pay in silver instead of resources.
    """
    
    if unit not in get_args(troops):
        return None

    cost = []
    match unit:

        case "Army":
            if use_silver:
                cost = [(35*count, "Silver")]
            else:
                cost = [(1*count, "Supplies")]
        
        case "Navy":
            if use_silver:
                cost = [(60*count, "Silver")]
            else:
                cost =  [(4*count, TIMBER), (1*count, "Tools"), (1*count, "Supplies")]
       
    return group_costs(cost)


async def check_attachment(ctx: Context, channel_id: int, channel_name: str, msg_link: str) -> Optional[Tuple[Attachment, Union[GuildChannel, Thread, PrivateChannel]]]:
    # Ensure the channel is configured
    channel_id = config.get(f"{channel_name}_channel_id")
    if not channel_id:
        await send(ctx, f"The {channel_name} channel has not been configured. Use `!config {channel_name}_channel` to set it.")
        return None, None

    # Get the channel
    channel = bot.get_channel(channel_id)
    if not channel:
        await send(ctx, f"The configured {channel_name} channel is invalid or not accessible.")
        return None, None

    specified_msg = bool(msg_link)
    # Check if the message is a reply
    if not (ctx.message.reference or specified_msg):
        await send(ctx, "You need to reply to (or specifiy) a message with an attached image to use this command.")
        return None, channel

    # Get the referenced message
    if specified_msg:
        referenced_message  = await ctx.channel.fetch_message(int(msg_link.split('/')[6]))
    elif ctx.message.reference:
        referenced_message = await ctx.channel.fetch_message(ctx.message.reference.message_id)

    # Check if the referenced message has an attachment
    if not referenced_message.attachments:
        await send(ctx, "The replied message must contain an image attachment.")
        return None, channel

    # Get the first attachment (image)
    attachment = referenced_message.attachments[0]
    if not attachment.content_type.startswith("image"):
        await send(ctx, "The attachment must be an image.")
        return None, channel

    return attachment, channel, referenced_message

def get_member(member: discord.Member):

    sheet = silver_sheet

    try:
        # Get all values in the column for user IDs
        user_ids = sheet.col_values(SHEET_COLUMNS[USER_ID])  
        user_row = user_ids.index(str(member.id)) + 1  # Convert index to 1-based row number
    except:
        return member

    record = sheet.cell(user_row, SHEET_COLUMNS[UNION_LEADER_ID]).value

    if record:
        user = bot.get_user(int(record))
        return user or member
    return member

def construct_unit(member: discord.Member, date: str, amount: int, type: unit_type, source: str):
    row = [
        str(member.id),      # UserID
        member.name,    # Discord name
        date,           # Date
        amount,         # Amount
        type,           # Type
        source,         # Source
    ]

    deployment_sheet.append_row(row)

def loan_unit(receiver: discord.Member, sender: discord.Member, date_of_arrival: str, date_of_return: str, amount: int, type: unit_type, source: str):
    row1 = [
        str(receiver.id), # UserID
        receiver.name,    # Discord name
        date_of_arrival,  # Date
        amount,           # Amount
        type,             # Type
        source,           # Source
        str(sender.id),   # UserID
        sender.name,      # Discord name
    ]

    row2 = [
        str(sender.id),   # UserID
        sender.name,      # Discord name
        date_of_return,   # Date
        amount,           # Amount
        type,             # Type
        source,           # Source
        str(receiver.id), # UserID
        receiver.name,    # Discord name
    ]

    mercenaries_sheet.append_rows([row1, row2])



async def add_log(ctx: Context, member: discord.Member, amount: str, unit: unit_type, log_msg: str):
    """Adds a unit and logs the transaction."""

    user_id = str(member.id)
    current_balance = add_user_balance(user_id , member.name, member.display_name, amount, unit)
    if current_balance is None:
        return await ctx.send(f"An error occurred when fetching the current {unit} balance of {member.display_name}.")

    log_transaction(
        user_id=user_id,
        user_name=member.name,
        change_amount=amount,
        unit=unit,
        source=log_msg,
        editor_id=ctx.author.id,
        editor_name=ctx.author.name,
        result=current_balance,
        message_link=ctx.message.jump_url,
    )

    return current_balance

@bot.event
async def on_ready():
    await bot.tree.sync()
    print(f'Bot is ready. Logged in as {bot.user}.')
@bot.event
async def on_command_error(ctx: Context, error):
    if isinstance(error, commands.CommandNotFound):
        # Command doesn't exist
        # Show a generic help message with the list of commands
        await send(ctx, "Command not found. Use `!help` to see the list of available commands.", ephemeral=True)
    elif isinstance(error, commands.MissingRequiredArgument):
        # Missing arguments for command
        # Show command usage from command.help
        cmd = ctx.command
        if cmd and cmd.help:
            await send(ctx, f"Missing arguments.\n**Usage:**\n```{ctx.prefix}{cmd.name} {cmd.help}```", ephemeral=True)
        else:
            await send(ctx, "You are missing required arguments for this command.")
    elif isinstance(error, commands.BadArgument):
        # Bad argument type
        cmd = ctx.command
        if cmd and cmd.help:
            await send(ctx, f"Invalid argument.\n**Usage:**\n```{ctx.prefix}{cmd.name} {cmd.help}```", ephemeral=True)
        else:
            await send(ctx, "Invalid argument provided.", ephemeral=True)
    elif isinstance(error, commands.CheckFailure):
        # User is not authorized or does not meet a check
        await send(ctx, "You do not have permission to use this command.", ephemeral=True)
    else:
        # Other errors, just print to console and notify user
        print(error)
        await send(ctx, "An error occurred. Check the console for more details.", ephemeral=True)

# -------------- COMMANDS --------------

# Basic functions


@bot.command(name="sync", brief="Syncs the command tree.")
async def sync(ctx: commands.Context):
    """
    Syncs the app command tree. Only used by admins.
    """

    # Check if the command issuer is authorized
    if not ctx.author.guild_permissions.administrator:
        return await ctx.send("You do not have permission to configure the bot. Only Admins are allowed.")

    await bot.tree.sync()
    return await ctx.send("Command Tree synced.")


@bot.hybrid_command(name="create", brief="Create a player entry.")
@app_commands.describe(
    member="The Discord member to create the entry for.",
    nation_name="The name of the player's nation.",
    capital="The capital tile of the player's nation.",
    religion="The religion of the player's nation (optional).",
    culture="The culture of the player's nation (optional).",
    subculture="The subculture of the player's nation (optional).",
    xp="The initial XP for the player (default is 0).",
    silver="The initial silver balance for the player (default is 0).",
    tiles="The initial number of tiles for the player (default is 1).",
    costal="Whether the nation starts with a coastal city (default is False).",
    status="The player's initial status (default is 'Independent')."
)
async def create(ctx: commands.Context,
                 member: discord.Member,

                 nation_name: str, 
                 capital: str, 

                 religion: str="", 
                 culture:str="", 
                 subculture: str="", 

                 xp: int=0,
                 silver: int=0, 
                 tiles: int=1,

                 costal: bool = False,

                 status: str="Independent"):
    """
    Create a player entry in the game sheet, initializing their stats across all sheets.\n
    ‎\n
    Usage\n
    -----------\n
    !create @User "Nation Name" "Capital" [Religion] [Culture] [Subculture] [XP] [Silver] [Tiles] [costal] [Status]\n
    ‎\n
    Example\n
    -----------\n
    !create @User "Empire of France" "Paris"\n

    Parameters
    -----------
    member: discord.Member
        The Discord member to create the entry for.
    nation_name: str
        The name of the player's nation.
    capital: str
        The capital of the player's nation.
    religion: str, optional
        The religion of the player's nation (default is "").
    culture: str, optional
        The culture of the player's nation (default is "").
    subculture: str, optional
        The subculture of the player's nation (default is "").
    xp: int, optional
        The initial XP for the player (default is 0).
    silver: int, optional
        The initial silver balance for the player (default is 0).
    tiles: int, optional
        The initial number of tiles for the player (default is 1).
    costal: bool, optional
        Whether the nation has coastal access (default is True).
    status: str, optional
        The player's initial status (default is "Independent").
    """
    if ctx.interaction and not ctx.interaction.response.is_done():
        await ctx.interaction.response.defer()  # Acknowledge the interaction immediately

    # Check if the command issuer is authorized
    if not is_authorized(ctx):
        return await ctx.send("You do not have permission to create player entries.")

    # Check if user exists
    user_id = str(member.id)
    user_name = str(member.name)

    user_ids = silver_sheet.col_values(SHEET_COLUMNS[USER_ID])  # Fetch all user IDs from the sheet
    if str(user_id) in user_ids:
        return await ctx.send(f"User {member.display_name} ({user_name}) already created.")

    # Column order:
    silver_row = [
        user_id,            # UserID 
        user_name,          # Discord Name 
        member.display_name,# Display Name 
        nation_name,        # Nation name
        capital,            # Capital

        religion,           # Religion
        culture,            # Culture 
        subculture,         # Subculture 

        xp,                 # XP 
        silver,             # Silver
        tiles,              # Tiles

        status              # Status

                            # Union Leader
                            # Union Leader ID
                            # Title
                            # Other bonuses
    ]

    war_row = [
        user_id,
        user_name,
        1,          # Army 
        1,          # Army cap
        int(costal),# Navy
        int(costal),# Navy cap
        "",         # Army Doctrine
        "",         # Navy Doctrine
        0,          # Temp Army
        0           # Temp Navy
    ]

    resource_row = [
        user_id,
        user_name,
        0,  # Crops
        0,  # Fuel
        0,  # Stone
        0,  # Timber
        0,  # Livestock
        0,  # Mounts
        0,  # Metal
        0,  # Fiber
        0,  # Industry
        0,  # Energy
        0,  # Tools
        0,  # Cement
        0,  # Supplies
        0,  # Used Spawns
    ]

    # Get the 10 first columns, so only the basic resources
    production_row = resource_row[:10]

    buildings_row = [
        user_id,
        user_name,
        1, # T1City
        0, # T2City
        0, # T3City
        0, # T1Industry
        0, # T2Industry
        0, # T1Fort
        0, # T2Fort
        0, # T3Fort
        0, # Monument
        0  # Emporium
    ]

    silver_sheet.append_row(silver_row)
    war_sheet.append_row(war_row)
    resource_sheet.append_row(resource_row)
    production_sheet.append_row(production_row)
    buildings_sheet.append_row(buildings_row)


    new_balance = log_transaction(member.id, user_name, silver, SILVER, "Creation", ctx.author.id, ctx.author.display_name, silver, ctx.message.jump_url)
    await ctx.send(f"Created initial player entry for {member.display_name} ({user_name}) with balance: {new_balance}.")


@bot.hybrid_command(name="set", brief="Set a user's property.")
@app_commands.describe(
    member="The Discord member whose property to set.",
    value="The value to set for the unit.",
    unit_str="The unit type (The column name in the game sheet e.g., Silver, XP, Crops, Status, Army).",
    source="The source or reason for setting this value (optional, defaults to 'No source provided')."
)
async def set(ctx: commands.Context, member: discord.Member, value: str, unit_str: str, *, source: str = "No source provided"):
    """
    Set a user's property in the game sheet.\n
    ‎\n
    Usage\n
    -----------\n
    !set @User value unit [source]\n
    ‎\n
    Example\n
    -----------\n
    !set @User 100 Silver "Starting Silver"\n

    Parameters
    -----------
    member: discord.Member
        The Discord member whose property you want to set.
    value: str
        The value to set for the unit (e.g., 100).
    unit_str: str
        The unit type to set (e.g., "Silver", "XP", "Crops").
    source: str, optional
        The reason or source for the change (default is "No source provided").
    """

    if ctx.interaction and not ctx.interaction.response.is_done():
        await ctx.interaction.response.defer()  # Acknowledge the interaction immediately

    if not is_authorized(ctx):
        return await ctx.send("You do not have permission to modify the sheet.")

    unit = get_unit(unit_str) 
    if not unit:
        return await ctx.send(f"{unit_str} is not a valid unit. Please input valid unit.")
    
    if unit not in [UNION_LEADER, UNION_LEADER_ID, STATUS]:
        member = get_member(member)
    if member == None:
        return await ctx.send(f"Member fetch error.")
    user_name = str(member.name)
    new_value = set_user_balance(member.id, user_name, member.display_name, value, unit)
    if new_value == None:
        return await ctx.send(f"User {member.display_name} ({user_name}) does not exist. Use !create to create the user entry.")

    if value.isnumeric():
        value = int(value)
    log_transaction(member.id, user_name, value, unit, source, ctx.author.id, ctx.author.name, value, ctx.message.jump_url)
    await ctx.send(f"Set {member.display_name}'s {unit} to {value} {unit}.")


@bot.hybrid_command(name="add", brief="Add an amount to a user's chosen unit.")
@app_commands.describe(
    member="The Discord member to whom the amount will be added.",
    amount="The amount to add to the unit.",
    unit_str="The unit type (The column name in the game sheet e.g., Silver, XP, Crops, Status, Army).",
    source="The source or reason for the addition (optional, defaults to 'No source provided').",
    add_xp="Whether to auto-add the same amount of XP if the unit is Silver (defaults to True)."
)
async def add(ctx: Context, member: discord.Member, amount: int, unit_str: str, *, source: str = "No source provided", add_xp: bool = True):
    """
    Add an amount of a chosen unit to a user's balance.\n
    ‎\n
    Usage\n
    -----------\n
    !add @User amount unit [source] [add_xp]\n
    ‎\n 
    Examples\n
    -----------\n
    !add @User 100 Silver "For brilliant essay!"\n
    !add @User 10 Silver "Xp-less gift" False\n

    Parameters
    -----------
    member: discord.Member
        The member to whom the amount will be added.
    amount: int
        The amount to add to the unit.
    unit_str: str
        The unit type (e.g., "Silver", "XP", "Crops").
    source: str, optional
        The reason for the addition (default is "No source provided").
    add_xp: bool, optional
        Whether to auto-add the same amount of XP if the unit is Silver (default is True).
    """
    if ctx.interaction and not ctx.interaction.response.is_done():
        await ctx.interaction.response.defer()  # Acknowledge the interaction immediately

    if not is_authorized(ctx):
        return await send(ctx, "You do not have permission to modify the sheet.")

    unit = get_unit(unit_str) 
    if not unit:
        return await send(ctx, f"{unit_str} is not a valid unit. Please input valid unit.")

    member = get_member(member)
    if member == None:
        return await ctx.send(f"Member fetch error.")
    user_name = str(member.name)


    result = add_user_balance(member.id, user_name, member.display_name, amount, unit)
    if result == None:
        return await ctx.send(f"User {member.display_name} ({user_name}) does not exist. Use !create to create the user entry.")
    new_balance = log_transaction(member.id, user_name, amount, unit, source, author(ctx).id, author(ctx).name, result, ctx.message.jump_url)

    if add_xp and unit == SILVER and amount > 0:
        result = add_user_balance(member.id, user_name, member.display_name, amount, XP)
        new_xp = log_transaction(member.id, user_name, amount, XP, source, author(ctx).id, author(ctx).name, result, ctx.message.jump_url)

    return await send(ctx, f"Added {amount} {unit} to {member.display_name}. New amount: {new_balance}")


@bot.hybrid_command(name="delete", brief="Delete a user's records from all sheets except the log.")
@app_commands.describe(
    member="The Discord member whose records will be deleted from all sheets."
)
async def delete_command(ctx: commands.Context, member: discord.Member):
    """
    Deletes all records of the specified user from the main sheets (View, Resource, Production, Buildings, etc.), 
    leaving no empty rows behind. Does not remove entries from the log sheet.\n 
    ‎\n 
    Example\n 
    -----------\n 
    !delete @User

    Parameters
    -----------
    member: discord.Member
        The Discord member whose records will be deleted.
    """
    if ctx.interaction and not ctx.interaction.response.is_done():
        await ctx.interaction.response.defer()  # Acknowledge the interaction immediately

    if not is_authorized(ctx):
        return await ctx.send("You do not have permission to delete player entries.")

    sheets = {
        f"{SILVER_SHEET} Sheet": silver_sheet,
        f"{RESOURCE_SHEET} Sheet": resource_sheet,
        f"{PRODUCTION_SHEET} Sheet": production_sheet,
        f"{BUILDINGS_SHEET} Sheet": buildings_sheet,
        f"{WAR_SHEET} Sheet": war_sheet,
        f"{DEPLOYMENT_SHEET} Sheet": deployment_sheet,
        f"{MERCENARIES_SHEET} Sheet": mercenaries_sheet,
        f"{ARTEFACTS_SHEET} Sheet": artefacts_sheet,
    }

    user_id_str = str(member.id)  # Convert user ID to a string once
    deleted_sheets = []

    for sheet_name, sheet_obj in sheets.items():
        try:
            user_ids = sheet_obj.col_values(SHEET_COLUMNS[USER_ID])  # Fetch all UserID values
            if user_id_str in user_ids:
                row_index = user_ids.index(user_id_str) + 1  # Convert to 1-based index
                sheet_obj.delete_rows(row_index)  # Delete the row
                deleted_sheets.append(sheet_name)
        except Exception as e:
            # Handle unexpected errors and continue
            print(f"Error processing {sheet_name}: {e}")

    if not deleted_sheets:
        await ctx.send(f"No records found for {member.display_name} in any of the main sheets.")
    else:
        await ctx.send(
            f"Deleted {member.display_name}'s entry from the following sheets: {', '.join(deleted_sheets)}."
        )

# Logging and showing

#@bot.command(name="mystatus", brief="Show your current info or a specific unit's value.")
async def mystatus(ctx: commands.Context, unit_str: str | None = None):
    """
    [unit]
    Show the user's current information.
    - If a unit is specified (e.g. !mystatus silver), shows that unit's value.
    - If no unit is specified (just !mystatus), show all user data.
    """
    if isinstance(ctx.interaction, Interaction):
        if not ctx.interaction.response.is_done():
            await ctx.interaction.response.defer()  # Acknowledge the interaction immediately

    user_id = ctx.author.id

    if unit_str:
        # If a unit is specified, show just that unit
        unit = get_unit(unit_str)
        if not unit:
            return await ctx.send(f"{unit_str} is not a valid unit. Please input a valid unit.")

        balance = get_user_balance(user_id, unit)
        if balance is None:
            return await ctx.send("You have no entry yet or the specified unit is unavailable.")

        embed = discord.Embed(title=f"Your {unit} Status", color=discord.Color.gold())
        embed.add_field(name="Balance", value=str(balance), inline=False)
        embed.set_thumbnail(url=ctx.author.avatar.url if ctx.author.avatar else "")
        embed.set_footer(text=f"{unit} status")
        return await ctx.send(embed=embed)
    else:
        # No unit specified, show all user info
        try:
            # Find user rows in each sheet
            user_row_silver = silver_sheet.find(str(user_id)).row
        except:
            return await ctx.send("You don't have any entry in the database yet. Please ask a mod to create your entry.")

        # Retrieve data from silver_sheet (View sheet)
        # Using SHEET_COLUMNS keys that map to that sheet
        def val(sheet: Worksheet, key):
            # Safely retrieve value by key if exists
            col = SHEET_COLUMNS.get(key)
            return sheet.cell(user_row_silver, col).value if col else None

        discord_name = val(silver_sheet, "Discord name")
        display_name = val(silver_sheet, "Display name")
        nation_name = val(silver_sheet, "Nation name")
        capital = val(silver_sheet, "Capital")
        status = val(silver_sheet, "Status")
        xp = val(silver_sheet, "XP")
        silver_val = val(silver_sheet, "Silver")
        tiles = val(silver_sheet, "Tiles")
        culture = val(silver_sheet, "Culture")
        subculture = val(silver_sheet, "Subculture")
        religion = val(silver_sheet, "Religion")
        title = val(silver_sheet, "Title")
        other_bonuses = val(silver_sheet, "Other bonuses")
        army = val(silver_sheet, "Army")
        army_cap = val(silver_sheet, "Army cap")
        navy = val(silver_sheet, "Navy")
        navy_cap = val(silver_sheet, "Navy cap")
        doctrines = val(silver_sheet, "Doctrines")

        # Try fetching Resource and Production data
        # We assume user_id also exists there; if not, we handle gracefully
        try:
            user_row_resource = resource_sheet.find(str(user_id)).row
        except:
            user_row_resource = None

        try:
            user_row_production = production_sheet.find(str(user_id)).row
        except:
            user_row_production = None

        try:
            user_row_buildings = buildings_sheet.find(str(user_id)).row
        except:
            user_row_buildings = None

        def val_resource(key):
            if not user_row_resource: return "N/A"
            col = SHEET_COLUMNS.get(key)
            return resource_sheet.cell(user_row_resource, col).value if col else "N/A"

        def val_production(key):
            if not user_row_production: return "N/A"
            col = SHEET_COLUMNS.get(key)
            return production_sheet.cell(user_row_production, col).value if col else "N/A"

        def val_building(key):
            if not user_row_buildings: return "N/A"
            col = SHEET_COLUMNS.get(key)
            return buildings_sheet.cell(user_row_buildings, col).value if col else "N/A"

        # Gather some key stats from resources
        resource_stats = {
            "Crops": val_resource("Crops"),
            "Fuel": val_resource("Fuel"),
            "Stone": val_resource("Stone"),
            "Timber": val_resource("Timber"),
            "Livestock": val_resource("Livestock"),
            "Mounts": val_resource("Mounts"),
            "Metal": val_resource("Metal"),
            "Fiber": val_resource("Fiber"),
            "Industry": val_resource("Industry"),
            "Energy": val_resource("Energy"),
            "Tools": val_resource("Tools"),
            "Cement": val_resource("Cement"),
            "Supplies": val_resource("Supplies")
        }

        # Production tiles (CropsTile, FuelTile, etc.) - these are on production_sheet
        production_stats = {
            "CropsTile": val_production("CropsTile"),
            "FuelTile": val_production("FuelTile"),
            "StoneTile": val_production("StoneTile"),
            "TimberTile": val_production("TimberTile"),
            "LivestockTile": val_production("LivestockTile"),
            "MountsTile": val_production("MountsTile"),
            "MetalTile": val_production("MetalTile"),
            "FiberTile": val_production("FiberTile")
        }

        # Buildings
        building_stats = {
            "T1City": val_building("T1City"),
            "T2City": val_building("T2City"),
            "T3City": val_building("T3City"),
            "T1Industry": val_building("T1Industry"),
            "T2Industry": val_building("T2Industry"),
            "T1Fort": val_building("T1Fort"),
            "T2Fort": val_building("T2Fort"),
            "T3Fort": val_building("T3Fort"),
            "Monument": val_building("Monument"),
            "Emporium": val_building("Emporium")
        }

        embed = discord.Embed(title=f"{display_name}'s Status", color=discord.Color.blue())
        embed.set_thumbnail(url=ctx.author.avatar.url if ctx.author.avatar else "")

        # Basic info field
        embed.add_field(name="Basic Info", value=(
            f"**Discord Name:** {discord_name}\n"
            f"**Nation Name:** {nation_name}\n"
            f"**Capital:** {capital}\n"
            f"**Status:** {status}\n"
            f"**XP:** {xp}\n"
            f"**Silver:** {silver_val}\n"
            f"**Tiles:** {tiles}\n"
            f"**Culture:** {culture}\n"
            f"**Subculture:** {subculture}\n"
            f"**Religion:** {religion}\n"
            f"**Title:** {title}\n"
            f"**Other Bonuses:** {other_bonuses}\n"
            f"**Army:** {army}/{army_cap}\n"
            f"**Navy:** {navy}/{navy_cap}\n"
            f"**Doctrines:** {doctrines}"
        ), inline=False)

        # Resources Field
        resource_str = "\n".join([f"**{r}:** {val}" for r, val in resource_stats.items()])
        embed.add_field(name="Resources", value=resource_str, inline=False)

        # Production Field
        production_str = "\n".join([f"**{p}:** {val}" for p, val in production_stats.items()])
        embed.add_field(name="Production Tiles", value=production_str, inline=False)

        # Buildings Field
        buildings_str = "\n".join([f"**{b}:** {val}" for b, val in building_stats.items()])
        embed.add_field(name="Buildings", value=buildings_str, inline=False)

        embed.set_footer(text="Overall Status")
        await ctx.send(embed=embed)


VALID_CATEGORIES = ["resources", "buildings", "production", "war", "view"]

async def category_autocomplete(
    ctx: Context,
    current: str,
) -> List[app_commands.Choice[str]]:
    return [
        app_commands.Choice(name=cat.capitalize(), value=cat)
        for cat in VALID_CATEGORIES if current.lower() in cat.lower()
    ]

@bot.hybrid_command(name="status", brief="Show a player's info of a specific category or unit.")
@app_commands.describe(
    category="The category or unit to display (e.g., resources, buildings, production, war, view; or Silver, Crops, XP, Status).",
    member="The member whose status is being queried. Defaults to the command issuer if not specified."
)
@app_commands.autocomplete(category=category_autocomplete)
async def status(ctx: commands.Context, category: str | None = None, member: discord.Member = None):
    """
    Show a player's current information or a specific category.\n 
    - If a category is specified (e.g., !status resources @User), shows that category.\n 
    - If no category is specified, the entire status is shown.\n 
    - If no user is specified, shows the author's status.\n 
    ‎\n 
    Usage\n 
    -----------\n 
    !status [category] [@member]\n 

    Parameters
    -----------
    category: str, optional
        The category to display. It can be a sheet (e.g., resources, buildings, production, war, view) or a specific unit (e.g. e.g., Silver, XP, Crops, Status, Army). If omitted, displays all info.
    member: discord.Member, optional
        The member whose status is being queried. Defaults to the command issuer if not specified.
    """
    if ctx.interaction and not ctx.interaction.response.is_done():
        await ctx.interaction.response.defer()   # Acknowledge the interaction immediately


    # Default to the command author if no member is provided
    if not member:
        member = ctx.author
    elif ctx.author != member and not is_authorized(ctx):
        return await ctx.send("You do not have permission to check another player's status.")


    user_id = member.id

    try:
        # Get all values in the column for user IDs
        user_ids = silver_sheet.col_values(SHEET_COLUMNS[USER_ID])  
        user_row = user_ids.index(str(user_id)) + 1  # Convert index to 1-based row number
    except:
        return await ctx.send(f"User {member.display_name} does not exist. Use !create to create the user entry.")

    # Check if the category or unit is specified
    if category:
        category = category.lower()
        if category not in VALID_CATEGORIES:
            unit = get_unit(category)
            if not unit:
                return await ctx.send(
                    f"Invalid category '{category}'. Valid categories: {', '.join(VALID_CATEGORIES)}."
                )
            else:
                sheet = get_sheet(unit)
                balance = sheet.cell(user_row, SHEET_COLUMNS[unit]).value

                embed = discord.Embed(title=f"{member.display_name}'s {unit} Status", color=discord.Color.gold())
                embed.add_field(name="Balance", value=str(balance), inline=False)
                embed.set_thumbnail(url=member.avatar.url if member.avatar else "")
                embed.set_footer(text=f"{unit} status")
                return await ctx.send(embed=embed)

        elif category == "resources":

            row = resource_sheet.row_values(user_row)
            def val_resource(key):
                col = SHEET_COLUMNS.get(key)-1
                if col >= len(row):
                    return "N/A"
                return row[col]

            resource_stats = {
                "Crops": val_resource(CROPS),
                "Fuel": val_resource(FUEL),
                "Stone": val_resource(STONE),
                "Timber": val_resource(TIMBER),
                "Livestock": val_resource(LIVESTOCK),
                "Mounts": val_resource(MOUNTS),
                "Metal": val_resource(METAL),
                "Fiber": val_resource(FIBER),
                "Industry": val_resource(INDUSTRY),
                "Energy": val_resource(ENERGY),
                "Tools": val_resource(TOOLS),
                "Cement": val_resource(CEMENT),
                "Supplies": val_resource(SUPPLIES),
            }

            resource_str = "\n".join([f"**{r}:** {val}" for r, val in resource_stats.items()])
            embed = discord.Embed(
                title=f"{member.display_name}'s Resources",
                description=resource_str,
                color=discord.Color.blue()
            )
            embed.set_thumbnail(url=member.avatar.url if member.avatar else "")
            return await ctx.send(embed=embed)

        elif category == "buildings":

            row = buildings_sheet.row_values(user_row)
            def val_building(key):
                col = SHEET_COLUMNS.get(key)-1
                if col >= len(row):
                    return "N/A"
                return row[col]

            building_stats = {
                "T1City": val_building(T1_CITY),
                "T2City": val_building(T2_CITY),
                "T3City": val_building(T3_CITY),
                "T1Industry": val_building(T1_INDUSTRY),
                "T2Industry": val_building(T2_INDUSTRY),
                "T1Fort": val_building(T1_FORT),
                "T2Fort": val_building(T2_FORT),
                "T3Fort": val_building(T3_FORT),
                "Monument": val_building(MONUMENT),
                "Emporium": val_building(EMPORIUM),
            }

            buildings_str = "\n".join([f"**{b}:** {val}" for b, val in building_stats.items()])
            embed = discord.Embed(
                title=f"{member.display_name}'s Buildings",
                description=buildings_str,
                color=discord.Color.blue()
            )
            embed.set_thumbnail(url=member.avatar.url if member.avatar else "")
            return await ctx.send(embed=embed)

        elif category == "production":

            row = production_sheet.row_values(user_row)
            def val_production(key):
                col = SHEET_COLUMNS.get(key)-1
                if col >= len(row):
                    return "N/A"
                return row[col]

            production_stats = {
                "CropsTile": val_production(CROPS_TILE),
                "FuelTile": val_production(FUEL_TILE),
                "StoneTile": val_production(STONE_TILE),
                "TimberTile": val_production(TIMBER_TILE),
                "LivestockTile": val_production(LIVESTOCK_TILE),
                "MountsTile": val_production(MOUNTS_TILE),
                "MetalTile": val_production(METAL_TILE),
                "FiberTile": val_production(FIBER_TILE),
            }

            production_str = "\n".join([f"**{p}:** {val}" for p, val in production_stats.items()])
            embed = discord.Embed(
                title=f"{member.display_name}'s Production Tiles",
                description=production_str,
                color=discord.Color.blue()
            )
            embed.set_thumbnail(url=member.avatar.url if member.avatar else "")
            return await ctx.send(embed=embed)

        elif category == "view":

            row = silver_sheet.row_values(user_row)
            def val_silver(key):
                col = SHEET_COLUMNS.get(key)-1
                if col >= len(row):
                    return "N/A"
                return row[col]


            discord_name = val_silver(DISCORD_NAME)
            nation_name = val_silver(NATION_NAME)
            capital = val_silver(CAPITAL)

            xp = val_silver(XP)
            silver_val = val_silver(SILVER)
            tiles = val_silver(TILES)

            culture = val_silver(CULTURE)
            subculture = val_silver(SUBCULTURE)
            religion = val_silver(RELIGION)


            embed = discord.Embed(
                title=f"{member.display_name}'s Basic Info",
                color=discord.Color.blue()
            )
            embed.set_thumbnail(url=member.avatar.url if member.avatar else "")
            embed.add_field(name="Basic Info", value=(
                f"**Discord Name:** {discord_name}\n"
                f"**Nation Name:** {nation_name}\n"
                f"**Capital:** {capital}\n"

                f"**XP:** {xp}\n"
                f"**Silver:** {silver_val}\n"
                f"**Tiles:** {tiles}\n"

                f"**Culture:** {culture}\n"
                f"**Subculture:** {subculture}\n"
                f"**Religion:** {religion}\n"
            ), inline=False)
            return await ctx.send(embed=embed)   

        elif category == "war":

            row = war_sheet.row_values(user_row)
            def val_war(key):
                col = SHEET_COLUMNS.get(key)-1
                if col >= len(row):
                    return "N/A"
                return row[col]


            army = val_war(ARMY)
            army_cap = val_war(ARMY_CAP)
            navy = val_war(NAVY)
            navy_cap = val_war(NAVY_CAP)

            army_doc = val_war(ARMY_DOCTRINE)
            navy_doc = val_war(NAVY_DOCTRINE)

            temp_army = val_war(TEMP_ARMY)
            temp_navy = val_war(TEMP_NAVY)

            embed = discord.Embed(
                title=f"{member.display_name}'s Basic Info",
                color=discord.Color.blue()
            )
            embed.set_thumbnail(url=member.avatar.url if member.avatar else "")
            embed.add_field(name="Basic Info", value=(
                f"**Army:** {army}+{temp_army}/{army_cap}\n"
                f"**Navy:** {navy}+{temp_navy}/{navy_cap}\n"
                f"**Army Doctrine:** {army_doc}\n"
                f"**Navy Doctrine:** {navy_doc}\n"
                
            ), inline=False)
            return await ctx.send(embed=embed)         
        else:
            return await ctx.send(f"Invalid category '{category}'. Valid categories: resources, buildings, production, war, view.")

    
    resource_row = resource_sheet.row_values(user_row)
    def val_resource(key):
        col = SHEET_COLUMNS.get(key)-1
        if col >= len(resource_row):
            return "N/A"
        return resource_row[col]

    buildings_row = buildings_sheet.row_values(user_row)
    def val_building(key):
        col = SHEET_COLUMNS.get(key)-1
        if col >= len(buildings_row):
            return "N/A"
        return buildings_row[col]

    production_row = production_sheet.row_values(user_row)
    def val_production(key):
        col = SHEET_COLUMNS.get(key)-1
        if col >= len(production_row):
            return "N/A"
        return production_row[col]

    silver_row = silver_sheet.row_values(user_row)
    def val_silver(key):
        col = SHEET_COLUMNS.get(key)-1
        if col >= len(silver_row):
            return "N/A"
        return silver_row[col]

    war_row = war_sheet.row_values(user_row)
    def val_war(key):
        col = SHEET_COLUMNS.get(key)-1
        if col >= len(war_row):
            return "N/A"
        return war_row[col]


    discord_name = val_silver(DISCORD_NAME)
    nation_name = val_silver(NATION_NAME)
    capital = val_silver(CAPITAL)

    xp = val_silver(XP)
    silver_val = val_silver(SILVER)
    tiles = val_silver(TILES)

    culture = val_silver(CULTURE)
    subculture = val_silver(SUBCULTURE)
    religion = val_silver(RELIGION)

    status = val_silver(STATUS)
    union_leader = val_silver(UNION_LEADER)
    title = val_silver(TITLE)
    other_bonuses = val_silver(OTHER_BONUSES)


    # Gather some key stats from resources
    resource_stats = {
        "Crops": val_resource(CROPS),
        "Fuel": val_resource(FUEL),
        "Stone": val_resource(STONE),
        "Timber": val_resource(TIMBER),
        "Livestock": val_resource(LIVESTOCK),
        "Mounts": val_resource(MOUNTS),
        "Metal": val_resource(METAL),
        "Fiber": val_resource(FIBER),
        "Industry": val_resource(INDUSTRY),
        "Energy": val_resource(ENERGY),
        "Tools": val_resource(TOOLS),
        "Cement": val_resource(CEMENT),
        "Supplies": val_resource(SUPPLIES),
    }

    # Production tiles (CropsTile, FuelTile, etc.) - these are on production_sheet
    production_stats = {
        "CropsTile": val_production(CROPS_TILE),
        "FuelTile": val_production(FUEL_TILE),
        "StoneTile": val_production(STONE_TILE),
        "TimberTile": val_production(TIMBER_TILE),
        "LivestockTile": val_production(LIVESTOCK_TILE),
        "MountsTile": val_production(MOUNTS_TILE),
        "MetalTile": val_production(METAL_TILE),
        "FiberTile": val_production(FIBER_TILE),
    }

    # Buildings

    building_stats = {
        "T1City": val_building(T1_CITY),
        "T2City": val_building(T2_CITY),
        "T3City": val_building(T3_CITY),
        "T1Industry": val_building(T1_INDUSTRY),
        "T2Industry": val_building(T2_INDUSTRY),
        "T1Fort": val_building(T1_FORT),
        "T2Fort": val_building(T2_FORT),
        "T3Fort": val_building(T3_FORT),
        "Monument": val_building(MONUMENT),
        "Emporium": val_building(EMPORIUM),
    }

    # War
    war_stats = {
        "Army": f"{val_war(ARMY)}+{val_war(TEMP_ARMY)}/{val_war(ARMY_CAP)}",
        "Navy": f"{val_war(NAVY)}+{val_war(TEMP_NAVY)}/{val_war(NAVY_CAP)}",
        "Army Doctrine": val_war(ARMY_DOCTRINE),
        "Navy Doctrine": val_war(NAVY_DOCTRINE)
    }

    embed = discord.Embed(title=f"{member.display_name}'s Status", color=discord.Color.blue())
    embed.set_thumbnail(url=ctx.author.avatar.url if ctx.author.avatar else "")

    # Basic info field
    embed.add_field(name="Basic Info", value=(
        f"**Discord Name:** {discord_name}\n"
        f"**Nation Name:** {nation_name}\n"
        f"**Capital:** {capital}\n"
        f"**XP:** {xp}\n"
        f"**Silver:** {silver_val}\n"
        f"**Tiles:** {tiles}\n"
        f"**Culture:** {culture}\n"
        f"**Subculture:** {subculture}\n"
        f"**Religion:** {religion}\n"
        f"**Status:** {status} {f'(Union Leader: {union_leader})' if union_leader else ''}\n"
        f"**Title:** {title}\n"
        f"**Other Bonuses:** {other_bonuses}\n"
    ), inline=False)

    # Resources Field
    resource_str = "\n".join([f"**{r}:** {val}" for r, val in resource_stats.items()])
    embed.add_field(name="Resources", value=resource_str, inline=False)

    # Production Field
    production_str = "\n".join([f"**{p}:** {val}" for p, val in production_stats.items()])
    embed.add_field(name="Production Tiles", value=production_str, inline=False)

    # Buildings Field
    buildings_str = "\n".join([f"**{b}:** {val}" for b, val in building_stats.items()])
    embed.add_field(name="Buildings", value=buildings_str, inline=False)

    # War Field
    war_str = "\n".join([f"**{b}:** {val}" for b, val in war_stats.items()])
    embed.add_field(name="War", value=war_str, inline=False)


    embed.set_footer(text="Overall Status")
    return await ctx.send(embed=embed)

"""
VALID_CATEGORIES = ["resources", "buildings", "production", "war", "view"]

async def category_autocomplete(
    ctx: Context,
    current: str,
) -> List[app_commands.Choice[str]]:
    return [
        app_commands.Choice(name=cat.capitalize(), value=cat)
        for cat in VALID_CATEGORIES if current.lower() in cat.lower()
    ]

@bot.hybrid_command(name="status", brief="Show a player's info of a specific category or unit.")
@app_commands.describe(
    category="The category or unit to display (e.g., resources, buildings, production, war, view; or Silver, Crops, XP, Status).",
    member="The member whose status is being queried. Defaults to the command issuer if not specified."
)
@app_commands.autocomplete(category=category_autocomplete)
async def status(ctx: commands.Context, category: str | None = None, member: discord.Member = None):
    ""
    Show a player's current information or a specific category.\n 
    - If a category is specified (e.g., !status resources @User), shows that category.\n 
    - If no category is specified, the entire status is shown.\n 
    - If no user is specified, shows the author's status.\n 
    ‎\n 
    Usage\n 
    -----------\n 
    !status [category] [@member]\n 

    Parameters
    -----------
    category: str, optional
        The category to display. It can be a sheet (e.g., resources, buildings, production, war, view) or a specific unit (e.g. e.g., Silver, XP, Crops, Status, Army). If omitted, displays all info.
    member: discord.Member, optional
        The member whose status is being queried. Defaults to the command issuer if not specified.
    ""
    if ctx.interaction and not ctx.interaction.response.is_done():
        await ctx.interaction.response.defer()

    # Default to the command author if no member is provided
    if not member:
        member = ctx.author
    elif ctx.author != member and not is_authorized(ctx):
        return await ctx.send("You do not have permission to check another player's status.")

    user_id = str(member.id)
    category = category.lower() if category else None

    # Define a helper function for retrieving cell values
    def get_value(sheet, row, key):
        col = SHEET_COLUMNS.get(key)
        return sheet.cell(row, col).value if col else "N/A"

    # Map categories to their corresponding sheets and data processors
    category_map = {
        "resources": (resource_sheet, lambda row: {
            key: get_value(resource_sheet, row, key)
            for key in SHEET_COLUMNS.keys() if key in resources
        }),
        "buildings": (buildings_sheet, lambda row: {
            key: get_value(buildings_sheet, row, key)
            for key in SHEET_COLUMNS.keys() if key in buildings
        }),
        "production": (production_sheet, lambda row: {
            key: get_value(production_sheet, row, key)
            for key in SHEET_COLUMNS.keys() if key in production_tiles
        }),
        "war": (war_sheet, lambda row: {
            "Army": f"{get_value(war_sheet, row, ARMY)}+{get_value(war_sheet, row, TEMP_ARMY)}/{get_value(war_sheet, row, ARMY_CAP)}",
            "Navy": f"{get_value(war_sheet, row, NAVY)}+{get_value(war_sheet, row, TEMP_NAVY)}/{get_value(war_sheet, row, NAVY_CAP)}",
            "Army Doctrine": get_value(war_sheet, row, ARMY_DOCTRINE),
            "Navy Doctrine": get_value(war_sheet, row, NAVY_DOCTRINE)
        }), 
        "view": (silver_sheet, lambda row: {
            key: get_value(silver_sheet, row, key)
            for key in SHEET_COLUMNS.keys() if key in mix
        }),
    }

    # If a category is specified, handle it
    if category:
        if category not in category_map:
            unit = get_unit(category)
            if not unit:
                return await ctx.send(
                    f"Invalid category '{category}'. Valid categories: {', '.join(category_map.keys())}."
                )
            else:
                if unit:
                    # If a unit is specified, show just that unit
                    if not unit:
                        return await ctx.send(f"{category} is not a valid unit. Please input a valid unit.")

                    balance = get_user_balance(user_id, unit)
                    if balance is None:
                        return await ctx.send(f"{member.display_name} does not have an entry yet or the specified unit is unavailable.")

                    embed = discord.Embed(title=f"{member.display_name}'s {unit} Status", color=discord.Color.gold())
                    embed.add_field(name="Balance", value=str(balance), inline=False)
                    embed.set_thumbnail(url=member.avatar.url if member.avatar else "")
                    embed.set_footer(text=f"{unit} status")
                    return await ctx.send(embed=embed)

        sheet, processor = category_map[category]
        try:
            user_row = sheet.find(user_id).row
        except gspread.exceptions.CellNotFound:
            return await ctx.send(f"{member.display_name} does not have data in the {category} category.")

        data = processor(user_row)
        formatted_data = "\n".join([f"**{key}:** {value}" for key, value in data.items()])
        embed = discord.Embed(
            title=f"{member.display_name}'s {category.capitalize()}",
            description=formatted_data,
            color=discord.Color.blue()
        )
        embed.set_thumbnail(url=member.avatar.url if member.avatar else "")
        return await ctx.send(embed=embed)

    # No category specified, show all user info
    try:
        user_row_silver = silver_sheet.find(user_id).row
    except gspread.exceptions.CellNotFound:
        return await ctx.send(f"{member.display_name} does not have any entry in the sheet.")

    basic_info = {
        "Discord Name": get_value(silver_sheet, user_row_silver, DISCORD_NAME),
        "Nation Name": get_value(silver_sheet, user_row_silver, NATION_NAME),
        "Capital": get_value(silver_sheet, user_row_silver, CAPITAL),
        "XP": get_value(silver_sheet, user_row_silver, XP),
        "Silver": get_value(silver_sheet, user_row_silver, SILVER),
        "Tiles": get_value(silver_sheet, user_row_silver, TILES),
        "Culture": get_value(silver_sheet, user_row_silver, CULTURE),
        "Subculture": get_value(silver_sheet, user_row_silver, SUBCULTURE),
        "Religion": get_value(silver_sheet, user_row_silver, RELIGION),
        "Status": get_value(silver_sheet, user_row_silver, STATUS),
        "Title": get_value(silver_sheet, user_row_silver, TITLE),
        "Other Bonuses": get_value(silver_sheet, user_row_silver, OTHER_BONUSES),
    }

    # Attempt to fetch additional data from other sheets
    def try_get_data(sheet, processor):
        try:
            user_row = sheet.find(user_id).row
            return processor(user_row)
        except gspread.exceptions.CellNotFound:
            return {}

    resource_data = try_get_data(resource_sheet, category_map["resources"][1])
    production_data = try_get_data(production_sheet, category_map["production"][1])
    building_data = try_get_data(buildings_sheet, category_map["buildings"][1])
    war_data = try_get_data(war_sheet, category_map["war"][1])

    # Format and send the full embed
    embed = discord.Embed(title=f"{member.display_name}'s Status", color=discord.Color.blue())
    embed.set_thumbnail(url=member.avatar.url if member.avatar else "")

    embed.add_field(name="Basic Info", value="\n".join([f"**{key}:** {value}" for key, value in basic_info.items()]), inline=False)
    embed.add_field(name="Resources", value="\n".join([f"**{key}:** {value}" for key, value in resource_data.items()]), inline=False)
    embed.add_field(name="Production", value="\n".join([f"**{key}:** {value}" for key, value in production_data.items()]), inline=False)
    embed.add_field(name="Buildings", value="\n".join([f"**{key}:** {value}" for key, value in building_data.items()]), inline=False)
    embed.add_field(name="War", value="\n".join([f"**{key}:** {value}" for key, value in war_data.items()]), inline=False)

    await ctx.send(embed=embed)
"""

MAXIMUM_SHORT_LOG_LENGTH = 20
LOG_CHUNK_SIZE = 5
VALID_PARAMS = ["all", "short"]
VALID_ACTIONS = ["latest", "full"]

async def length_autocomplete(
    ctx: Context,
    current: str,
) -> List[app_commands.Choice[str]]:
    return [
        app_commands.Choice(name=param.capitalize(), value=param)
        for param in VALID_PARAMS if current.lower() in param.lower()
    ]

async def action_autocomplete(
    ctx: Context,
    current: str,
) -> List[app_commands.Choice[str]]:
    return [
        app_commands.Choice(name=action.capitalize(), value=action)
        for action in VALID_ACTIONS if current.lower() in action.lower()
    ]


@bot.hybrid_command(name="changelog", brief="Show transaction logs.")
@app_commands.describe(
    params="Specify 'all' for detailed logs or 'short' for minimal logs.",
    action="Specify 'latest' for recent logs or 'full' for the complete log.",
    number="The number of recent entries to display (used with 'latest').",
    user="Filter logs for a specific user (optional)."
)
@app_commands.autocomplete(params=length_autocomplete, action=action_autocomplete)
async def changelog(ctx: commands.Context, params: str, action: str = "latest", number: int = 5, user: discord.Member = None):
    """
    Show transaction logs for users or the entire game.\n 
    ‎\n 
    Usage\n 
    -----------\n 
    !changelog [all | short] [latest | full] [number] [@User]\n 
    ‎\n 
    Examples\n 
    -----------\n 
    - !changelog all latest 5\n 
      Shows the last 5 detailed log entries in the channel.\n 
    - !changelog short\n 
      Shows the last 5 minimal log entries in the channel.\n 
    - !changelog all full\n 
      Sends the complete detailed log via DM (mods only).\n 
    - !changelog short latest 10 @User\n 
      Shows the last 10 minimal log entries for a specific user in the channel.\n 

    Parameters
    -----------
    params: str
        Specify 'all' for detailed logs or 'short' for minimal logs.
    action: str, optional
        Specify 'latest' for recent logs or 'full' for the complete log (default is 'latest').
    number: int, optional
        The number of recent entries to display when using 'latest' (default is 5).
    user: discord.Member, optional
        Filter logs for a specific user. Defaults to all users.
    """
    if isinstance(ctx.interaction, Interaction) and not ctx.interaction.response.is_done():
        await ctx.interaction.response.defer()

    # Fetch all data from the log sheet
    records = log_sheet.get_all_records()

    # Filter by user if specified
    if user:
        records = [r for r in records if r[USER_ID] == str(user.id)]

    # Handle empty records
    if not records:
        if action.lower() == "full" and is_authorized(ctx):
            await ctx.author.send("The changelog is empty.")
            await ctx.send("No records found.")
        else:
            await ctx.send("No records found.")
        return

    # Determine whether to show detailed logs
    all_params = params.lower() == "all"

    if action.lower() == "latest":
        # Show the latest entries
        number = min(number, MAXIMUM_SHORT_LOG_LENGTH)  # Limit to the maximum allowed
        latest_entries = records[-number:] if len(records) > number else records

        if not latest_entries:
            return await ctx.send("No records found.")

        # Prepare and send log entries in chunks
        lines = [show_log(entry, all_params) for entry in latest_entries]
        for i in range(0, len(lines), LOG_CHUNK_SIZE):
            chunk = lines[i:i + LOG_CHUNK_SIZE]
            embed = discord.Embed(title="Latest Changes", description="\n".join(chunk), color=discord.Color.blue())
            await ctx.send(embed=embed)

    elif action.lower() == "full":
        # Only authorized users can view the full log
        if not is_authorized(ctx):
            return await ctx.send("You don't have permission to view the full changelog.")

        # Prepare and send all log entries in chunks via DM
        lines = [show_log(entry, all_params) for entry in records]
        for i in range(0, len(lines), LOG_CHUNK_SIZE):
            chunk = lines[i:i + LOG_CHUNK_SIZE]
            embed = discord.Embed(title="Full Changelog", description="\n".join(chunk), color=discord.Color.blue())
            await ctx.author.send(embed=embed)

        await ctx.send("Full changelog sent via DM.")
    else:
        await ctx.send("Invalid action for changelog. Use 'latest' or 'full'.")


# Config 
CONFIG_PARAMETERS = [
    "expansion_channel",
    "resource_channel",
    "building_channel"
]

async def param_autocomplete(
    ctx: Context,
    current: str,
) -> List[app_commands.Choice[str]]:
    return [
        app_commands.Choice(name=parameter, value=parameter)
        for parameter in CONFIG_PARAMETERS if current.lower() in parameter.lower()
    ]

@bot.hybrid_command(name="config", brief="Set the configuration parameters.")
@app_commands.autocomplete(parameter=param_autocomplete)
async def config_command(ctx: commands.Context, parameter: str):
    """
    !config expansion_channel
    !config resource_channel
    !config building_channel

    Sets the current channel as the specified logging channel.
    """

    if isinstance(ctx.interaction, Interaction) and not ctx.interaction.response.is_done():
        await ctx.interaction.response.defer()  # Acknowledge the interaction immediately

    if not ctx.author.guild_permissions.administrator:
        return await ctx.send("You do not have permission to configure the bot. Only Admins are allowed.")


    if parameter.strip().lower()[-8:] == "_channel":
        config[f"{parameter.strip().lower()[:-8]}_channel_id"] = ctx.channel.id
        await ctx.send(f"The {parameter.strip().lower()[:-8]} channel has been set to {ctx.channel.mention}.")
    else:
        await ctx.send("Invalid parameter. Supported parameters: 'expansion_channel', 'building_channel', 'resource_channel'.")


# Temp functions 

#@bot.hybrid_command(name="name", brief="Test command ensuring display and global names.")
async def name(ctx: commands.Context, member: discord.Member):
    """
    @User 
    Example: !name @User
    """
    if isinstance(ctx.interaction, Interaction) and not ctx.interaction.response.is_done():
        await ctx.interaction.response.defer() # Acknowledge the interaction immediately

    if not is_authorized(ctx):
        return await ctx.send("You do not have permission to modify the sheet.")

    user_global_name = member.global_name if member.global_name else member.display_name

    return await ctx.send(f"User display: {member.display_name}; user global: {member.global_name}; user name: {member.name} (chosen: {user_global_name}).")

# Shortcuts 

@bot.hybrid_command(name="build", brief="Build or upgrade a building by deducting required resources.")
@app_commands.describe(
    member="The user for whom the building is constructed.",
    building_name="The building to construct (e.g., T1City, T2City, T1Fort, etc.).",
    tile_name="The name of the tile where the building is constructed (optional).",
    cost="The cost for the building, including the tools for skipping essays (e.g., '50 Silver, 6 Crops').",
    costal="Whether the building is coastal (yes/no).",
    msg_link="A message link to the message with an attached image showing the tile where the building is built. This is required when using slash commands."
)
async def build(
    ctx: commands.Context,
    member: discord.Member,
    building_name: str,
    tile_name: str = "",
    cost: str = "",
    costal: str = "",
    msg_link: str = ""
):
    """
    Construct or upgrade a building for a user, deducting the required resources.\n 
    ‎\n 
    Usage\n 
    -----------\n 
    !build @User building_name [cost] [costal] [msg_link]\n   
    ‎\n 
    Examples\n 
    -----------\n 
    - !build @User T2City "2 tools, 200 silver" Coastal \n 
    - !build @User T1City "6 Crops"\n 
    - /build @User T2City "50 silver, 6 cement, 2 supplies" no "https://discord.com/channels/123/456/789"\n 

    Parameters
    -----------
    member: discord.Member
        The user for whom the building is constructed.
    building_name: str
        The building to construct (e.g., T1City, T2City, T1Fort, etc.).
    tile_name: str, optional
        The name of the tile where the building is constructed (default is "").
    costs: str, optional
        A comma-separated list of the resources and amounts making up the cost of the building (e.g., '100 Silver, 2 Crops').
    costal: str, optional
        Whether the building is coastal. Acceptable values: "yes" "true", "costal", "no", "false" (default is "").
    msg_link: str, optional
        A message link to the message with an attached image showing the tile where the building is built. This is required when using slash commands. (default is "").
    """
    if isinstance(ctx.interaction, Interaction) and not ctx.interaction.response.is_done():
        await ctx.interaction.response.defer()  # Acknowledge the interaction immediately

    if not is_authorized(ctx):
        return await ctx.send("You do not have permission to build structures.")

    # Ensure the expansion channel is configured
    building_channel_id = config["building_channel_id"]
    attachment, building_channel, referenced_msg = await check_attachment(ctx, building_channel_id, "building", msg_link)
    has_image = bool(attachment)
    if not has_image:
        return
        
    is_costal = costal.strip().lower() in {"costal", "true", "yes"}


    building_name = get_unit(building_name)
    if not building_name:
        return await ctx.send(f"Building type '{building_name}' is invalid or not implemented.")

    existing_tier = 0
    try:
        if building_name == MONUMENT:
            existing_tier = int(get_unit(MONUMENT))
        elif building_name == EMPORIUM:
            existing_tier = int(get_unit(EMPORIUM))
    except:
        existing_tier = 0


    # Get the resource costs from your building_need function
    costs, bonuses = building_need(building_name, cost, existing_tier, is_costal)
    if not costs:
        return await ctx.send(f"Building type '{building_name}' is invalid or not implemented.")

    

    # We'll gather a summary of the cost and also check the user’s balance
    member = get_member(member)
    user_id = member.id

    extra_msg = (
        f"**Building:** {building_name}\n"
        f"**Building Costs:** {', '.join(f"{count} {name}" for count, name in costs)}\n"
    )

    check = await check_debt(ctx, member, costs, True, extra_msg)
    if not check: return


    # At this point, we confirmed we want to proceed. Deduct resources from the sheet.
    for qty, resource_name in costs:

        await add_log(ctx, member, -qty, get_unit(resource_name), f"Building {building_name} in {tile_name}")


    for qty, resource_name in bonuses:

        await add_log(ctx, member, qty, get_unit(resource_name), f"Building {building_name} in {tile_name}")

    await add_log(ctx, member, 1, building_name, f"Building {building_name} in {tile_name}")

    await referenced_msg.add_reaction("✅")
    # Post the image and message in the building channel
    embed = discord.Embed(
        description=None,
        color=discord.Color.blue()
    )
    embed.set_author(name=f"Building of {building_name} in {tile_name} by {member.display_name}", icon_url=member.avatar.url if member.avatar else None)
    embed.set_footer(text=f"Cost: {", ".join([str(q)+" "+r for q,r in costs])}")
    if has_image:
        embed.set_image(url=attachment.url)

    await building_channel.send(embed=embed)
    await ctx.send(f"Building of {building_name} in {tile_name} by {member.display_name} has been successfully posted in {building_channel.mention}.")

    await ctx.send(
        f"Successfully built {building_name} for {member.mention}, "
        f"the required resources have been deducted from {member.display_name}."
    )


@bot.hybrid_command(name="trade", brief="Swap resources between two users.")
@app_commands.describe(
    user1="The first user involved in the trade.",
    user1_sends="Resources sent by the first user (e.g., '100 Silver, 2 Crops').",
    user2="The second user involved in the trade.",
    user2_sends="Resources sent by the second user (e.g., '50 Timber, 3 Tools')."
)
async def trade(
    ctx: commands.Context,
    user1: discord.Member,
    user1_sends: str,
    user2: discord.Member,
    user2_sends: str = ""
):
    """
    Trade resources between two users by deducting and adding specified amounts.\n
    ‎\n
    Usage\n
    -----------\n
      !trade @User1 [resources user1 sends] @User2 [resources user2 sends]\n
    ‎\n
    Examples\n
    -----------\n
      - !trade @UserA "100 Silver, 2 Crops" @UserB "30 Timber, 2 Tools"\n
        This means:\n
          - UserA sends 100 Silver and 2 Crops to UserB.\n
          - UserB sends 30 Timber and 2 Tools to UserA.\n
      - !trade @User1 "100 Silver, 2 Crops" @User2 "9 Energy"\n
      - !trade @User1 "5 Silver" @User2\n
      - !trade @User1 "" @User2 "5 Silver"\n

    Parameters
    -----------
    user1: discord.Member
        The first user involved in the trade.
    user1_sends: str
        A comma-separated list of resources and amounts sent by the first user (e.g., '100 Silver, 2 Crops').
    user2: discord.Member
        The second user involved in the trade.
    user2_sends: str, optional
        A comma-separated list of resources and amounts sent by the second user (default is an empty string).
    """
    if isinstance(ctx.interaction, Interaction) and not ctx.interaction.response.is_done():
        await ctx.interaction.response.defer()  # Acknowledge the interaction immediately

    # 1. Authorization check
    if not is_authorized(ctx):
        return await ctx.send("You do not have permission to initiate trades.")

    user1 = get_member(user1)
    user2 = get_member(user2)
    # 2. Parse resource strings
    user1_list, invalid1 = parse_resource_list(user1_sends)
    user2_list, invalid2 = parse_resource_list(user2_sends)

    if invalid1 or invalid2:
        return await ctx.send(f"Invalid resources: {invalid1 + invalid2}. Aborting Trade.")

    if not user1_list and not user2_list:
        return await ctx.send("No resources specified to trade.")

    user1_id = user1.id
    user2_id = user2.id

    # Track resource shortfalls for each user
    insufficient_user1 = []
    insufficient_user2 = []

    # Summaries
    summary_user1_lines = []
    summary_user2_lines = []

    # 3. Check user1's resources for user1_list
    for (qty, res_name) in user1_list:
        current_balance = get_user_balance(user1_id, res_name)
        if current_balance is None:
            return await ctx.send(f"An error occurred when fetching the current {res_name} balance of {user1.display_name}.")
        current_balance = int(current_balance)

    
        new_balance = current_balance - qty
        if new_balance < 0:
            insufficient_user1.append(res_name)
        summary_user1_lines.append(f"{res_name}: {current_balance} -> {new_balance}")

        current_balance = get_user_balance(user2_id, res_name)
        if current_balance is None:
            return await ctx.send(f"An error occurred when fetching the current {res_name} balance of {user2.display_name}.")
        current_balance = int(current_balance)

        new_balance = current_balance + qty
        summary_user2_lines.append(f"{res_name}: {current_balance} -> {new_balance}")

    # Check user2's resources for user2_list
    for  (qty, res_name) in user2_list:
        current_balance = get_user_balance(user2_id, res_name)
        if current_balance is None:
            return await ctx.send(f"An error occurred when fetching the current {res_name} balance of {user2.display_name}.")
        current_balance = int(current_balance)

        new_balance = current_balance - qty
        if new_balance < 0:
            insufficient_user2.append(res_name)
        summary_user2_lines.append(f"{res_name}: {current_balance} -> {new_balance}")

        current_balance = get_user_balance(user1_id, res_name)
        if current_balance is None:
            return await ctx.send(f"An error occurred when fetching the current {res_name} balance of {user1.display_name}.")
        current_balance = int(current_balance)
        new_balance = current_balance + qty
        summary_user1_lines.append(f"{res_name}: {current_balance} -> {new_balance}")


    # Create an embed summarizing the trade
    embed = discord.Embed(title="Trade Summary", color=discord.Color.blue())
    embed.add_field(
        name=f"{user1.display_name} → {user2.display_name}",
        value="\n".join(summary_user1_lines) if summary_user1_lines else "Nothing sent",
        inline=False
    )
    embed.add_field(
        name=f"{user2.display_name} → {user1.display_name}",
        value="\n".join(summary_user2_lines) if summary_user2_lines else "Nothing sent",
        inline=False
    )

    # Warnings if negative
    negatives = []
    if insufficient_user1:
        negatives.append(f"{user1.display_name} going negative in: {', '.join(insufficient_user1)}")
    if insufficient_user2:
        negatives.append(f"{user2.display_name} going negative in: {', '.join(insufficient_user2)}")

    if negatives:
        embed.add_field(
            name="Warning: Insufficient Resources",
            value=("\n".join(negatives) + "\nReact with ✅ to confirm or ❌ to cancel."),
            inline=False
        )

    msg = await ctx.send(embed=embed)

    # 4. If negatives exist, prompt confirmation
    if insufficient_user1 or insufficient_user2:
        await msg.add_reaction("✅")
        await msg.add_reaction("❌")

        def check(reaction: Reaction, user):
            return (
                user == ctx.author
                and str(reaction.emoji) in ["✅", "❌"]
                and reaction.message.id == msg.id
            )

        try:
            reaction, user = await bot.wait_for("reaction_add", timeout=60.0, check=check)
        except asyncio.TimeoutError:
            await ctx.send("Trade request timed out. No changes made.")
            return
        if str(reaction.emoji) == "❌":
            await ctx.send("Trade canceled. No changes made.")
            return
        # Otherwise proceed

    # 5. Execute the trade
    # user1 -> user2
    for (qty, res_name) in user1_list:
        res_name = get_unit(res_name)
        # Deduct from user1
        await add_log(ctx, user1, -qty, res_name, f"Trade to {user2.name}")

        # Add to user2
        await add_log(ctx, user2, qty, res_name, f"Trade from {user1.name}")

    # user2 -> user1
    for (qty, res_name) in user2_list:
        res_name = get_unit(res_name)
        # Deduct from user2
        await add_log(ctx, user2, -qty, res_name, f"Trade to {user1.name}")

        # Add to user1
        await add_log(ctx, user1, qty, res_name, f"Trade from {user2.name}")


    await ctx.send(f"Trade completed between {user1.display_name} and {user2.display_name}.")



@bot.hybrid_command(name="respawn_month", brief="Clear all resource balances and re-adds them based on each user's production tiles.")
async def respawn(ctx: commands.Context):
    """
    Clear all resource balances and re-add them based on each user's production tiles.

    This command wipes out all resource columns in the Resource sheet, then for each user, 
    re-adds amounts equal to their production tiles from the Production sheet. This affects 
    every player in the game.

    Usage
    -----------
      !respawn_month

    **WARNING:** This command affects all users and resets their resource balances 
    before recalculating them.
    """
    if isinstance(ctx.interaction, Interaction) and not ctx.interaction.response.is_done():
        await ctx.interaction.response.defer()  # Acknowledge the interaction immediately

    if not is_authorized(ctx):
        return await ctx.send("You do not have permission to perform a respawn.")

    # Prompt for confirmation
    embed = discord.Embed(
        title="Respawn Confirmation",
        description=(
            "**WARNING:** This will clear **all** resource amounts in the Resource sheet and then re-add them "
            "based on each user's Production tiles. This change affects every user.\n\n"
            "React with ✅ to confirm or ❌ to cancel."
        ),
        color=discord.Color.red()
    )
    confirm_msg = await ctx.send(embed=embed)
    await confirm_msg.add_reaction("✅")
    await confirm_msg.add_reaction("❌")

    def check_reaction(reaction: Reaction, user):
        return (
            user == ctx.author
            and str(reaction.emoji) in ["✅", "❌"]
            and reaction.message.id == confirm_msg.id
        )

    try:
        reaction, user = await bot.wait_for("reaction_add", timeout=60.0, check=check_reaction)
    except asyncio.TimeoutError:
        await ctx.send("Respawn action timed out. No changes were made.")
        return

    if str(reaction.emoji) == "❌":
        await ctx.send("Respawn canceled.")
        return

    # If we got here, user reacted with ✅
    await ctx.send("Respawn is in progress...")

    # 1) Wipe all resource columns to 0 in the Resource sheet
    for key, col in RESOURCE_COLUMNS.items():
        batch_reset_column(resource_sheet, key)

    # 2) For each user, read how many tiles they have from Production sheet,
    #    then set that resource count in Resource sheet to match.
    production_records = production_sheet.get_all_records()
    # We'll create a map {user_id_str: { resource: tile_count, ... }, ...}
    # so we can quickly look up user tile counts
    production_map = {}
    for record in production_records:
        uid = record[USER_ID]
        if uid not in production_map:
            production_map[uid] = {}
        for resource_name, tile_name in resource_to_tile.items():
            # E.g. if resource_name="Crops", tile_name="CropsTile"
            if tile_name in record:
                tile_count = record[tile_name]
                # We assume tile_count is an int. If needed, convert or handle errors
                if tile_count is None:
                    tile_count = 0
                production_map[uid][resource_name] = (tile_count)
    
    buildings_records = buildings_sheet.get_all_records()
    
    for record in buildings_records:
        uid = record[USER_ID]
        if uid not in production_map:
            production_map[uid] = {}

        if T1_INDUSTRY in record:
            tile_count = record[tile_name]
            if tile_count is None:
                tile_count = 0
            production_map[uid][resource_name] = (3*tile_count)

        if T2_INDUSTRY  in record:
            tile_count = record[tile_name]
            if tile_count is None:
                tile_count = 0
            production_map[uid][resource_name] = (6*tile_count)
        

    # Now we have a map of how many of each resource the user should get based on tile count
    # We'll iterate over the Resource sheet again, row by row, and update from production_map
    # Also, we can log these changes if needed.

    resource_records = resource_sheet.get_all_records()  # Refresh if needed
    for i, record in enumerate(resource_records, start=2):
        user_id = record["UserID"]
        # Check if we have tile data for them
        if user_id not in production_map:
            continue  # This user might not exist in production sheet

        # For each resource_name, find how many tiles the user has:
        tile_counts_for_user = production_map[user_id]
        for resource_name, tile_count in tile_counts_for_user.items():
            # resource_name is like "Crops"
            # tile_count is how many tiles -> how many resources we want to set
            col = SHEET_COLUMNS.get(resource_name)
            if not col:
                continue

            # Set the resource in the Resource sheet to tile_count
            resource_sheet.update_cell(i, col, tile_count)

    log_transaction(
        user_id="ALL",
        user_name="ALL",     
        change_amount="A LOT",  
        unit="ALL",
        source="Respawn",
        editor_id=ctx.author.id,
        editor_name=ctx.author.name,
        result="A BUNCH",
        message_link=ctx.message.jump_url
    )

    await ctx.send("Respawn complete. All resources cleared and re-added according to production tiles.")


@bot.hybrid_command(name="spawn", brief="Add a resource and its corresponding tile to a player. Counts towards the monthly spawn.")
@app_commands.describe(
    member="The player to whom the resource and tile will be added.",
    resource_amount="The amount of the resource to add.",
    resource_name="The resource name (e.g., Crops, Timber, Metal).",
    tile_amount="The number of tiles to add (defaults to resource_amount if not specified).",
    reason="The reason for adding the resource and tile (defaults to 'Use of monthly resource spawn.').",
    msg_link="A message link to the message with an attached image showing where the tiles are spawned for context (required for slash commands)."
)
async def spawn(
    ctx: commands.Context,
    member: discord.Member,
    resource_amount: int,
    resource_name: str,
    tile_amount: int = -1,
    reason: str = "Use of monthly resource spawn.",
    msg_link: str = ""
):
    """
    Add a resource and its corresponding tile to a player.\n
    ‎\n
    Adds both a resource (e.g., "Crops") and its corresponding tile (e.g., "CropsTile") to the specified user.
    Logs each change in the log sheet.\n
    ‎\n
    Usage\n
    ------\n
    !spawn @User resource_amount resource_name [tile_amount] [reason]\n
    ‎\n
    Examples\n
    ---------\n
    !spawn @User 100 Crops 2 "For completing a minor quest"\n
        - This would add 100 Crops and 2 CropsTile to @User, logging each addition.\n
    !spawn @User 6 Crops\n
        - This would add 6 Crops and 6 CropsTile to @User (default behavior).\n

    Parameters
    ----------
    member: discord.Member
        The player to whom the resource and tile will be added.
    resource_amount: int
        The amount of the resource to add.
    resource_name: str
        The resource name (e.g., Crops, Timber, Metal).
    tile_amount: int, optional
        The number of tiles to add (defaults to resource_amount if not specified).
    reason: str, optional
        The reason for adding the resource and tile (defaults to 'Use of monthly resource spawn.').
    msg_link: str, optional
       A message link to the message with an attached image showing where the tiles are spawned for context (required for slash commands).
    """
    if isinstance(ctx.interaction, Interaction) and not ctx.interaction.response.is_done():
        await ctx.interaction.response.defer()  # Acknowledge the interaction immediately

    if not is_authorized(ctx):
        return await ctx.send("You do not have permission to add resources and tiles.")

    # Ensure the expansion channel is configured
    resource_channel_id = config["resource_channel_id"]
    attachment, resource_channel, referenced_msg  = await check_attachment(ctx, resource_channel_id, "resource", msg_link)
    has_image = bool(attachment)
    if not has_image:
        return

    member = get_member(member)
    user_id = member.id
    if tile_amount < 0:
        tile_amount = resource_amount

    spawns = get_user_balance(user_id, USED_SPAWNS)
    if spawns is None:
        return await ctx.send(f"An error occurred when fetching the current {USED_SPAWNS} balance of {member.display_name}.")
    spawns = int(spawns)
    if spawns >= NUMBER_OF_SPAWNS:
        embed = discord.Embed(
            title = f"Spawn cap reached - 'Proceed?",
            description=(
                f"{member.display_name} has reached the resource spawn cap.\n"
                f"React with ✅ to confirm the spawn anyway, or ❌ to cancel."
            ),
            color=discord.Color.red()
        )
        msg = await send(ctx, embed=embed)

        # Add reaction emojis
        await msg.add_reaction("✅")
        await msg.add_reaction("❌")

        def check(reaction: Reaction, user: discord.Member):
            return (
                user == author(ctx)
                and str(reaction.emoji) in ["✅", "❌"]
                and reaction.message.id == msg.id
            )

        try:
            reaction, user = await bot.wait_for("reaction_add", timeout=60.0, check=check)
        except asyncio.TimeoutError:
            await send(ctx, "Action timed out. No changes were made.")
            return None

        if str(reaction.emoji) == "❌":
            await send(ctx, "Action canceled.")
            return None

    await send(ctx, "Proceeding with spawn.")

    og_name = resource_name
    resource_name = get_unit(resource_name)
    if resource_name is None:
        return await ctx.send(f"Invalid resource '{og_name}'.")

    tile_name = resource_to_tile[resource_name]

    # Update the resource in the resource sheet
    await add_log(ctx, member, resource_amount, resource_name, reason)

    # Update the tile count in the production sheet
    await add_log(ctx, member, tile_amount, tile_name, reason)

    # Update the number of used spawns
    await add_log(ctx, member, 1, USED_SPAWNS, reason)


    await referenced_msg.add_reaction("✅")
    # Post the image and message in the resource log channel
    embed = discord.Embed(
        description=None,
        color=discord.Color.blue()
    )
    embed.set_author(name=f"Resource spawn of {tile_amount} {resource_name} tiles by {member.display_name}", icon_url=member.avatar.url if member.avatar else None)
    embed.set_image(url=attachment.url)

    await resource_channel.send(embed=embed)
    await ctx.send(f"Resource spawn by {member.display_name} has been successfully posted in {resource_channel.mention}.")


    # 6) Confirm success
    await ctx.send(
        f"Added **{resource_amount} {resource_name}** and **{tile_amount} {tile_name}** to {member.mention}.\n"
    )


@bot.hybrid_command(name="arrival", brief="Process all army and navy arrivals for the current date.")
async def arrival(ctx: commands.Context):
    """
    Process all army and navy arrivals for the current date.

    This command processes all troop arrivals (both regular and mercenaries) where the date is 
    today or earlier. It deletes rows from the Deployment and Mercenaries sheets, adds the 
    corresponding troop types to the respective players' balances, and logs the transactions.

    Usage
    ------
    !arrival

    Example
    --------
    If today is 2024-12-28 and a row in the Deployment sheet has a date of 2024-12-27, 
    the troops listed in that row will be added to the player's balance, and the row 
    will be removed from the sheet.
    """
    if ctx.interaction and not ctx.interaction.response.is_done():
        await ctx.interaction.response.defer()

    # Authorization check
    if not is_authorized(ctx):
        return await send(ctx, "You do not have permission to process arrivals.", ephemeral=True)

    # Get the current date in YYYY-MM-DD format
    current_date = datetime.now(timezone.utc).strftime("%Y-%m-%d")

    # Fetch all data from the deployment sheet
    records = deployment_sheet.get_all_records()

    rows_to_delete = []
    for i, record in enumerate(records):
        record_date = record.get(DATE)
        user_id = record.get(USER_ID)
        unit = get_unit(record.get(TYPE))
        if not unit:
            continue
        amount = int(record.get(AMOUNT, 0))
        source = record.get(SOURCE, "Arrival")

        user = bot.get_user(int(user_id))
        user = get_member(user)

        # If the date is today or earlier, process the row
        if record_date and record_date <= current_date:
            # Add the amount to the user's balance

            await add_log(ctx, user, amount, unit, source)


            # Mark the row for deletion
            rows_to_delete.append(i + 2)  # +2 because gspread rows are 1-indexed and the first row is headers

    # Reverse the rows_to_delete list to avoid shifting issues during deletion
    rows_to_delete.reverse()

    # Delete rows from the sheet
    for row in rows_to_delete:
        deployment_sheet.delete_rows(row)

    # Send confirmation
    await send(
        ctx,
        f"Processed arrivals for {len(rows_to_delete)} rows. All troops have been added, and the rows have been removed."
    )

    ########### Mercenaries ###################

     # Fetch all data from the mercenaries sheet
    records = mercenaries_sheet.get_all_records()

    rows_to_delete = []
    for i, record in enumerate(records):
        record_date = record.get(DATE)
        user_id = record.get(USER_ID)
        sender_id = record.get(SENDER_ID)
        unit = get_unit(record.get(TYPE))
        if not unit:
            continue
        amount = int(record.get(AMOUNT, 0))
        receiver = get_member(bot.get_user(int(user_id)))
        sender = get_member(bot.get_user(int(sender_id)))

        source = record.get(SOURCE, f"Loaning {unit} from {sender.name} to {receiver.name}")



        # If the date is today or earlier, process the row
        if record_date and record_date <= current_date:
            # Add the amount to the user's balance

            await add_log(ctx, receiver, amount, unit, source)
            await add_log(ctx, sender, -amount, unit, source)
    

            # Mark the row for deletion
            rows_to_delete.append(i + 2)  # +2 because gspread rows are 1-indexed and the first row is headers

    # Reverse the rows_to_delete list to avoid shifting issues during deletion
    rows_to_delete.reverse()

    # Delete rows from the sheet
    for row in rows_to_delete:
        deployment_sheet.delete_rows(row)

    # Send confirmation
    await send(
        ctx,
        f"Processed arrivals for {len(rows_to_delete)} rows. All troops have been added, and the rows have been removed."
    )

@bot.hybrid_command(name="deploy", brief="Deploy an Army or Navy, arriving in 2 weeks.")
@app_commands.describe(
    member="The player to deploy the unit for.",
    count="The number of troops to deploy.",
    unit="The unit type (e.g., Army, Temp Army, Navy, Temp Navy).",
    pay_with="The payment method ('Silver' or 'Resources').",
    date_of_arrival="The date when the deployment arrives (YYYY-MM-DD, defaults to 2 weeks from the current date).",
    source="The reason or source of the deployment (optional, defaults to 'Deployment.')."
)
async def deploy(
    ctx: commands.Context,
    member: discord.Member,
    count: int,
    unit: str,
    pay_with: str,
    date_of_arrival: str = "",
    *,
    source: str = "Deployment."
):
    """
    Deploy an Army or Navy for a user.\n
    ‎\n
    This command deploys a specified number of Army or Navy units for the user,
    deducting resources or silver as payment. The units will arrive in 2 weeks unless
    a specific arrival date is provided. Ensures the deployment does not exceed
    the user's Army/Navy cap.\n
    ‎\n
    Usage\n
    ------\n
    !deploy @User Army Silver 3 "Loaned Armies from Hungary"\n
    !deploy @User Navy Resources 1 2024-12-23 "Event-spawned boats"\n

    Parameters
    ----------
    member: discord.Member
        The user to deploy the unit for.
    count: int
        The number of troops to deploy.
    unit: str
        The unit type (e.g., Army, Temp Army, Navy, Temp Navy).
    pay_with: str
        The payment method ('Silver' or 'Resources').
    date_of_arrival: str, optional
        The date when the deployment arrives (YYYY-MM-DD). Defaults to 2 weeks from now.
    source: str, optional
        The reason or source of the deployment. Defaults to "Deployment."
    """
    if isinstance(ctx.interaction, Interaction) and not ctx.interaction.response.is_done():
        await ctx.interaction.response.defer()  # Acknowledge the interaction immediately

    if not is_authorized(ctx):
        return await ctx.send("You do not have permission to deploy units.")

    date_of_arrival = date_of_arrival or (datetime.now(timezone.utc) + timedelta(weeks=2)).strftime("%Y-%m-%d")

    member = get_member(member)
    unit = get_unit(unit)
    if not unit:
        return await ctx.send(f"Invalid unit type. Please choose '{ARMY}', '{TEMP_ARMY}', '{NAVY}', '{TEMP_NAVY}'.")
    

    unit_short = unit[-4:].capitalize()
    unit_cap = f"{unit_short} cap"
    use_silver = pay_with.strip().lower() == "silver"


    current_troop = get_user_balance(member.id, unit_short)  
    if current_troop is None:
        return await ctx.send(f"An error occurred when fetching the current {unit_short} count of {member.display_name}.")
    current_troop = int(current_troop)

    current_temp = get_user_balance(member.id, f"Temp {unit_short}")  
    if current_temp is None:
        return await ctx.send(f"An error occurred when fetching the current Temp {unit_short} count of {member.display_name}.")
    current_temp = int(current_temp)

    current_cap = get_user_balance(member.id, unit_cap) 
    if current_cap is None:
        return await ctx.send(f"An error occurred when fetching the current {unit_cap} of {member.display_name}.")
    current_cap = int(current_cap)

    if current_troop + current_temp + count > current_cap:
        return await ctx.send(f"Deployment not possible. Current {unit_short}: {current_troop}+{current_temp}/{current_cap} (Would go over cap).")

    costs = troop_costs(unit_short, int(count), use_silver)

    # 5. Check if user can afford the costs
    check = await check_debt(ctx, member, costs)
    if not check: return

    # 6. Deduct resources and deploy the unit
    for qty, resource in costs:

        await add_log(ctx, member, -qty, get_unit(resource), f"Deploying {unit} on {date_of_arrival}")
    
    # 7. Deploy unit to unit sheet

    construct_unit(member, date_of_arrival, count, unit, source)

    # 8. Confirm deployment
    await ctx.send(
        f"{member.mention} has successfully deployed {count} **{unit}**. It will arrive on {date_of_arrival}\n"
    )


@bot.hybrid_command(name="expand", brief="Approves an expansion image and deducts silver accordingly.")
@app_commands.describe(
    member="The user performing the expansion.",
    tile_count="The number of tiles being expanded.",
    cost="The cost of the expansion in resources (e.g., '100 Silver, 40 Mounts'). For free expansions, input '0 Silver'.",
    text="Optional message to include with the expansion image.",
    msg_link="A message link to the message with an attached image showing the expansion and the tiles (required for slash commands)."
)
async def expand(ctx: commands.Context, member: discord.Member, tile_count: int, cost: str, text: str = "", msg_link: str = ""):
    """
    Approves an expansion and deducts the specified cost from the user's resources.\n 
    ‎\n
    This command is used to approve a player's expansion request by processing the attached image 
    (posted in the player's expansionchannel) and deducting the associated resource cost. 
    It also increments the player's tile count.
    ‎\n
    Usage\n
    ------\n
    !expand @User 10 "100 Silver, 40 Mounts"\n
        Approves an expansion for @User, deducting 100 Silver and 40 Mounts, and increments tile count by 10.\n
    !expand @User 5 "200 Silver" "Expanding territory" \n
        Approves an expansion for @User with an additional message.\n
    ‎\n
    Notes\n
    ------\n
    The command checks if the user has sufficient resources for the expansion. If the resources are 
    insufficient, the expansion is not approved.

    Parameters
    -----------
    member: discord.Member
        The Discord member performing the expansion.
    tile_count: int
        The number of tiles being expanded.
    cost: str
        The resource cost of the expansion (e.g., '100 Silver, 40 Mounts').
    text: str, optional
        Optional message text to include with the expansion image (default is an empty string).
    msg_link: str, optional
        A message link to the message with the attached image showing the expansion (required for slash commands).
    """
    if isinstance(ctx.interaction, Interaction) and not ctx.interaction.response.is_done():
        await ctx.interaction.response.defer()  # Acknowledge the interaction immediately

    if not is_authorized(ctx):
        return await ctx.send("You do not have permission to approve expansions.")

    # Ensure the expansion channel is configured
    expansion_channel_id = config.get("expansion_channel_id")
    attachment, expansion_channel, referenced_msg  = await check_attachment(ctx, expansion_channel_id, "expansion", msg_link)
    if not attachment:
        return

    # Ensure the user has enough silver
    member = get_member(member)
    user_id = member.id

    # Parse resource strings
    costs, invalid = parse_resource_list(cost)

    if invalid:
        return await ctx.send(f"Invalid resources: {invalid}. Aborting Expansion.")

    check = await check_debt(ctx, member, costs)
    if not check: return #await ctx.send(f"{member.display_name} does not have enough silver or resources for this expansion (Cost: {cost}).")

    # Deduct resources
    for qty, resource in costs:

        await add_log(ctx, member, -qty, get_unit(resource), f"Expansion")

    
    # Increment the tile count
    await add_log(ctx, member, int(tile_count), TILES, f"Expansion")

    await referenced_msg.add_reaction("✅")
    # Post the image and message in the expansion channel
    embed = discord.Embed(
        description=text if text else None,
        color=discord.Color.blue()
    )
    embed.set_author(name=f"Expansion by {member.display_name}", icon_url=member.avatar.url if member.avatar else None)
    embed.set_footer(text=f"Cost: {cost}")
    embed.set_image(url=attachment.url)

    await expansion_channel.send(embed=embed)
    await ctx.send(f"Expansion by {member.display_name} has been successfully posted in {expansion_channel.mention}.")


@bot.hybrid_command(name="leaderboard", brief="Shows a leaderboard for a specific unit.")
@app_commands.describe(
    unit_type="The unit type (e.g., Silver, Crops, XP, Tiles, Army) to generate the leaderboard for."
)
async def leaderboard(ctx: commands.Context, unit_type: str):
    """
    Display a leaderboard for a specific unit type, showing all players sorted by their value.\n 
    ‎\n
    Usage\n
    ------\n
    !leaderboard Silver\n
        Displays a leaderboard for Silver.\n
    !leaderboard Crops\n
        Displays a leaderboard for Crops.\n
    ‎\n       
    Notes\n
    ------\n
    The leaderboard displays the top 10 players sorted in descending order of their value for the specified unit type.\n

    Parameters
    -----------
    unit_type: str
        The unit type to generate the leaderboard for (e.g., "Silver", "Crops", "XP").
    """
    if isinstance(ctx.interaction, Interaction) and not ctx.interaction.response.is_done():
        await ctx.interaction.response.defer()  # Acknowledge the interaction immediately

    # Validate the unit type
    unit = get_unit(unit_type)
    if not unit:
        return await ctx.send(f"Invalid unit type '{unit_type}'. Please use a valid unit.")

    # Get the appropriate sheet for the unit type
    sheet = get_sheet(unit)
    if not sheet:
        return await ctx.send(f"Could not find a sheet for unit '{unit}'.")

    # Fetch all records
    try:
        records = sheet.get_all_records()
    except Exception as e:
        return await ctx.send(f"Failed to fetch data from the sheet. Error: {str(e)}")

    # Validate that the unit's values are numeric
    try:
        leaderboard = []
        for record in records:
            user_id = record.get(USER_ID)
            value = record.get(unit, "0")
            value = int(value)  # Ensure value is numeric
            
            user = ctx.message.guild.get_member(user_id)
            user_name = user.display_name
            leaderboard.append((user_id, user_name, value))
    except ValueError:
        return await ctx.send(f"The unit '{unit}' does not have numeric values. Leaderboard cannot be generated.")

    # Sort the leaderboard by unit value in descending order
    leaderboard.sort(key=lambda x: x[2], reverse=True)

    # Prepare the leaderboard for display
    embed = discord.Embed(
        title=f"🏆Leaderboard for {unit}🏆",
        description=f"Top players sorted by {unit}.",
        color=discord.Color.gold()
    )
    for rank, (user_id, user_name, value) in enumerate(leaderboard[:10], start=1):  # Display top 10
        embed.add_field(name=f"#{rank}: {user_name}", value=f"**{value}** {unit}", inline=False)

    # Send the leaderboard
    await ctx.send(embed=embed)

async def leaderboard_command(ctx: commands.Context, unit_type: str):
    """
    !leaderboard <unit_type>

    Displays a leaderboard for the specified unit type, showing all players sorted
    by their value for that unit in descending order.

    Arguments:
      - unit_type: The unit type to generate the leaderboard for (e.g., "Silver", "Crops").

    Usage:
      !leaderboard Silver
      !leaderboard Tiles
    """
    # Validate the unit type
    unit = get_unit(unit_type)
    if not unit:
        return await ctx.send(f"Invalid unit type '{unit_type}'. Please use a valid unit.")

    # Get the appropriate sheet for the unit type
    sheet = get_sheet(unit)
    if not sheet:
        return await ctx.send(f"Could not find a sheet for unit '{unit}'.")

    # Fetch all records
    try:
        records = sheet.get_all_records()
    except Exception as e:
        return await ctx.send(f"Failed to fetch data from the sheet. Error: {str(e)}")

    # Validate that the unit's values are numeric
    try:
        leaderboard = []
        for record in records:
            user_id = record.get("UserID")
            value = record.get(unit, "0")
            value = int(value)  # Ensure value is numeric

            # Fetch avatar
            user = ctx.message.guild.get_member(user_id)
            avatar_url = user.avatar.url if user and user.avatar else None

            leaderboard.append((user_id, user.display_name, value, avatar_url))
    except ValueError:
        return await ctx.send(f"The unit '{unit}' does not have numeric values. Leaderboard cannot be generated.")

    # Sort the leaderboard by unit value in descending order
    leaderboard.sort(key=lambda x: x[2], reverse=True)

    # Paginate leaderboard (10 entries per page)
    entries_per_page = 10
    pages = ceil(len(leaderboard) / entries_per_page)

    async def generate_leaderboard_page(page: int):
        embed = discord.Embed(
            title=f"Leaderboard for {unit}",
            description=f"Page {page + 1}/{pages}",
            color=discord.Color.gold()
        )
        start_idx = page * entries_per_page
        end_idx = start_idx + entries_per_page
        for rank, (user_id, display_name, value, avatar_url) in enumerate(leaderboard[start_idx:end_idx], start=start_idx + 1):
            embed.add_field(
                name=f"#{rank}: {display_name}",
                value=f"**{value}** {unit}",
                inline=False
            )
            if avatar_url:
                embed.set_thumbnail(url=avatar_url)
        return embed

    # Create a view with buttons for pagination
    class LeaderboardView(View):
        def __init__(self):
            super().__init__()
            self.current_page = 0

        @discord.ui.button(label="Previous", style=discord.ButtonStyle.primary)
        async def previous_button(self, button: Button, interaction: discord.Interaction):
            if self.current_page > 0:
                self.current_page -= 1
                await interaction.response.edit_message(embed=await generate_leaderboard_page(self.current_page), view=self)

        @discord.ui.button(label="Next", style=discord.ButtonStyle.primary)
        async def next_button(self, button: Button, interaction: discord.Interaction):
            if self.current_page < pages - 1:
                self.current_page += 1
                await interaction.response.edit_message(embed=await generate_leaderboard_page(self.current_page), view=self)

    # Send the first page
    view = LeaderboardView()
    await ctx.send(embed=await generate_leaderboard_page(0), view=view)


verb = "Manufacture"
# name suggestions: process, manufacture, convert
@bot.hybrid_command(name="manufacture", brief="Manufacture advanced resources from basic resources.")
@app_commands.describe(
    member="The member performing the manufacturing.",
    count="The number of industry and basic resources to use.",
    basic_resource="The type of basic resource to use (e.g., Crops, Metal, etc.)."
)
async def manufacture(ctx: commands.Context, member: discord.Member, count: int, basic_resource: str):
    """
    Manufacture advanced resources by using a specified number of industry and basic resources.\n
    Adds the equivalent amount of advanced resources to the user's balance.\n
    ‎\n
    Rules\n
    -----\n
    - Supplies: Created using Crops, Mounts, or Livestock.\n
    - Tools: Created using Metal or Timber.\n
    - Energy: Created using Fuel or Fiber.\n
    - Cement: Created using Stone.\n
    ‎\n
    Usage\n
    ------\n
    !manufacture @User 10 Crops\n
        Converts 10 Crops and 10 Industry into 10 Supplies for @User.\n
    ‎\n
    Notes\n
    -----\n
    - The advanced resource created depends on the basic resource used, according to the rules specified.\n
    

    Parameters
    -----------
    member: discord.Member
        The member performing the manufacturing.
    count: int
        The number of industry and basic resources to use for manufacturing.
    basic_resource: str
        The type of basic resource to use (e.g., Crops, Metal, Timber, etc.).
    """

    if ctx.interaction and not ctx.interaction.response.is_done():
        await ctx.interaction.response.defer()

    # Authorization check
    if not is_authorized(ctx):
        return await send(ctx, "You do not have permission to manufacture resources.", ephemeral=True)

    basic_resource = get_unit(basic_resource)
    advanced_resource = advanced_resource_map.get(basic_resource)
    if not advanced_resource:
        return await send(ctx, f"{basic_resource} is not a valid basic resource for manufacturing.", ephemeral=True)

    # Check if the user has enough industry and basic resources
    costs = [(count, basic_resource), (count, INDUSTRY)]
    
    member = get_member(member)
    check = await check_debt(ctx, member, costs)
    if not check: return

    # Deduct the required resources
    updated_industry =  await add_log(ctx, member, -count, INDUSTRY, verb)
    updated_basic = await add_log(ctx, member, -count, basic_resource, verb)

    # Add the advanced resources
    updated_advanced = await add_log(ctx, member, count, advanced_resource, verb)


    # Send a confirmation message
    await send(ctx, (
        f"{member.display_name} has manufactured {count} {advanced_resource} using {count} {basic_resource} "
        f"and {count} {INDUSTRY} points.\n"
        f"Updated balances:\n"
        f"- {INDUSTRY}: {updated_industry}\n"
        f"- {basic_resource}: {updated_basic}\n"
        f"- {advanced_resource}: {updated_advanced}"
    ))


@bot.hybrid_command(name="union", brief="Create a union between 2 (or 3) players.")
@app_commands.describe(
    leader="The player who will be the union leader.",
    member_2="The second player to join the union.",
    member_3="The third player to join the union (optional)."
)
async def union(ctx: commands.Context, leader: discord.Member, member_2: discord.Member, member_3: discord.Member = None):
    """
    Create a union between 2 (or 3) players by consolidating their stats under a single leader.\n
    ‎\n
    How it Works\n
    -------------\n
    - Updates the 'Union Leader' and 'Union Leader ID' fields for all participants.\n
    - Sets the 'Status' field to indicate the union.\n
    - Merges numeric values (e.g., XP, Silver, Tiles, Army, Navy) from all participants into the leader.\n
      - Adds sums for most columns (e.g., XP, resources).\n
      - Takes the maximum for specific columns (notably Monument, Emporium since the count represents a tier).\n
    - Logs the union creation in the transaction log.\n
    ‎\n
    Usage\n
    ------\n
    !union @Leader @Member2\n
        Creates a union between Leader and Member2, with Leader as the union leader.\n
    ‎\n
    Notes\n
    ------\n
    - This command modifies multiple sheets, including View, Resource, Production, Buildings, and War sheets.\n
    - Union participants' stats are consolidated, and all numeric fields are merged into the leader's stats.\n
    

    Parameters
    -----------
    leader: discord.Member
        The player who will become the union leader.
    member_2: discord.Member
        The second player joining the union.
    member_3: discord.Member, optional
        The third player joining the union (default is None).
    """

    if ctx.interaction and not ctx.interaction.response.is_done():
        await ctx.interaction.response.defer()

    # Authorization check
    if not is_authorized(ctx):
        return await send(ctx, "You do not have permission to create unions.", ephemeral=True)

    # List of players in the union
    players = [leader, member_2]
    if member_3:
        players.append(member_3)

    # Prepare the union details
    leader_name = leader.name
    leader_id = leader.id
    player_names = ", ".join([player.display_name for player in players])

    for player in players:
        # Get the player's row in the sheet
        try:
            user_ids = silver_sheet.col_values(SHEET_COLUMNS[USER_ID])  
            player_row = user_ids.index(str(player.id)) + 1
        except:
            await send(ctx, f"Player {player.display_name} does not exist in the sheet. Please add them first.", ephemeral=True)
            return

        # Update the "Union Leader", "Union Leader ID", and "Status" columns
        silver_sheet.update_cell(player_row, SHEET_COLUMNS[UNION_LEADER], leader_name)
        silver_sheet.update_cell(player_row, SHEET_COLUMNS[UNION_LEADER_ID], str(leader_id))
        silver_sheet.update_cell(player_row, SHEET_COLUMNS[STATUS], f"Full Union with {player_names}")

    # 2) For each participant (except the leader), merge numeric columns into the leader.

    # Identify the sheets we need to update and which columns to skip or handle specially.
    # Format: (sheet_object, skip_cols_set, special_max_cols_set)
    # - skip_cols: columns we do not want to sum (like "UserID", "Discord name", etc.)
    # - special_max_cols: columns we want to take the max instead of summing (like "Emporium", "Monument").
    #   For other numeric columns, we sum them.
    union_sheets = [
        (silver_sheet, {USER_ID, DISCORD_NAME, DISPLAY_NAME, NATION_NAME, CAPITAL, RELIGION, CULTURE, SUBCULTURE, STATUS, UNION_LEADER, UNION_LEADER_ID, TITLE, OTHER_BONUSES}, {}),  # Summation for XP, Silver, Tiles
        (war_sheet, {USER_ID, DISCORD_NAME, ARMY_DOCTRINE, NAVY_DOCTRINE}, {}),  # Summation for Army, Navy, Caps, Temp Army/Navy
        (resource_sheet, {USER_ID, DISCORD_NAME}, {}),            # Summation for all numeric columns
        (production_sheet, {USER_ID, DISCORD_NAME}, {}),          # Summation for production numeric columns
        (buildings_sheet, {USER_ID, DISCORD_NAME}, {EMPORIUM, MONUMENT})  # Summation for buildings except Emporium, Monument => max
    ]

    # The function to retrieve an entire row's values as a dictionary: {col_name: str_value}
    def get_row_values(sheet_obj: Worksheet, user_id_str: str) -> Dict[str, str]:
        """Returns a dict of column_name -> cell_value for the given user_id_str in the provided sheet."""
        # Attempt to find the row
        col_userids = sheet_obj.col_values(SHEET_COLUMNS[USER_ID])
        try:
            row_idx = col_userids.index(user_id_str) + 1
        except ValueError:
            return {}  # The user doesn't exist in this sheet
        # Retrieve entire row once, then map columns
        row_values = sheet_obj.row_values(row_idx)
        row_dict = {}
        # Construct a map of col_name -> str_value using SHEET_COLUMNS
        # But we invert SHEET_COLUMNS to get col_idx -> col_name
        inverted_cols = {v: k for k, v in SHEET_COLUMNS.items() if v is not None}
        for col_index, cell_value in enumerate(row_values, start=1):
            if col_index in inverted_cols:
                row_dict[inverted_cols[col_index]] = cell_value
        return row_dict, row_idx

    # The function to update the entire row in a single batch update
    def update_row_values(sheet_obj: Worksheet, row_idx: int, new_values: Dict[str, Any]) -> None:
        """Updates the specified row in batch with new_values. new_values is {col_name: new_str_value}."""
        requests = []
        for col_name, updated_val in new_values.items():
            col_index = SHEET_COLUMNS.get(col_name)
            if col_index:
                a1_notation = gspread.utils.rowcol_to_a1(row_idx, col_index)
                requests.append({
                    "range": a1_notation,
                    "values": [[str(updated_val)]]
                })
        if requests:
            sheet_obj.batch_update(requests)

    # Merge data from each participant into the leader
    leader_id_str = str(leader.id)
    for participant in players:
        if participant == leader:
            continue  # Skip the leader themself

        participant_id_str = str(participant.id)
        # For each sheet in union_sheets, retrieve row for leader and participant, sum numeric columns, update leader
        for sheet_obj, skip_cols, special_max_cols in union_sheets:
            # Retrieve row data for participant
            part_data_tuple = get_row_values(sheet_obj, participant_id_str)
            if not part_data_tuple:
                continue
            part_data, part_row_idx = part_data_tuple

            # Retrieve row data for leader
            lead_data_tuple = get_row_values(sheet_obj, leader_id_str)
            if not lead_data_tuple:
                continue
            lead_data, lead_row_idx = lead_data_tuple

            updated_lead_data = dict(lead_data)  # Copy to update

            # For each column in participant's row data
            for col_name, part_val_str in part_data.items():
                if col_name in skip_cols:
                    continue  # skip userID, discord name, etc.

                part_val_str = part_val_str.strip()
                lead_val_str = updated_lead_data.get(col_name, "").strip()

                # Attempt to parse both as integers
                if part_val_str.isdigit() or (part_val_str.startswith("-") and part_val_str[1:].isdigit()):
                    part_val = int(part_val_str)
                else:
                    continue  # skip non-numeric or empty

                if lead_val_str.isdigit() or (lead_val_str.startswith("-") and lead_val_str[1:].isdigit()):
                    lead_val = int(lead_val_str)
                else:
                    lead_val = 0

                if col_name in special_max_cols:
                    # e.g. Monument, Emporium => take max
                    new_val = max(lead_val, part_val)
                else:
                    # sum
                    new_val = lead_val + part_val

                updated_lead_data[col_name] = new_val

            # Write updated leader row in a single batch update
            update_row_values(sheet_obj, lead_row_idx, updated_lead_data)

    
    log_transaction(
        user_id=leader_id_str,
        user_name=leader.name,
        change_amount=f"Full Union with {player_names}",
        unit=STATUS,
        source="Full union creation",
        editor_id=ctx.author.id,
        editor_name=ctx.author.name,
        result=f"Full Union with {player_names}",
        message_link=ctx.message.jump_url,
    )


    # Send confirmation
    await send(
        ctx,
        f"Union successfully created! {leader.display_name} is the Union Leader.\nParticipants: {player_names}."
    )

@bot.hybrid_command(name="loan", brief="Loans an Army or Navy from one player to the other, arriving in 2 weeks.")
@app_commands.describe(
    receiver="The member receiving the loaned unit.",
    sender="The member loaning the unit.",
    count="The number of units being loaned.",
    unit="The type of unit to loan (Army or Navy).",
    compensation="Optional resources or silver as compensation for the loan (e.g., '100 Silver, 2 Crops').",
    date_of_arrival="The date when the loaned units will arrive (default is 2 weeks from today).",
    date_of_return="The date when the loaned units are expected to be returned (default is 4 weeks from today).",
    source="Optional source or reason for the loan (default is 'Mercenary.')."
)
async def loan(
    ctx: commands.Context,
    receiver: discord.Member,
    sender: discord.Member,
    count: int,
    unit: str,
    compensation: str = "0 Silver",
    date_of_arrival: str = "",
    date_of_return: str = "",
    *,
    source: str = "Mercenary."
):
    """
    Loans an Army or Navy from one player to another, specifying compensation, arrival, and return dates.\n 
    ‎\n 
    How it Works\n 
    -------------\n 
    - Deducts compensation resources from the receiver and adds them to the sender.\n 
    - Validates unit caps to ensure the receiver can accommodate the loaned units.\n 
    - Logs the transaction and schedules the arrival of loaned units.\n 
    ‎\n 
    Usage\n 
    -------------\n 
    !loan @Receiver @Sender 10 Army "100 Silver, 50 Crops"\n 
        Loans 10 Army units from @Sender to @Receiver with 100 Silver and 50 Crops as compensation.\n 
    !loan @Receiver @Sender 5 Navy\n 
        Loans 5 Navy units from @Sender to @Receiver without any compensation.\n 

    Parameters
    -----------
    receiver: discord.Member
        The player receiving the loaned units.
    sender: discord.Member
        The player loaning the units.
    count: int
        The number of units being loaned.
    unit: str
        The type of unit to loan ('Army' or 'Navy').
    compensation: str, optional
        A comma-separated list of resources and amounts provided as compensation for the loan (default is "").
        Example: "100 Silver, 50 Crops".
    date_of_arrival: str, optional
        The date when the loaned units will arrive in YYYY-MM-DD format (default is 2 weeks from today).
    date_of_return: str, optional
        The date when the loaned units are expected to be returned in YYYY-MM-DD format (default is 4 weeks from today).
    source: str, optional
        The source or reason for the loan (default is "Mercenary.").
    """
    if isinstance(ctx.interaction, Interaction) and not ctx.interaction.response.is_done():
        await ctx.interaction.response.defer()  # Acknowledge the interaction immediately

    if not is_authorized(ctx):
        return await ctx.send("You do not have approve mercenary armies units.")

    date_of_arrival = date_of_arrival or (datetime.now(timezone.utc) + timedelta(weeks=2)).strftime("%Y-%m-%d")
    date_of_return = date_of_return or (datetime.now(timezone.utc) + timedelta(weeks=4)).strftime("%Y-%m-%d")

    receiver = get_member(receiver)
    unit = get_unit(unit)
    if not unit or unit :
        return await ctx.send(f"Invalid unit type. Please choose '{ARMY}' or '{NAVY}'.")
    

    unit_cap = f"{unit} cap"


    current_troop = get_user_balance(receiver.id, unit)  
    if current_troop is None:
        return await ctx.send(f"An error occurred when fetching the current {unit} count of {receiver.display_name}.")
    current_troop = int(current_troop)

    current_temp = get_user_balance(receiver.id, f"Temp {unit}")  
    if current_temp is None:
        return await ctx.send(f"An error occurred when fetching the current Temp {unit} count of {receiver.display_name}.")
    current_temp = int(current_temp)

    current_cap = get_user_balance(receiver.id, unit_cap) 
    if current_cap is None:
        return await ctx.send(f"An error occurred when fetching the current {unit_cap} of {receiver.display_name}.")
    current_cap = int(current_cap)

    if current_troop + current_temp + count > current_cap:
        return await ctx.send(f"Deployment not possible. Current {unit}: {current_troop}+{current_temp}/{current_cap} (Would go over cap).")

    costs, invalid = parse_resource_list(compensation)
    if invalid:
        return await ctx.send(f"Invalid resources: {invalid}. Aborting Loaning.")


    # 5. Check if user can afford the costs
    check = await check_debt(ctx, receiver, costs)
    if not check: return

    # 6. Deduct resources and deploy the unit
    for qty, resource in costs:

        await add_log(ctx, receiver, -qty, get_unit(resource), f"Loaning {unit} from {sender.name} arriving {date_of_arrival}")
        await add_log(ctx, sender, qty, get_unit(resource), f"Loaning {unit} to {receiver.name} arriving {date_of_arrival}")

    # 7. Deploy unit to unit sheet
    loan_unit(receiver, sender, date_of_arrival, date_of_return, count, unit, source)

    # 8. Confirm deployment
    await ctx.send(
        f"{sender.mention} has successfully loaned {receiver.mention} {count} **{unit}**. It will arrive on {date_of_arrival} and be returned on {date_of_return}\n"
    )


bot.run(DISCORD_TOKEN)
