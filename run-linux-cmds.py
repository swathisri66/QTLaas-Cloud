import logging
from os import system

# Setting up logging parameters
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def run_linux_cmds():
    try:
        f = open("linux_commands.txt", "r")

    except:
        logger.error("__ACC__:Something went wrong while attempting to open 'linux_commands.txt' file. "
                     "Make sure to run qtlaas_automation.find_new_workers() first and try again.")
        return False

    linux_cmds = f.readlines()
    for line in linux_cmds:
        command = line.strip()
        try:
            system(command)
        except:
            logger.error("__ACC__:Something went wrong while attempting to run: " + line + "Skipping this instance")
            logger.error('__ACC__: Try to run the command manually.')
            continue
run_linux_cmds()

