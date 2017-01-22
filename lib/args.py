#!/usr/bin/python3 -uB


import argparse
import info
import config

# the command line cannot change so all of this can be done in the module root

ap = argparse.ArgumentParser(description = info.APP_NAME + " - The main " + info.APP_TITLE + " daemon")
ap.add_argument("--config", metavar = "CFG_FILE", type = str, nargs = None, default = True, help = "Config")
ap.add_argument("--foreground", metavar = None, type = bool, nargs = None, default = True, help = "Do not detach (the deafult)")
ap.add_argument("--daemonize", metavar = None, type = bool, nargs = None, default = False, help = "Daemonize")


