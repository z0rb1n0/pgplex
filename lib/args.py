#!/usr/bin/python3 -uB
import argparse
import info
import guc


# list of alternate short forms for configuration options
ABBREVIATIONS = {
	"config_file": ("c"),
	"daemonize": ("d")
}


# the command line cannot change so all of this can be done in the module root


def get_from_parser():
	"""
		Simply returns a populated parser, generated from the GUC.
		Or fails hard...
	"""

	ap = argparse.ArgumentParser(description = info.APP_NAME + " - The main " + info.APP_TITLE + " daemon", allow_abbrev = False)

	# we need to create a global dictionary of sorted options, to
	# keep track of what section they belong to
	all_options = {}
	for (guc_section, guc_options) in guc.DEFINITIONS.items():
		for option_name in guc_options.keys():
			all_options[option_name] = guc_section

	for option_name in sorted(all_options.keys()):
		option_params = guc.DEFINITIONS[all_options[option_name]][option_name]

		# For some weird reasonm, argparse does not like the option string
		# as a keyword argument. This forces us to supply the kwargs separately


		option_strings = ["--%s" % (option_name.replace("_", "-"),)]
		
		# note that we universally force none if they're not passed,
		# even for booleans. This is to preserve other defaults
		arg_settings = {
			"dest": option_name,
			"default": None,
			"help": option_params[3]
		}

		# booleans have a different behavior.
		if (option_params[0] is bool):
			arg_settings.update({
				"action": "store_true"
			})
		else:
			arg_settings.update({
				"type": option_params[0],
				"metavar": option_params[0].__name__.upper(),
			})

		# we add the single-dash abbreviations if needed
		if option_name in ABBREVIATIONS:
			option_strings += map(lambda abb : "-" + abb, ABBREVIATIONS[option_name])
			#arg_settings["aliases"] = map(lambda abb : "-" + abb, ABBREVIATIONS[option_name])
		
		# the multiple options, for strings, dictate what is available
		default_str = None
		if ((option_params[0] is bool) and (option_params[2] is not None)):
			default_str = "True" if (option_params[2]) else "False"
		elif ((option_params[0] is int) and (option_params[2] is not None)):
			default_str = "%d" % (option_params[2],)
		elif ((option_params[0] is float) and (option_params[2] is not None)):
			default_str = "%.3f" % (option_params[2],)
		elif (option_params[0] is str):
			if (isinstance(option_params[1], (tuple, list))):
				arg_settings.update({
					"choices": option_params[1],
				})
				arg_settings["help"] += " - Valid options: `%s`" % "`, `".join(arg_settings["choices"])
				default_str = option_params[1][option_params[2] if (option_params[2] is not None) else 0]
			else:
				if (option_params[2] is not None):
					default_str = "`%s`" % (option_params[2],)

		arg_settings["help"] += " - "
		if (default_str is not None):
			arg_settings["help"] += "Default: %s" % (default_str,)
		else:
			arg_settings["help"] += "No default"

		# what kind of argument we generate depends on what the GUC expects
		ap.add_argument(
			*option_strings,
			**arg_settings
		)
	return ap.parse_args()
