#!/usr/bin/python3 -uB

"""
	Logging manager for PgPlex
"""
import logging
import logging.config


LOGGER = logging.getLogger(__name__)


def setup_loggers():
	logging.config.dictConfig({
		"version": 1,
		"disable_existing_loggers": False,
		"formatters": {
			"standard": {
				"format": "%(asctime)s [%(levelname)s] %(name)s: %(message)s"
			},
		},
		"handlers": {
			"default": {
				"level": "INFO",
				"formatter": "standard",
				"class": "logging.StreamHandler",
			},
		},
		"loggers": {
			"": {
				"handlers": [ "default" ],
				"level": "INFO",
				"propagate": True
			},
		}
	})
	return True


