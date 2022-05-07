import mysql.connector
import json
import argparse

def interactive_conf():
	conf = {}
	print("Host:");
	conf["host"] = input()

	print("Database:")
	conf["db"] = input()

	print("Username:")	
	conf["user"] = input()

	print("Password")
	conf["password"] = input()

	return conf

def file_conf(conf_name):
	with open(conf_name, "r") as cfg:
		conf = json.load(cfg)
		return conf

def make_connection(conf):	
	cnx = mysql.connector.connect(user = conf["user"], password = conf["password"],
                              host = conf["host"],
                              database = conf["db"])
	return cnx

def get_select_log_query():
	return "SELECT argument from mysql.general_log where command_type='Query'"

def get_truncate_log_query():
	return "TRUNCATE mysql.general_log"

def dump(conf):
	cnx = make_connection(conf)
	cursor = cnx.cursor()
	cursor.execute(get_select_log_query())
	for x in cursor:
		query = x[0].decode("utf-8")
		if query not in get_blacklist():
			print(query)

def truncate(conf):
	cnx = make_connection(conf)
	cursor = cnx.cursor()
	cursor.execute(get_truncate_log_query())
	return

def get_check_general_log_query():
	return 

def get_create_general_log_query():
	return "CREATE TABLE IF NOT EXISTS mysql.general_log ( \
   		`event_time` timestamp(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6) \
                          ON UPDATE CURRENT_TIMESTAMP(6), \
   		`user_host` mediumtext NOT NULL, \
   		`thread_id` bigint(21) unsigned NOT NULL, \
   		`server_id` int(10) unsigned NOT NULL, \
   		`command_type` varchar(64) NOT NULL, \
   		`argument` mediumblob NOT NULL \
  		) ENGINE=CSV DEFAULT CHARSET=utf8 COMMENT='General log'"

def get_table_query_query():
	return "SET global log_output = 'table'"

def get_enable_general_log_query():
	return "SET global general_log = 1"

def get_disable_general_log_query():
	return "SET global general_log = 0"

def get_blacklist():
	blacklist = {
		get_select_log_query(),
		get_enable_general_log_query(),
		get_table_query_query(),
		get_disable_general_log_query(),
		get_create_general_log_query(),
		"SET NAMES 'utf8mb4' COLLATE 'utf8mb4_general_ci'",
		"SET NAMES utf8mb4",
		"set autocommit=0"
	}
	return blacklist


def log_on(conf):
	cnx = make_connection(conf)
	cursor = cnx.cursor()
	cursor.execute(get_create_general_log_query())
	cursor.execute(get_table_query_query())
	cursor.execute(get_enable_general_log_query())
	return

def log_off(conf):
	cnx = make_connection(conf)
	cursor = cnx.cursor()
	cursor.execute(get_disable_general_log_query())
	return

def main():
	actions = {"on", "off", "dump", "truncate"}
	parser = argparse.ArgumentParser(description='Manages mysql server general log')
	parser.add_argument("action", help=" | ".join(actions))
	parser.add_argument("--config", help="json config, that contains mysql connection parameters")
	args = parser.parse_args()
	if args.action not in actions:
		print("[Error] action should be one of: " + " | ".join(actions))
		return
	
	conf = None
	if args.config is not None:
		print("using config " + args.config)
		conf = file_conf(args.config)
	else:
		print("interactive mode")
		conf = interactive_conf()

	if args.action == "dump":
		dump(conf)
	elif args.action == "truncate":
		log_off(conf)
		truncate(conf)
		log_on(conf)
	elif args.action == "on":
		log_on(conf)
	elif args.action == "off":
		log_off(conf)

if __name__ == "__main__":
	main()
