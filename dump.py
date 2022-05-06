import mysql.connector
import json

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

def file_conf():
	with open("config.json", "r") as cfg:
		conf = json.load(cfg)
		print(conf)
		return conf

def make_connection(conf):	
	cnx = mysql.connector.connect(user = conf["user"], password = conf["password"],
                              host = conf["host"],
                              database = conf["db"])
	return cnx

def get_select_log_query():
	return "SELECT argument from mysql.general_log where command_type='Query'"

def get_blacklist():
	blacklist = {
		get_select_log_query(),
		"SET NAMES 'utf8mb4' COLLATE 'utf8mb4_general_ci'",
		"SET NAMES utf8mb4",
		"set autocommit=0"
	}
	return blacklist
def main():
	# conf = interactive_conf()
	conf = file_conf()
	cnx = make_connection(conf)
	cursor = cnx.cursor()
	cursor.execute(get_select_log_query())
	for x in cursor:
		query = x[0].decode("utf-8")
		if query not in get_blacklist():
			print(query)
		

if __name__ == "__main__":
	main()
